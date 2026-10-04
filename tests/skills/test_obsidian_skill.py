"""Exercise the bundled vault helper against real Markdown files."""

import json
import subprocess
import sys
from pathlib import Path

import pytest


SCRIPT = (
    Path(__file__).resolve().parents[2]
    / "skills/note-taking/obsidian/scripts/vault_search.py"
)


def note(vault, path, content):
    target = vault / path
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(content, encoding="utf-8")
    return target


def invoke(vault, *args, success=True):
    result = subprocess.run(
        [sys.executable, str(SCRIPT), "--vault", str(vault), *args],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == (0 if success else 1), result.stderr or result.stdout
    return json.loads(result.stdout)


def test_folder_topics_find_tasks_without_topic_words_in_content(tmp_path):
    path = "Finance/Cleanup/To Be Cancelled.md"
    source = note(tmp_path, path, "- [ ] Twitch Sub\n- [x] Finished\n- [ ] \n- [ ] Upstash\n")
    note(tmp_path, "Inbox/Finance Cleanup.md", "- [ ] Cancel another subscription\n")
    note(tmp_path, "Personal/Todo.md", "- [ ] Buy groceries\n")
    before = source.read_bytes()

    result = invoke(tmp_path, "--query", "finance cleanup", "--tasks")

    assert result["matched_by"] == "path"
    assert result["matched_files"] == 2
    assert result["results"] == [
        {"path": path, "line": 1, "text": "Twitch Sub"},
        {"path": path, "line": 4, "text": "Upstash"},
        {"path": "Inbox/Finance Cleanup.md", "line": 1, "text": "Cancel another subscription"},
    ]
    assert source.read_bytes() == before


def test_content_fallback_when_no_paths_match(tmp_path):
    note(tmp_path, "Inbox/Review.md", "# FINANCE cleanup\n- [ ] Close account\n")
    note(tmp_path, "Inbox/Other.md", "# Finance\n- [ ] Check budget\n")

    result = invoke(tmp_path, "--query", "finance cleanup", "--tasks")

    assert result["matched_by"] == "content"
    assert result["results"] == [{"path": "Inbox/Review.md", "line": 2, "text": "Close account"}]


@pytest.mark.parametrize("query", [(), ("--query", "finance cleanup")])
def test_scope_preserves_ancestry_and_excludes_other_folders(tmp_path, query):
    note(tmp_path, "Finance/Cleanup/One.md", "- [ ] Close account\n")
    note(tmp_path, "Finance/Other.md", "- [ ] Keep account\n")

    result = invoke(tmp_path, "--path", "Finance/Cleanup", "--tasks", *query)

    assert result["total"] == 1
    assert result["results"][0]["path"] == "Finance/Cleanup/One.md"


def test_pagination_counts_tasks_across_notes(tmp_path):
    note(tmp_path, "Finance/Cleanup/A.md", "- [ ] First\n- [ ] Second\n")
    note(tmp_path, "Finance/Cleanup/B.md", "- [ ] Third\n")

    result = invoke(tmp_path, "--query", "finance cleanup", "--tasks", "--limit", "1", "--offset", "1")

    assert result["total"] == 3
    assert result["offset"] == 1
    assert result["truncated"] is True
    assert result["results"] == [{"path": "Finance/Cleanup/A.md", "line": 2, "text": "Second"}]
    final = invoke(tmp_path, "--query", "finance cleanup", "--tasks", "--limit", "1", "--offset", "2")
    assert final["truncated"] is False
    assert final["results"][0]["text"] == "Third"


def test_task_parser_ignores_fenced_examples_and_blank_checkboxes(tmp_path):
    note(tmp_path, "Tasks.md", "\n".join([
        "````markdown", "- [ ] Example", "```", "- [ ] Still an example", "````",
        "~~~", "- [ ] Another example", "~~~", "  * [ ] Real bullet",
        "1. [ ] Real numbered task", "+ [x] Finished", "- [-] Custom status", "- [ ]   ",
    ]))

    result = invoke(tmp_path, "--path", "Tasks.md", "--tasks")

    assert result["results"] == [
        {"path": "Tasks.md", "line": 9, "text": "Real bullet"},
        {"path": "Tasks.md", "line": 10, "text": "Real numbered task"},
    ]


def test_note_discovery_does_not_require_reading_path_matches(tmp_path):
    (tmp_path / "Finance Cleanup.md").write_bytes(b"\xff")

    result = invoke(tmp_path, "--query", "finance cleanup")

    assert result["results"] == [{"path": "Finance Cleanup.md"}]
    assert result["skipped_files"] == []


def test_reports_unreadable_notes_in_content_fallback(tmp_path):
    (tmp_path / "Broken.md").write_bytes(b"\xff")
    note(tmp_path, "Good.md", "finance cleanup\n- [ ] Close account\n")

    result = invoke(tmp_path, "--query", "finance cleanup", "--tasks")

    assert result["total"] == 1
    assert result["skipped_files"] == ["Broken.md"]


def test_path_match_with_no_tasks_does_not_silently_widen_scope(tmp_path):
    note(tmp_path, "Finance/Cleanup/Done.md", "- [x] Finished\n")
    note(tmp_path, "Inbox.md", "finance cleanup\n- [ ] Unrelated reminder\n")

    result = invoke(tmp_path, "--query", "finance cleanup", "--tasks")

    assert result["matched_by"] == "path"
    assert result["matched_files"] == 1
    assert result["total"] == 0


def test_skips_hidden_and_symlinked_notes(tmp_path):
    vault = tmp_path / "Vault"
    note(vault, "Finance/Cleanup/Visible.md", "- [ ] Visible\n")
    note(vault, ".obsidian/Finance Cleanup.md", "- [ ] Hidden\n")
    external = note(tmp_path, "Outside.md", "- [ ] Outside\n")
    try:
        (vault / "Finance/Cleanup/Linked.md").symlink_to(external)
        (vault / "Linked directory").symlink_to(tmp_path, target_is_directory=True)
    except OSError:
        pytest.skip("Symlinks unavailable on this platform")

    result = invoke(vault, "--tasks")

    assert result["total"] == 1
    assert result["results"][0]["text"] == "Visible"


@pytest.mark.parametrize("scope", ["../Outside.md", "Missing"])
def test_rejects_invalid_scope(tmp_path, scope):
    vault = tmp_path / "Vault"
    vault.mkdir()

    result = invoke(vault, "--path", scope, success=False)

    assert "error" in result


def test_rejects_missing_vault(tmp_path):
    result = invoke(tmp_path / "Missing vault", "--tasks", success=False)
    assert "error" in result


def test_no_match_returns_empty_results(tmp_path):
    note(tmp_path, "Other.md", "- [ ] Buy groceries\n")
    result = invoke(tmp_path, "--query", "finance cleanup", "--tasks")
    assert result["total"] == 0
    assert result["results"] == []
    assert result["truncated"] is False
