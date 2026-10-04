---
title: "Obsidian — Read, search, create, and edit notes in the Obsidian vault"
sidebar_label: "Obsidian"
description: "Read, search, create, and edit notes in the Obsidian vault"
---

{/* This page is auto-generated from the skill's SKILL.md by website/scripts/generate-skill-docs.py. Edit the source SKILL.md, not this page. */}

# Obsidian

Read, search, create, and edit notes in the Obsidian vault.

## Skill metadata

| | |
|---|---|
| Source | Bundled (installed by default) |
| Path | `skills/note-taking/obsidian` |
| Version | `1.0.0` |
| Author | Teknium (teknium1), Hermes Agent |
| License | MIT |
| Platforms | linux, macos, windows |
| Tags | `Obsidian`, `Notes`, `Markdown`, `Vault` |

## Reference: full SKILL.md

:::info
The following is the complete skill definition that Hermes loads when this skill is triggered. This is what the agent sees as instructions when the skill is active.
:::

# Obsidian Vault Skill

Read, search, create, and edit local Markdown notes. Use the bundled read-only
helper for topic discovery and unchecked tasks; use file tools for note edits.
No running Obsidian app, MCP server, or persistent index is required.

## When to Use

Use for vault notes, topic searches, and requests to list unfinished tasks.
When an exact note path is already known, go directly to `read_file`.

## Prerequisites

Resolve the vault path once. Use an already configured `OBSIDIAN_VAULT_PATH`,
or fall back to `~/Documents/Obsidian Vault`. If unknown, use `terminal` to
resolve the path or check that the fallback exists.

File tools do not expand shell variables: pass concrete absolute paths to
`read_file`, `write_file`, `patch`, and `search_files`. Quote paths in shell
commands. The helper needs Python 3.11+ and access to the vault filesystem.

## How to Run

Run `scripts/vault_search.py` through `terminal`. Form its absolute path from
the `skill_dir` returned by `skill_view`; execute it without loading its source.
Replace the placeholders below with that directory and the resolved vault path:

```bash
python3 "<skill_dir>/scripts/vault_search.py" \
  --vault "<resolved-vault-path>" --query "finance cleanup" --tasks
```

This matches all query words case-insensitively against **full relative paths**,
including parent folders. It reads those notes and returns nonblank unchecked
tasks with source paths and line numbers in the same invocation. If no paths
match, it searches note contents for all the words instead.

Query words are literal topic keywords, not a sentence or regex. For example,
use `finance cleanup` for "find my finance cleanup tasks".

## Quick Reference

| Need | Helper arguments or file tool |
|---|---|
| Find topic notes | `--query "finance cleanup"` |
| List topic tasks | `--query "finance cleanup" --tasks` |
| Tasks in a known folder or note | `--path "Finance/Cleanup" --tasks` |
| List notes in a folder | `--path "Finance/Cleanup"` |
| Next result page | Same arguments, plus `--offset N --limit 100` |
| Read a known note | `read_file` with its absolute path |
| Create or replace a note | `write_file` with full Markdown content |
| Make a focused edit | `patch` after reading the note |

Every helper invocation needs `--vault`. `--path` is relative to the vault and
can name a folder or note. Without a query or path, the helper scans the vault.

## Procedure

1. Resolve the vault and reuse known note/folder paths from the conversation.
2. For topic discovery or task lists, run the helper once with short topic
   keywords or a known `--path`. Prefer folder structure over guessing which
   topic words appear in task text.
3. For task requests, answer directly from the returned task text and sources.
   Read a note only when more context is needed or before editing it.
4. Inspect `matched_by`: `path` means only matching paths were searched;
   `content` means a whole-scope content fallback; `scope` means no query.
   These are literal matches, not exhaustive semantic search. If no relevant
   notes match, try one broader topic query or list notes in the likely folder.
   Do not cycle through synonym-heavy regex searches across the entire vault.
5. `total`, `offset`, `limit`, and `truncated` refer to returned notes, or to
   task rows with `--tasks`. If truncated, fetch the next page at
   `offset + len(results)` before claiming to list all results. Report
   incomplete coverage when `skipped_count` is nonzero.
6. For edits, use `read_file`, then `patch` with stable context. Use
   `write_file` when a complete rewrite is clearer. For an anchored append,
   replace the anchor with the anchor plus new content; a simple append without
   stable context may use `terminal`. Add `[[Note Name]]` wikilinks as needed.

## Pitfalls

- Folder names are not note content. A subscription list under
  `Finance/Cleanup` may contain neither word in its text.
- `search_files` filename patterns do not discover parent folders. If the
  helper is unavailable on the vault's filesystem, list Markdown notes with
  `search_files`, `target: "files"`, `pattern: "*.md"`, and the vault path.
  Inspect returned full paths, then scope content searches to the matching
  folder. Use `target: "content"` and `file_glob: "*.md"` for content regexes.
- During discovery, the helper skips hidden descendants and symlinks. Unreadable paths are
  reported in `skipped_files` (first 20) and `skipped_count`.
- Task extraction excludes checked/custom-status boxes, empty checkboxes,
  and fenced code examples. It returns Markdown checkbox tasks, not plugin
  queries or generated views.
- If matching paths have no unchecked tasks, the helper keeps that scope;
  it does not silently widen to unrelated notes.

## Verification

Preserve source paths and line numbers when reporting tasks. Check truncation
and skipped files before saying the list is complete. After an edit, verify
the affected note with `read_file`.
