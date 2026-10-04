"""Read-only, path-first note discovery and unchecked task extraction."""

import argparse
import json
import os
import re
from pathlib import Path


TASK = re.compile(r"^\s*(?:[-+*]|\d+[.)])\s+\[ \]\s+(\S.*?)\s*$")
FENCE = re.compile(r"^\s*(`{3,}|~{3,})")


def unchecked_tasks(text):
    """Yield original line numbers and task text, excluding fenced examples."""
    fence = ""
    for number, line in enumerate(text.splitlines(), 1):
        if fence:
            stripped = line.strip()
            if len(stripped) >= len(fence) and set(stripped) == {fence[0]}:
                fence = ""
            continue
        opening = FENCE.match(line)
        if opening:
            fence = opening.group(1)
            continue
        task = TASK.match(line)
        if task:
            yield number, task.group(1)


def search_vault(vault, query="", scope="", tasks=False, limit=100, offset=0):
    vault = Path(vault).expanduser().resolve()
    if not vault.is_dir():
        raise ValueError(f"Vault is not a directory: {vault}")
    root = (vault / scope).resolve()
    if not root.is_relative_to(vault):
        raise ValueError("Search path must be inside the vault")
    if not root.exists():
        raise ValueError(f"Search path does not exist: {scope}")
    if limit < 1 or offset < 0:
        raise ValueError("Limit must be positive and offset must be nonnegative")

    skipped = []

    def walk_error(error):
        skipped.append(str(Path(error.filename).relative_to(vault)))

    paths = []
    if root.is_file():
        if root.suffix.casefold() == ".md":
            paths.append(root)
    else:
        for directory, dirs, files in os.walk(root, onerror=walk_error):
            dirs[:] = sorted(
                name for name in dirs
                if not name.startswith(".") and not (Path(directory) / name).is_symlink()
            )
            paths.extend(
                Path(directory) / name for name in files
                if not name.startswith(".") and name.casefold().endswith(".md")
                and not (Path(directory) / name).is_symlink()
            )
    paths.sort(key=lambda path: path.relative_to(vault).as_posix().casefold())
    terms = query.casefold().split()
    candidates = [
        path for path in paths
        if all(term in path.relative_to(vault).as_posix().casefold() for term in terms)
    ]
    matched_by = "path" if terms else "scope"
    if not candidates and terms:
        matched_by = "content"
        candidates = paths

    results = []
    matched_files = 0
    total = 0
    for path in candidates:
        relative = path.relative_to(vault).as_posix()
        text = ""
        if tasks or matched_by == "content":
            try:
                text = path.read_text(encoding="utf-8-sig")
            except (OSError, UnicodeError):
                skipped.append(relative)
                continue
        if matched_by == "content" and not all(term in text.casefold() for term in terms):
            continue
        matched_files += 1
        rows = (
            ({"path": relative, "line": number, "text": content}
             for number, content in unchecked_tasks(text))
            if tasks else iter([{"path": relative}])
        )
        for row in rows:
            if offset <= total < offset + limit:
                results.append(row)
            total += 1

    return {
        "vault": str(vault),
        "scope": root.relative_to(vault).as_posix(),
        "matched_by": matched_by,
        "matched_files": matched_files,
        "results": results,
        "total": total,
        "offset": offset,
        "limit": limit,
        "truncated": offset + len(results) < total,
        "skipped_files": skipped[:20],
        "skipped_count": len(skipped),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--vault", required=True, help="Local vault directory")
    parser.add_argument("--query", default="", help="Literal topic words, matched case-insensitively")
    parser.add_argument("--path", default="", help="Restrict to a vault-relative folder or note")
    parser.add_argument("--tasks", action="store_true", help="Return nonblank unchecked tasks with source lines")
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--offset", type=int, default=0)
    args = parser.parse_args()
    try:
        result = search_vault(
            args.vault, args.query, args.path, args.tasks, args.limit, args.offset
        )
    except (OSError, ValueError) as error:
        print(json.dumps({"error": str(error)}, ensure_ascii=False))
        return 1
    print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
