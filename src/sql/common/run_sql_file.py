"""Execute repository SQL or Python with explicit bundle configuration."""

from __future__ import annotations

import argparse
import inspect
import json
import os
import re
import runpy
import sys
from pathlib import Path


def split_statements(sql_text: str) -> list[str]:
    """Split SQL on semicolons outside quoted strings and comments."""
    statements: list[str] = []
    buffer: list[str] = []
    quote: str | None = None
    line_comment = False
    block_comment = False
    index = 0
    while index < len(sql_text):
        char = sql_text[index]
        following = sql_text[index + 1] if index + 1 < len(sql_text) else ""
        if line_comment:
            buffer.append(char)
            if char == "\n":
                line_comment = False
        elif block_comment:
            buffer.append(char)
            if char == "*" and following == "/":
                buffer.append(following)
                index += 1
                block_comment = False
        elif quote:
            buffer.append(char)
            if char == quote:
                if following == quote:
                    buffer.append(following)
                    index += 1
                else:
                    quote = None
        elif char == "-" and following == "-":
            buffer.extend((char, following))
            index += 1
            line_comment = True
        elif char == "/" and following == "*":
            buffer.extend((char, following))
            index += 1
            block_comment = True
        elif char in ("'", '"', "`"):
            quote = char
            buffer.append(char)
        elif char == ";":
            statement = "".join(buffer).strip()
            if statement:
                statements.append(statement)
            buffer.clear()
        else:
            buffer.append(char)
        index += 1
    if quote or block_comment:
        raise ValueError("SQL file ends inside a quoted string or block comment")
    tail = "".join(buffer).strip()
    if tail:
        statements.append(tail)
    return statements


def render_sql(sql_text: str, catalog: str, source_volume: str) -> str:
    """Apply bundle-selected catalog and source-volume settings to SQL."""
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", catalog):
        raise ValueError("AHON_CATALOG must be a simple SQL identifier")
    rendered = re.sub(r"\bahon\.", f"{catalog}.", sql_text)
    rendered = re.sub(
        r"(?i)(\b(?:CREATE\s+CATALOG\s+IF\s+NOT\s+EXISTS|SHOW\s+SCHEMAS\s+IN)\s+)ahon\b",
        rf"\g<1>{catalog}",
        rendered,
    )
    return rendered.replace(
        "/Volumes/ahon/reference/source", source_volume.rstrip("/")
    )


def resolve_sql_path(
    sql_file: str,
    repository_root: str | None,
    script_file: str | None,
    working_directory: str | None = None,
) -> Path:
    """Resolve task paths when Databricks does not define ``__file__``."""
    path = Path(sql_file)
    if path.is_absolute():
        return path
    candidates: list[Path] = []
    if repository_root:
        candidates.append(Path(repository_root) / path)
    if script_file:
        candidates.extend(
            parent / path for parent in Path(script_file).resolve().parents
        )
    if working_directory:
        candidates.append(Path(working_directory) / path)
    for candidate in candidates:
        if candidate.is_file():
            return candidate
    raise RuntimeError(
        "Could not locate task file. Checked: "
        + ", ".join(str(candidate) for candidate in candidates)
    )


def parse_task_config(config_json: str) -> dict[str, str]:
    """Accept non-secret AHON settings without relying on cluster env vars."""
    config = json.loads(config_json)
    if not isinstance(config, dict) or any(
        not isinstance(key, str)
        or not re.fullmatch(r"AHON_[A-Z0-9_]+", key)
        or not isinstance(value, str)
        for key, value in config.items()
    ):
        raise ValueError("Task config must contain AHON_ keys and string values")
    return config


def execute_python(path: Path) -> None:
    # Each existing script keeps its own entry point and local sibling imports.
    # Runner flags must not leak into a child's argparse parser.
    original_argv = sys.argv
    original_path = sys.path[:]
    try:
        sys.argv = [str(path)]
        sys.path.insert(0, str(path.parent))
        runpy.run_path(str(path), run_name="__main__")
    finally:
        sys.argv = original_argv
        sys.path[:] = original_path


def main() -> None:
    parser = argparse.ArgumentParser()
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--sql-file")
    source.add_argument("--python-file")
    parser.add_argument("--config-json", default="{}")
    args = parser.parse_args()
    os.environ.update(parse_task_config(args.config_json))
    catalog = os.environ.get("AHON_CATALOG", "ahon")
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", catalog):
        raise ValueError("AHON_CATALOG must be a simple SQL identifier")
    path = resolve_sql_path(
        args.sql_file or args.python_file,
        os.environ.get("AHON_REPOSITORY_ROOT"),
        globals().get("__file__") or globals().get("filename") or inspect.currentframe().f_code.co_filename,
        os.getcwd(),
    )
    if args.python_file:
        execute_python(path)
        return
    source_volume = os.environ.get(
        "AHON_SOURCE_VOLUME", f"/Volumes/{catalog}/reference/source"
    )
    sql_text = render_sql(path.read_text(encoding="utf-8"), catalog, source_volume)

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    for index, statement in enumerate(split_statements(sql_text), start=1):
        print(f"Running {path.name} statement {index}")
        spark.sql(statement)


if __name__ == "__main__":
    main()
