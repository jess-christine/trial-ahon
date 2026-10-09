"""Execute a repository SQL file with the bundle-selected catalog."""

from __future__ import annotations

import argparse
import os
import re
import inspect
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
    return re.sub(r"\bahon\.", f"{catalog}.", sql_text).replace(
        "/Volumes/ahon/reference/source", source_volume.rstrip("/")
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--sql-file", required=True)
    args = parser.parse_args()
    catalog = os.environ.get("AHON_CATALOG", "ahon")
    source_volume = os.environ.get(
        "AHON_SOURCE_VOLUME", f"/Volumes/{catalog}/reference/source"
    )
    sql_path = Path(args.sql_file)
    if not sql_path.is_absolute():
        sql_path = Path(inspect.currentframe().f_code.co_filename).resolve().parents[3] / sql_path
    sql_text = sql_path.read_text(encoding="utf-8")
    sql_text = render_sql(sql_text, catalog, source_volume)

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    for index, statement in enumerate(split_statements(sql_text), start=1):
        print(f"Running {sql_path.name} statement {index}")
        spark.sql(statement)


if __name__ == "__main__":
    main()
