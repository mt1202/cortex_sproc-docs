from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Any, Dict, Optional


DOCUMENTATION_VERSION = "v1.0"


def to_iso(value: Any) -> Optional[str]:
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def build_prompt(row: Dict[str, Any], skill_name: str) -> str:
    metadata = {
        "catalog_name": row["CATALOG_NAME"],
        "schema_name": row["SCHEMA_NAME"],
        "procedure_name": row["PROCEDURE_NAME"],
        "arguments": row["ARGUMENTS"],
        "object_type": row["OBJECT_TYPE"],
        "change_hash": row["CHANGE_HASH"],
        "created_on": to_iso(row.get("CREATED_ON")),
        "source_effective_from": to_iso(row.get("SOURCE_EFFECTIVE_FROM")),
        "source_effective_to": to_iso(row.get("SOURCE_EFFECTIVE_TO")),
        "source_is_current": bool(row.get("SOURCE_IS_CURRENT", True)),
        "procedure_fqn": row["PROCEDURE_FQN"],
        "procedure_signature": row["PROCEDURE_SIGNATURE"],
        "procedure_id": row["PROCEDURE_ID"],
        "version_rank": int(row.get("VERSION_RANK") or 1),
        "is_latest_source_version": True,
        "is_latest_doc_version": True,
        "is_current_documentation": True,
    }

    metadata_json = json.dumps(metadata, indent=2, default=str)
    ddl_text = row["PROCEDURE_DDL"] or ""

    return f"""Use the "{skill_name}" skill.

Analyze the Snowflake stored procedure below.

Return EXACTLY this format and nothing else:

===BEGIN_JSON===
<one valid JSON object only>
===END_JSON===
===BEGIN_MARKDOWN===
<markdown only>
===END_MARKDOWN===

Important:
- The JSON must follow the required contract for documentation_version "{DOCUMENTATION_VERSION}".
- The JSON source block must preserve the metadata exactly as provided.
- The markdown must match the JSON.
- Do not wrap the JSON in markdown fences.
- Do not wrap the markdown in markdown fences.
- Do not add commentary before, between, or after the marker blocks.

METADATA:
{metadata_json}

DDL:
{ddl_text}
"""


def call_cortex_cli(
    prompt_text: str,
    connection_name: str,
    workdir: Path,
    model_name: str,
    keep_prompt_files: bool = False,
    bypass: bool = True,
) -> str:
    cmd = [
        "cortex",
        "-c",
        connection_name,
        "-w",
        str(workdir),
        "-m",
        str(model_name),
        "-p",
        prompt_text,
    ]

    if bypass:
        cmd.append("--bypass")

    completed = subprocess.run(
        cmd,
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )

    if completed.returncode != 0:
        raise RuntimeError(
            f"Cortex CLI failed with exit code {completed.returncode}. "
            f"STDERR: {completed.stderr.strip()}"
        )

    stdout = completed.stdout.strip()
    if not stdout:
        raise RuntimeError("Cortex CLI returned empty stdout")

    return stdout