from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

import snowflake.connector
from snowflake.connector import DictCursor

from runner.cortex_client import build_prompt, call_cortex_cli
from runner.parser import parse_cortex_response, safe_list

DOCUMENTATION_VERSION = "v1.0"
TARGET_TABLE = "PROD_TELEMETRY_DB.BACKUP.PROCEDURE_DOCUMENTATION"
SOURCE_TABLE = "PROD_TELEMETRY_DB.BACKUP.DDL_HISTORY_PROCEDURES"

CATALOG_QUERY = f"""
WITH latest_source AS (
    SELECT
        h.CATALOG_NAME,
        h.SCHEMA_NAME,
        h.PROCEDURE_NAME,
        h.ARGUMENTS,
        h.OBJECT_TYPE,
        h.CREATED_ON,
        h.EFFECTIVE_FROM,
        h.EFFECTIVE_TO,
        h.IS_CURRENT,
        h.PROCEDURE_DDL,
        h.CHANGE_HASH,
        ROW_NUMBER() OVER (
            PARTITION BY h.CATALOG_NAME, h.SCHEMA_NAME, h.PROCEDURE_NAME, h.ARGUMENTS, h.OBJECT_TYPE
            ORDER BY h.EFFECTIVE_FROM DESC, h.CREATED_ON DESC
        ) AS VERSION_RANK
    FROM {SOURCE_TABLE} h
    WHERE h.IS_CURRENT = TRUE
),
latest_docs AS (
    SELECT
        d.CATALOG_NAME,
        d.SCHEMA_NAME,
        d.PROCEDURE_NAME,
        d.ARGUMENTS,
        d.OBJECT_TYPE,
        d.CHANGE_HASH,
        d.DOCUMENTATION_VERSION,
        d.DOCUMENTATION_STATUS,
        d.DOCUMENTED_AT,
        d.LANGUAGE,
        d.RETURNS_TYPE,
        d.IDEMPOTENCY_CLASSIFICATION,
        d.USES_DYNAMIC_SQL,
        d.RISK_COUNT,
        d.SUMMARY,
        d.MARKDOWN_DOC,
        d.DOCUMENTATION_JSON,
        ROW_NUMBER() OVER (
            PARTITION BY d.CATALOG_NAME, d.SCHEMA_NAME, d.PROCEDURE_NAME, d.ARGUMENTS, d.OBJECT_TYPE, d.CHANGE_HASH, d.DOCUMENTATION_VERSION
            ORDER BY d.DOCUMENTED_AT DESC
        ) AS DOC_RANK
    FROM {TARGET_TABLE} d
    WHERE d.DOCUMENTATION_VERSION = %s
)
SELECT
    s.CATALOG_NAME,
    s.SCHEMA_NAME,
    s.PROCEDURE_NAME,
    s.ARGUMENTS,
    s.OBJECT_TYPE,
    s.CREATED_ON,
    s.EFFECTIVE_FROM AS SOURCE_EFFECTIVE_FROM,
    s.EFFECTIVE_TO AS SOURCE_EFFECTIVE_TO,
    s.IS_CURRENT AS SOURCE_IS_CURRENT,
    s.PROCEDURE_DDL,
    s.CHANGE_HASH,
    s.VERSION_RANK,
    s.CATALOG_NAME || '.' || s.SCHEMA_NAME || '.' || s.PROCEDURE_NAME AS PROCEDURE_FQN,
    s.CATALOG_NAME || '|' || s.SCHEMA_NAME || '|' || s.PROCEDURE_NAME || '|' || s.ARGUMENTS || '|' || s.OBJECT_TYPE AS PROCEDURE_ID,
    d.DOCUMENTATION_STATUS,
    d.DOCUMENTED_AT,
    d.LANGUAGE,
    d.RETURNS_TYPE,
    d.IDEMPOTENCY_CLASSIFICATION,
    d.USES_DYNAMIC_SQL,
    d.RISK_COUNT,
    d.SUMMARY,
    d.MARKDOWN_DOC,
    d.DOCUMENTATION_JSON,
    CASE WHEN d.CHANGE_HASH IS NOT NULL THEN TRUE ELSE FALSE END AS HAS_DOCUMENTATION
FROM latest_source s
LEFT JOIN latest_docs d
    ON  s.CATALOG_NAME = d.CATALOG_NAME
    AND s.SCHEMA_NAME = d.SCHEMA_NAME
    AND s.PROCEDURE_NAME = d.PROCEDURE_NAME
    AND s.ARGUMENTS = d.ARGUMENTS
    AND s.OBJECT_TYPE = d.OBJECT_TYPE
    AND s.CHANGE_HASH = d.CHANGE_HASH
    AND d.DOC_RANK = 1
ORDER BY s.CATALOG_NAME, s.SCHEMA_NAME, s.PROCEDURE_NAME, s.ARGUMENTS
"""

INSERT_SQL = f"""
INSERT INTO {TARGET_TABLE} (
    CATALOG_NAME, SCHEMA_NAME, PROCEDURE_NAME, ARGUMENTS, OBJECT_TYPE, CHANGE_HASH,
    PROCEDURE_FQN, PROCEDURE_SIGNATURE, PROCEDURE_ID,
    CREATED_ON, DOCUMENTED_AT, SOURCE_EFFECTIVE_FROM, SOURCE_EFFECTIVE_TO, SOURCE_IS_CURRENT,
    DOCUMENTATION_STATUS, DOCUMENTATION_MODEL, DOCUMENTATION_VERSION,
    VERSION_RANK, IS_LATEST_SOURCE_VERSION, IS_LATEST_DOC_VERSION, IS_CURRENT_DOCUMENTATION,
    LANGUAGE, RETURNS_TYPE, EXECUTE_AS, HANDLER, RUNTIME_VERSION, PACKAGES_JSON,
    SUMMARY, BUSINESS_PURPOSE,
    IDEMPOTENCY_CLASSIFICATION, IDEMPOTENCY_EXPLANATION, IDEMPOTENCY_EVIDENCE_JSON, IDEMPOTENCY_ASSUMPTIONS_JSON,
    USES_DYNAMIC_SQL, DYNAMIC_SQL_NOTES, ERROR_HANDLING, SECURITY_NOTES,
    PARAMETERS_JSON, READS_FROM_JSON, WRITES_TO_JSON, CALLS_JSON, CREATES_OBJECTS_JSON,
    LOGIC_STEPS_JSON, STEP_BY_STEP_JSON, RISKS_JSON, OPEN_QUESTIONS_JSON,
    READ_COUNT, WRITE_COUNT, CALL_COUNT, CREATE_OBJECT_COUNT, RISK_COUNT, OPEN_QUESTION_COUNT, STEP_COUNT,
    DOCUMENTATION_JSON, MARKDOWN_DOC
)
SELECT
    %(CATALOG_NAME)s, %(SCHEMA_NAME)s, %(PROCEDURE_NAME)s, %(ARGUMENTS)s, %(OBJECT_TYPE)s, %(CHANGE_HASH)s,
    %(PROCEDURE_FQN)s, %(PROCEDURE_SIGNATURE)s, %(PROCEDURE_ID)s,
    %(CREATED_ON)s, CURRENT_TIMESTAMP(), %(SOURCE_EFFECTIVE_FROM)s, %(SOURCE_EFFECTIVE_TO)s, %(SOURCE_IS_CURRENT)s,
    %(DOCUMENTATION_STATUS)s, %(DOCUMENTATION_MODEL)s, %(DOCUMENTATION_VERSION)s,
    %(VERSION_RANK)s, %(IS_LATEST_SOURCE_VERSION)s, %(IS_LATEST_DOC_VERSION)s, %(IS_CURRENT_DOCUMENTATION)s,
    %(LANGUAGE)s, %(RETURNS_TYPE)s, %(EXECUTE_AS)s, %(HANDLER)s, %(RUNTIME_VERSION)s, PARSE_JSON(%(PACKAGES_JSON)s),
    %(SUMMARY)s, %(BUSINESS_PURPOSE)s,
    %(IDEMPOTENCY_CLASSIFICATION)s, %(IDEMPOTENCY_EXPLANATION)s, PARSE_JSON(%(IDEMPOTENCY_EVIDENCE_JSON)s), PARSE_JSON(%(IDEMPOTENCY_ASSUMPTIONS_JSON)s),
    %(USES_DYNAMIC_SQL)s, %(DYNAMIC_SQL_NOTES)s, %(ERROR_HANDLING)s, %(SECURITY_NOTES)s,
    PARSE_JSON(%(PARAMETERS_JSON)s), PARSE_JSON(%(READS_FROM_JSON)s), PARSE_JSON(%(WRITES_TO_JSON)s), PARSE_JSON(%(CALLS_JSON)s), PARSE_JSON(%(CREATES_OBJECTS_JSON)s),
    PARSE_JSON(%(LOGIC_STEPS_JSON)s), PARSE_JSON(%(STEP_BY_STEP_JSON)s), PARSE_JSON(%(RISKS_JSON)s), PARSE_JSON(%(OPEN_QUESTIONS_JSON)s),
    %(READ_COUNT)s, %(WRITE_COUNT)s, %(CALL_COUNT)s, %(CREATE_OBJECT_COUNT)s, %(RISK_COUNT)s, %(OPEN_QUESTION_COUNT)s, %(STEP_COUNT)s,
    PARSE_JSON(%(DOCUMENTATION_JSON)s), %(MARKDOWN_DOC)s
"""

FAILURE_INSERT_SQL = f"""
INSERT INTO {TARGET_TABLE} (
    CATALOG_NAME, SCHEMA_NAME, PROCEDURE_NAME, ARGUMENTS, OBJECT_TYPE, CHANGE_HASH,
    PROCEDURE_FQN, PROCEDURE_SIGNATURE, PROCEDURE_ID,
    CREATED_ON, DOCUMENTED_AT, SOURCE_EFFECTIVE_FROM, SOURCE_EFFECTIVE_TO, SOURCE_IS_CURRENT,
    DOCUMENTATION_STATUS, DOCUMENTATION_MODEL, DOCUMENTATION_VERSION,
    VERSION_RANK, IS_LATEST_SOURCE_VERSION, IS_LATEST_DOC_VERSION, IS_CURRENT_DOCUMENTATION,
    LANGUAGE, RETURNS_TYPE, EXECUTE_AS, HANDLER, RUNTIME_VERSION, PACKAGES_JSON,
    SUMMARY, BUSINESS_PURPOSE,
    IDEMPOTENCY_CLASSIFICATION, IDEMPOTENCY_EXPLANATION, IDEMPOTENCY_EVIDENCE_JSON, IDEMPOTENCY_ASSUMPTIONS_JSON,
    USES_DYNAMIC_SQL, DYNAMIC_SQL_NOTES, ERROR_HANDLING, SECURITY_NOTES,
    PARAMETERS_JSON, READS_FROM_JSON, WRITES_TO_JSON, CALLS_JSON, CREATES_OBJECTS_JSON,
    LOGIC_STEPS_JSON, STEP_BY_STEP_JSON, RISKS_JSON, OPEN_QUESTIONS_JSON,
    READ_COUNT, WRITE_COUNT, CALL_COUNT, CREATE_OBJECT_COUNT, RISK_COUNT, OPEN_QUESTION_COUNT, STEP_COUNT,
    DOCUMENTATION_JSON, MARKDOWN_DOC
)
SELECT
    %(CATALOG_NAME)s, %(SCHEMA_NAME)s, %(PROCEDURE_NAME)s, %(ARGUMENTS)s, %(OBJECT_TYPE)s, %(CHANGE_HASH)s,
    %(PROCEDURE_FQN)s, %(PROCEDURE_SIGNATURE)s, %(PROCEDURE_ID)s,
    %(CREATED_ON)s, CURRENT_TIMESTAMP(), %(SOURCE_EFFECTIVE_FROM)s, %(SOURCE_EFFECTIVE_TO)s, %(SOURCE_IS_CURRENT)s,
    'FAILED', %(DOCUMENTATION_MODEL)s, %(DOCUMENTATION_VERSION)s,
    %(VERSION_RANK)s, %(IS_LATEST_SOURCE_VERSION)s, %(IS_LATEST_DOC_VERSION)s, %(IS_CURRENT_DOCUMENTATION)s,
    NULL, NULL, NULL, NULL, NULL, PARSE_JSON('[]'),
    %(SUMMARY)s, NULL,
    'UNKNOWN', %(IDEMPOTENCY_EXPLANATION)s, PARSE_JSON('[]'), PARSE_JSON('[]'),
    NULL, NULL, %(ERROR_HANDLING)s, NULL,
    PARSE_JSON('[]'), PARSE_JSON('[]'), PARSE_JSON('[]'), PARSE_JSON('[]'), PARSE_JSON('[]'),
    PARSE_JSON('[]'), PARSE_JSON('[]'), PARSE_JSON('[]'), PARSE_JSON('[]'),
    0, 0, 0, 0, 0, 0, 0,
    PARSE_JSON(%(DOCUMENTATION_JSON)s), %(MARKDOWN_DOC)s
"""

DELETE_DOC_SQL = f"""
DELETE FROM {TARGET_TABLE}
WHERE CATALOG_NAME = %(CATALOG_NAME)s
  AND SCHEMA_NAME = %(SCHEMA_NAME)s
  AND PROCEDURE_NAME = %(PROCEDURE_NAME)s
  AND ARGUMENTS = %(ARGUMENTS)s
  AND OBJECT_TYPE = %(OBJECT_TYPE)s
  AND CHANGE_HASH = %(CHANGE_HASH)s
  AND DOCUMENTATION_VERSION = %(DOCUMENTATION_VERSION)s
"""

def delete_documentation_for_current_version(
    conn,
    row: Dict[str, Any],
) -> int:
    params = {
        "CATALOG_NAME": row["CATALOG_NAME"],
        "SCHEMA_NAME": row["SCHEMA_NAME"],
        "PROCEDURE_NAME": row["PROCEDURE_NAME"],
        "ARGUMENTS": row["ARGUMENTS"],
        "OBJECT_TYPE": row["OBJECT_TYPE"],
        "CHANGE_HASH": row["CHANGE_HASH"],
        "DOCUMENTATION_VERSION": DOCUMENTATION_VERSION,
    }
    with conn.cursor() as cur:
        cur.execute(DELETE_DOC_SQL, params)
        deleted = cur.rowcount
    conn.commit()
    return deleted


@dataclass
class JobResult:
    total_selected: int
    documented: int
    failed: int
    failures: List[Dict[str, str]]


def get_python_connection(conn_params: Dict[str, Any]):
    return snowflake.connector.connect(**conn_params)


def fetch_rows(conn, sql: str, params=None) -> List[Dict[str, Any]]:
    with conn.cursor(DictCursor) as cur:
        cur.execute(sql, params)
        return list(cur.fetchall())


def execute_sql(conn, sql: str, params=None) -> None:
    with conn.cursor() as cur:
        cur.execute(sql, params)


def extract_argument_tail(arguments: Optional[str]) -> str:
    if not arguments:
        return "()"

    text = str(arguments).strip()
    text = re.sub(r"^[^(]+", "", text).strip()

    if not text.startswith("("):
        return text

    return text


def build_display_signature(row: Dict[str, Any]) -> str:
    procedure_fqn = row["PROCEDURE_FQN"]
    arg_tail = extract_argument_tail(row.get("ARGUMENTS"))
    return f"{procedure_fqn}{arg_tail}"


def enrich_catalog_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    enriched: List[Dict[str, Any]] = []
    for row in rows:
        new_row = dict(row)
        new_row["PROCEDURE_SIGNATURE"] = build_display_signature(new_row)
        enriched.append(new_row)
    return enriched


def list_procedure_catalog(conn) -> List[Dict[str, Any]]:
    rows = fetch_rows(conn, CATALOG_QUERY, (DOCUMENTATION_VERSION,))
    return enrich_catalog_rows(rows)


def get_catalog_row_by_id(
    conn,
    procedure_id: str,
) -> Optional[Dict[str, Any]]:
    rows = list_procedure_catalog(conn)
    for row in rows:
        if row.get("PROCEDURE_ID") == procedure_id:
            return row
    return None


def build_insert_payload(
    row: Dict[str, Any],
    doc_json: Dict[str, Any],
    markdown_doc: str,
    documentation_model_label: str,
) -> Dict[str, Any]:
    source = doc_json.get("source", {})
    technical = doc_json.get("technical_metadata", {})
    idempotency = doc_json.get("idempotency", {})
    dynamic_sql = doc_json.get("dynamic_sql", {})

    parameters = safe_list(doc_json.get("parameters"))
    reads_from = safe_list(doc_json.get("reads_from"))
    writes_to = safe_list(doc_json.get("writes_to"))
    calls = safe_list(doc_json.get("calls"))
    creates_objects = safe_list(doc_json.get("creates_objects"))
    logic_steps = safe_list(doc_json.get("logic_steps"))
    step_by_step = safe_list(doc_json.get("step_by_step"))
    risks = safe_list(doc_json.get("risks"))
    open_questions = safe_list(doc_json.get("open_questions"))

    return {
        "CATALOG_NAME": source.get("catalog_name", row["CATALOG_NAME"]),
        "SCHEMA_NAME": source.get("schema_name", row["SCHEMA_NAME"]),
        "PROCEDURE_NAME": source.get("procedure_name", row["PROCEDURE_NAME"]),
        "ARGUMENTS": source.get("arguments", row["ARGUMENTS"]),
        "OBJECT_TYPE": source.get("object_type", row["OBJECT_TYPE"]),
        "CHANGE_HASH": source.get("change_hash", row["CHANGE_HASH"]),
        "PROCEDURE_FQN": source.get("procedure_fqn", row["PROCEDURE_FQN"]),
        "PROCEDURE_SIGNATURE": source.get("procedure_signature", row["PROCEDURE_SIGNATURE"]),
        "PROCEDURE_ID": source.get("procedure_id", row["PROCEDURE_ID"]),
        "CREATED_ON": source.get("created_on") or row.get("CREATED_ON"),
        "SOURCE_EFFECTIVE_FROM": source.get("source_effective_from") or row.get("SOURCE_EFFECTIVE_FROM"),
        "SOURCE_EFFECTIVE_TO": source.get("source_effective_to") or row.get("SOURCE_EFFECTIVE_TO"),
        "SOURCE_IS_CURRENT": bool(source.get("source_is_current", True)),
        "DOCUMENTATION_STATUS": "DOCUMENTED",
        "DOCUMENTATION_MODEL": documentation_model_label,
        "DOCUMENTATION_VERSION": DOCUMENTATION_VERSION,
        "VERSION_RANK": int(source.get("version_rank", row.get("VERSION_RANK") or 1)),
        "IS_LATEST_SOURCE_VERSION": bool(source.get("is_latest_source_version", True)),
        "IS_LATEST_DOC_VERSION": bool(source.get("is_latest_doc_version", True)),
        "IS_CURRENT_DOCUMENTATION": bool(source.get("is_current_documentation", True)),
        "LANGUAGE": technical.get("language"),
        "RETURNS_TYPE": technical.get("returns_type"),
        "EXECUTE_AS": technical.get("execute_as"),
        "HANDLER": technical.get("handler"),
        "RUNTIME_VERSION": technical.get("runtime_version"),
        "PACKAGES_JSON": json.dumps(safe_list(technical.get("packages"))),
        "SUMMARY": doc_json.get("summary"),
        "BUSINESS_PURPOSE": doc_json.get("business_purpose"),
        "IDEMPOTENCY_CLASSIFICATION": idempotency.get("classification"),
        "IDEMPOTENCY_EXPLANATION": idempotency.get("explanation"),
        "IDEMPOTENCY_EVIDENCE_JSON": json.dumps(safe_list(idempotency.get("evidence"))),
        "IDEMPOTENCY_ASSUMPTIONS_JSON": json.dumps(safe_list(idempotency.get("assumptions"))),
        "USES_DYNAMIC_SQL": dynamic_sql.get("uses_dynamic_sql"),
        "DYNAMIC_SQL_NOTES": dynamic_sql.get("notes"),
        "ERROR_HANDLING": doc_json.get("error_handling"),
        "SECURITY_NOTES": doc_json.get("security_notes"),
        "PARAMETERS_JSON": json.dumps(parameters),
        "READS_FROM_JSON": json.dumps(reads_from),
        "WRITES_TO_JSON": json.dumps(writes_to),
        "CALLS_JSON": json.dumps(calls),
        "CREATES_OBJECTS_JSON": json.dumps(creates_objects),
        "LOGIC_STEPS_JSON": json.dumps(logic_steps),
        "STEP_BY_STEP_JSON": json.dumps(step_by_step),
        "RISKS_JSON": json.dumps(risks),
        "OPEN_QUESTIONS_JSON": json.dumps(open_questions),
        "READ_COUNT": len(reads_from),
        "WRITE_COUNT": len(writes_to),
        "CALL_COUNT": len(calls),
        "CREATE_OBJECT_COUNT": len(creates_objects),
        "RISK_COUNT": len(risks),
        "OPEN_QUESTION_COUNT": len(open_questions),
        "STEP_COUNT": len(step_by_step),
        "DOCUMENTATION_JSON": json.dumps(doc_json),
        "MARKDOWN_DOC": markdown_doc,
    }


def build_failure_payload(
    row: Dict[str, Any],
    documentation_model_label: str,
    error_message: str,
) -> Dict[str, Any]:
    failure_json = {
        "documentation_version": DOCUMENTATION_VERSION,
        "source": {
            "catalog_name": row["CATALOG_NAME"],
            "schema_name": row["SCHEMA_NAME"],
            "procedure_name": row["PROCEDURE_NAME"],
            "arguments": row["ARGUMENTS"],
            "object_type": row["OBJECT_TYPE"],
            "change_hash": row["CHANGE_HASH"],
            "created_on": str(row.get("CREATED_ON")),
            "source_effective_from": str(row.get("SOURCE_EFFECTIVE_FROM")),
            "source_effective_to": str(row.get("SOURCE_EFFECTIVE_TO")),
            "source_is_current": bool(row.get("SOURCE_IS_CURRENT", True)),
            "procedure_fqn": row["PROCEDURE_FQN"],
            "procedure_signature": row["PROCEDURE_SIGNATURE"],
            "procedure_id": row["PROCEDURE_ID"],
            "version_rank": int(row.get("VERSION_RANK") or 1),
            "is_latest_source_version": True,
            "is_latest_doc_version": True,
            "is_current_documentation": True,
        },
        "failure": {
            "message": error_message,
            "failed_at_utc": datetime.now(timezone.utc).isoformat(),
        },
    }

    return {
        "CATALOG_NAME": row["CATALOG_NAME"],
        "SCHEMA_NAME": row["SCHEMA_NAME"],
        "PROCEDURE_NAME": row["PROCEDURE_NAME"],
        "ARGUMENTS": row["ARGUMENTS"],
        "OBJECT_TYPE": row["OBJECT_TYPE"],
        "CHANGE_HASH": row["CHANGE_HASH"],
        "PROCEDURE_FQN": row["PROCEDURE_FQN"],
        "PROCEDURE_SIGNATURE": row["PROCEDURE_SIGNATURE"],
        "PROCEDURE_ID": row["PROCEDURE_ID"],
        "CREATED_ON": row.get("CREATED_ON"),
        "SOURCE_EFFECTIVE_FROM": row.get("SOURCE_EFFECTIVE_FROM"),
        "SOURCE_EFFECTIVE_TO": row.get("SOURCE_EFFECTIVE_TO"),
        "SOURCE_IS_CURRENT": bool(row.get("SOURCE_IS_CURRENT", True)),
        "DOCUMENTATION_MODEL": documentation_model_label,
        "DOCUMENTATION_VERSION": DOCUMENTATION_VERSION,
        "VERSION_RANK": int(row.get("VERSION_RANK") or 1),
        "IS_LATEST_SOURCE_VERSION": True,
        "IS_LATEST_DOC_VERSION": True,
        "IS_CURRENT_DOCUMENTATION": True,
        "SUMMARY": "Documentation generation failed.",
        "IDEMPOTENCY_EXPLANATION": error_message,
        "ERROR_HANDLING": error_message,
        "DOCUMENTATION_JSON": json.dumps(failure_json),
        "MARKDOWN_DOC": f"# {row['PROCEDURE_SIGNATURE']}\n\n## Documentation Failure\n{error_message}\n",
    }


def insert_documentation(conn, payload: Dict[str, Any]) -> None:
    with conn.cursor() as cur:
        cur.execute(INSERT_SQL, payload)


def insert_failure(conn, payload: Dict[str, Any]) -> None:
    with conn.cursor() as cur:
        cur.execute(FAILURE_INSERT_SQL, payload)


def delete_documentation_for_current_version(
    conn,
    row: Dict[str, Any],
) -> int:
    params = {
        "CATALOG_NAME": row["CATALOG_NAME"],
        "SCHEMA_NAME": row["SCHEMA_NAME"],
        "PROCEDURE_NAME": row["PROCEDURE_NAME"],
        "ARGUMENTS": row["ARGUMENTS"],
        "OBJECT_TYPE": row["OBJECT_TYPE"],
        "CHANGE_HASH": row["CHANGE_HASH"],
        "DOCUMENTATION_VERSION": DOCUMENTATION_VERSION,
    }
    with conn.cursor() as cur:
        cur.execute(DELETE_DOC_SQL, params)
        deleted = cur.rowcount
    conn.commit()
    return deleted


def run_documentation_job(
    conn_params: Dict[str, Any],
    selected_connection_name: str,
    workdir: Path,
    model_name: str,
    skill_name: str,
    documentation_model_label: str,
    selected_procedure_id: str,
    dry_run: bool = False,
    bypass: bool = True,
    progress_callback: Optional[Callable[[str], None]] = None,
) -> JobResult:
    def log(msg: str) -> None:
        if progress_callback:
            progress_callback(msg)

    conn = get_python_connection(conn_params)
    documented = 0
    failed = 0
    failures: List[Dict[str, str]] = []

    try:
        row = get_catalog_row_by_id(conn, selected_procedure_id)
        if not row:
            raise RuntimeError("Selected procedure was not found in the current catalog.")

        total_selected = 1
        signature = row["PROCEDURE_SIGNATURE"]
        log(f"Selected {total_selected} procedure.")
        log(f"Processing {signature}")

        if dry_run:
            log("Dry run enabled. No documentation row will be written.")
            return JobResult(
                total_selected=1,
                documented=0,
                failed=0,
                failures=[],
            )

        try:
            prompt_text = build_prompt(row, skill_name)
            cortex_output = call_cortex_cli(
                prompt_text=prompt_text,
                connection_name=selected_connection_name,
                workdir=workdir,
                model_name=model_name,
                keep_prompt_files=False,
                bypass=bypass,
            )
            doc_json, markdown_doc = parse_cortex_response(cortex_output)
            payload = build_insert_payload(
                row=row,
                doc_json=doc_json,
                markdown_doc=markdown_doc,
                documentation_model_label=documentation_model_label,
            )
            insert_documentation(conn, payload)
            conn.commit()
            documented = 1
            log(f"Success: {signature}")
        except Exception as exc:
            failed = 1
            error_message = str(exc)
            failures.append({"procedure_signature": signature, "error": error_message})
            failure_payload = build_failure_payload(
                row=row,
                documentation_model_label=documentation_model_label,
                error_message=error_message,
            )
            insert_failure(conn, failure_payload)
            conn.commit()
            log(f"FAILED: {signature} -> {error_message}")

        return JobResult(
            total_selected=1,
            documented=documented,
            failed=failed,
            failures=failures,
        )
    finally:
        conn.close()