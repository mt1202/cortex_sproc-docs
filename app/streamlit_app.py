from __future__ import annotations

import json
import shutil
import subprocess
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import streamlit as st

from runner.connections import (
    get_connection_params,
    load_connections_toml,
    pick_connection_name,
)
from runner.job import (
    delete_documentation_for_current_version,
    get_python_connection,
    list_procedure_catalog,
    run_documentation_job,
)

APP_TITLE = "Stored Procedure Documentation Browser"
DEFAULT_SKILL_NAME = "sproc-documenter"
DEFAULT_MODEL_NAME = "auto"
DEFAULT_DOC_MODEL_LABEL = "cortex-cli:auto"


# ----------------------------
# Basic helpers
# ----------------------------

def check_wsl_interop() -> Tuple[bool, str]:
    marker = Path("/proc/sys/fs/binfmt_misc/WSLInterop")
    if not marker.exists():
        return False, "WSLInterop marker file not found"
    try:
        value = marker.read_text(encoding="utf-8").strip()
        return True, f"Marker present: {value}"
    except Exception as exc:
        return True, f"Marker present but could not read cleanly: {exc}"


def check_connections_file() -> Tuple[bool, str, Optional[Path]]:
    path = Path.home() / ".snowflake" / "connections.toml"
    if path.exists():
        return True, str(path), path
    return False, str(path), path


def check_skill_file(skill_name: str) -> Tuple[bool, str]:
    path = Path.home() / ".snowflake" / "cortex" / "skills" / f"{skill_name}.md"
    if path.exists():
        return True, str(path)
    return False, str(path)


def check_python() -> Tuple[bool, str]:
    python_path = shutil.which("python") or shutil.which("python3")
    if python_path:
        return True, python_path
    return False, "Python not found on PATH"


def check_cortex_details() -> Tuple[bool, str]:
    cortex_path = shutil.which("cortex")
    if not cortex_path:
        return False, "cortex CLI not found on PATH"

    try:
        version_proc = subprocess.run(
            [cortex_path, "--version"],
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
        )
        version_text = (version_proc.stdout or version_proc.stderr or "").strip()

        help_proc = subprocess.run(
            [cortex_path, "--help"],
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
        )
        help_text = (help_proc.stdout or "") + "\n" + (help_proc.stderr or "")
        supports_print = ("-p," in help_text) or ("--print" in help_text)

        return True, f"path={cortex_path} | version={version_text} | supports_-p={supports_print}"
    except Exception as exc:
        return False, f"Could not inspect cortex CLI: {exc}"


def load_available_connections(
    connections_file: Path,
) -> Tuple[List[str], Dict[str, Any], Optional[str], Optional[str]]:
    try:
        toml_data = load_connections_toml(connections_file)
    except Exception as exc:
        return [], {}, None, f"Could not load connections file: {exc}"

    names = [k for k, v in toml_data.items() if isinstance(v, dict)]
    try:
        preferred = pick_connection_name(None, toml_data)
    except Exception:
        preferred = None

    return names, toml_data, preferred, None


def get_selected_catalog_row() -> Optional[Dict[str, Any]]:
    rows = st.session_state.get("catalog_rows", [])
    selected_id = st.session_state.get("selected_procedure_id")
    if not rows or not selected_id:
        return None
    return next((row for row in rows if row.get("PROCEDURE_ID") == selected_id), None)


def normalize_json_value(value: Any) -> Any:
    if isinstance(value, str):
        try:
            return json.loads(value)
        except Exception:
            return value
    return value


def refresh_catalog_and_selected_row(
    toml_data: Dict[str, Any],
    selected_connection_name: str,
    selected_procedure_id: Optional[str],
) -> None:
    if not selected_connection_name:
        return

    conn = None
    try:
        conn_params = get_connection_params(toml_data, selected_connection_name)
        conn = get_python_connection(conn_params)
        rows = list_procedure_catalog(conn)
        st.session_state["catalog_rows"] = rows

        if selected_procedure_id:
            matching = next((row for row in rows if row.get("PROCEDURE_ID") == selected_procedure_id), None)
            if matching:
                st.session_state["selected_procedure_id"] = matching["PROCEDURE_ID"]
                st.session_state["selected_procedure_label"] = matching["PROCEDURE_SIGNATURE"]
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


# ----------------------------
# PDF export helper
# ----------------------------

def build_pdf_bytes(title: str, markdown_text: str) -> bytes:
    from reportlab.lib.pagesizes import letter
    from reportlab.lib.styles import getSampleStyleSheet
    from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer
    from reportlab.lib.units import inch

    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=letter)
    styles = getSampleStyleSheet()

    story = [Paragraph(title, styles["Title"]), Spacer(1, 0.2 * inch)]

    for line in markdown_text.splitlines():
        line = line.strip()
        if not line:
            story.append(Spacer(1, 0.1 * inch))
        else:
            safe_line = (
                line.replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;")
            )
            story.append(Paragraph(safe_line, styles["BodyText"]))

    doc.build(story)
    return buffer.getvalue()


# ----------------------------
# Diagnostics / top panels
# ----------------------------

def get_preflight_data(skill_name: str) -> Tuple[Optional[Path], List[str], Dict[str, Any], Optional[str], List[Tuple[str, bool, str]]]:
    conn_ok, conn_detail, conn_path = check_connections_file()
    connection_names: List[str] = []
    toml_data: Dict[str, Any] = {}
    preferred_name: Optional[str] = None

    if conn_ok and conn_path is not None:
        connection_names, toml_data, preferred_name, load_error = load_available_connections(conn_path)
        if load_error:
            st.error(load_error)

    python_ok, python_detail = check_python()
    cortex_ok, cortex_detail = check_cortex_details()
    skill_ok, skill_detail = check_skill_file(skill_name)
    interop_ok, interop_detail = check_wsl_interop()

    checks = [
        ("Python available", python_ok, python_detail),
        ("Cortex CLI available", cortex_ok, cortex_detail),
        ("connections.toml exists", conn_ok, conn_detail),
        (f"{skill_name} skill exists", skill_ok, skill_detail),
        ("WSL interoperability check", interop_ok, interop_detail),
    ]

    return conn_path, connection_names, toml_data, preferred_name, checks


def render_preflight_panel(
    skill_name: str,
    connection_names: List[str],
    preferred_name: Optional[str],
    checks: List[Tuple[str, bool, str]],
) -> None:
    with st.expander("Preflight Diagnostics", expanded=False):
        for label, ok, detail in checks:
            if ok:
                st.success(f"{label}: {detail}")
            else:
                st.error(f"{label}: {detail}")

        if connection_names:
            st.caption(f"Available connection profiles: {', '.join(connection_names)}")
            if preferred_name:
                st.caption(f"Preferred profile: {preferred_name}")

        st.info(
            "Open Streamlit manually in your Windows browser at http://localhost:8501. "
            "If Snowflake login does not open automatically, copy the login URL from the Ubuntu terminal "
            "into your Windows browser."
        )


def render_test_connection_expander(toml_data: Dict[str, Any], selected_connection_name: str) -> None:
    with st.expander("Test Connection", expanded=False):
        st.caption("Authenticate with Snowflake and validate the selected connection profile.")

        if st.button("Run Test Connection", use_container_width=True):
            if not selected_connection_name:
                st.warning("Select a connection profile in the sidebar first.")
                return

            status = st.status("Starting connection test...", expanded=True)
            conn = None
            try:
                status.write(f"Selected connection profile: {selected_connection_name}")
                conn_params = get_connection_params(toml_data, selected_connection_name)
                status.write(
                    "If authentication uses externalbrowser and no browser opens automatically, "
                    "check the Ubuntu terminal for a login URL and paste it into your Windows browser."
                )
                conn = get_python_connection(conn_params)
                with conn.cursor() as cur:
                    cur.execute("SELECT CURRENT_ACCOUNT(), CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE()")
                    row = cur.fetchone()

                status.update(label="Connection test succeeded", state="complete")
                st.success("Connection succeeded.")
                st.json(
                    {
                        "current_account": row[0] if row else None,
                        "current_user": row[1] if row else None,
                        "current_role": row[2] if row else None,
                        "current_warehouse": row[3] if row else None,
                    }
                )
            except Exception as exc:
                status.update(label="Connection test failed", state="error")
                st.error(f"Connection failed: {exc}")
            finally:
                if conn is not None:
                    try:
                        conn.close()
                    except Exception:
                        pass


def render_instructions_expander() -> None:
    with st.expander("Instructions", expanded=False):
        st.markdown(
            """
            **Typical workflow**
            1. Select a connection profile in the sidebar.
            2. Refresh the procedure catalog.
            3. Search or filter to the stored procedure you need.
            4. Select it from the table.
            5. Review existing documentation on the right.
            6. Generate, replace, delete, or export documentation as needed.

            **Notes**
            - The app shows **current procedure versions** from `BACKUP.DDL_HISTORY_PROCEDURES`.
            - A procedure is considered documented when a row exists in `BACKUP.PROCEDURE_DOCUMENTATION`
              for the same current `CHANGE_HASH` and documentation contract version.
            - If a procedure changes and gets a new `CHANGE_HASH`, it becomes eligible for documentation again.
            """
        )


# ----------------------------
# Sidebar
# ----------------------------

def render_sidebar(
    connection_names: List[str],
    preferred_connection: Optional[str],
) -> Dict[str, Any]:
    with st.sidebar:
        st.header("Configuration")

        default_index = 0
        if preferred_connection and preferred_connection in connection_names:
            default_index = connection_names.index(preferred_connection)

        if connection_names:
            selected_connection = st.selectbox(
                "Connection Profile",
                options=connection_names,
                index=default_index,
            )
        else:
            selected_connection = ""
            st.warning("No connection profiles available.")

        workdir = st.text_input("Workdir", value=str(Path.cwd()))
        skill_name = st.text_input("Skill Name", value=DEFAULT_SKILL_NAME)
        model_name = st.text_input("Model Name", value=DEFAULT_MODEL_NAME)
        documentation_model_label = st.text_input(
            "Documentation Model Label",
            value=DEFAULT_DOC_MODEL_LABEL,
        )
        log_level = st.selectbox("Log Level", options=["DEBUG", "INFO", "WARNING", "ERROR"], index=1)
        dry_run = st.selectbox("Dry Run", options=[False, True], index=0)

    return {
        "selected_connection": selected_connection,
        "workdir": workdir,
        "skill_name": skill_name,
        "model_name": model_name,
        "documentation_model_label": documentation_model_label,
        "log_level": log_level,
        "dry_run": dry_run,
    }


# ----------------------------
# Catalog
# ----------------------------

def load_catalog(
    toml_data: Dict[str, Any],
    selected_connection_name: str,
) -> None:
    if not selected_connection_name:
        st.warning("Select a connection profile first.")
        return

    status = st.status("Loading procedure catalog...", expanded=True)
    conn = None
    try:
        status.write(f"Selected connection profile: {selected_connection_name}")
        conn_params = get_connection_params(toml_data, selected_connection_name)
        status.write(
            "If authentication uses externalbrowser and no browser opens automatically, "
            "check the Ubuntu terminal for a login URL and paste it into your Windows browser."
        )
        conn = get_python_connection(conn_params)
        rows = list_procedure_catalog(conn)
        st.session_state["catalog_rows"] = rows
        status.update(label="Procedure catalog loaded", state="complete")
        st.success(f"Loaded {len(rows)} current procedure row(s).")
    except Exception as exc:
        status.update(label="Procedure catalog failed", state="error")
        st.error(f"Catalog load failed: {exc}")
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


def apply_catalog_filters(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    search_text = st.session_state.get("catalog_search_text", "").strip().lower()
    status_filter = st.session_state.get("catalog_status_filter", [])
    catalog_filter = st.session_state.get("catalog_catalog_filter", [])
    schema_filter = st.session_state.get("catalog_schema_filter", [])
    language_filter = st.session_state.get("catalog_language_filter", [])

    filtered = rows

    if search_text:
        filtered = [
            row for row in filtered
            if search_text in str(row.get("PROCEDURE_SIGNATURE", "")).lower()
            or search_text in str(row.get("PROCEDURE_NAME", "")).lower()
            or search_text in str(row.get("SCHEMA_NAME", "")).lower()
            or search_text in str(row.get("CATALOG_NAME", "")).lower()
        ]

    if status_filter:
        filtered = [
            row for row in filtered
            if ("Documented" if row.get("HAS_DOCUMENTATION") else "Not Documented") in status_filter
        ]

    if catalog_filter:
        filtered = [row for row in filtered if row.get("CATALOG_NAME") in catalog_filter]

    if schema_filter:
        filtered = [row for row in filtered if row.get("SCHEMA_NAME") in schema_filter]

    if language_filter:
        filtered = [row for row in filtered if (row.get("LANGUAGE") or "Unknown") in language_filter]

    return filtered


def render_catalog_panel(
    toml_data: Dict[str, Any],
    selected_connection_name: str,
) -> None:
    st.subheader("Procedure Catalog")

    top_left, top_mid, top_right = st.columns([1, 2, 1])

    with top_left:
        if st.button("Refresh Catalog", use_container_width=True):
            load_catalog(toml_data, selected_connection_name)

    with top_mid:
        st.text_input(
            "Select / Find Procedure",
            key="catalog_search_text",
            placeholder="Search by procedure name, signature, catalog, or schema",
            label_visibility="collapsed",
        )

    with top_right:
        st.empty()

    rows = st.session_state.get("catalog_rows", [])

    if not rows:
        st.info("Refresh the catalog to load current procedures.")
        return

    catalogs = sorted({row.get("CATALOG_NAME") for row in rows if row.get("CATALOG_NAME")})
    schemas = sorted({row.get("SCHEMA_NAME") for row in rows if row.get("SCHEMA_NAME")})
    languages = sorted({row.get("LANGUAGE") or "Unknown" for row in rows})

    f1, f2, f3, f4 = st.columns(4)
    with f1:
        st.multiselect(
            "Documentation Status",
            options=["Documented", "Not Documented"],
            key="catalog_status_filter",
        )
    with f2:
        st.multiselect(
            "Catalog",
            options=catalogs,
            key="catalog_catalog_filter",
        )
    with f3:
        st.multiselect(
            "Schema",
            options=schemas,
            key="catalog_schema_filter",
        )
    with f4:
        st.multiselect(
            "Language",
            options=languages,
            key="catalog_language_filter",
        )

    filtered_rows = apply_catalog_filters(rows)

    display_rows = []
    label_to_id: Dict[str, str] = {}

    for row in filtered_rows:
        label = row["PROCEDURE_SIGNATURE"]
        label_to_id[label] = row["PROCEDURE_ID"]
        display_rows.append(
            {
                "Procedure": label,
                "Documentation Status": row.get("DOCUMENTATION_STATUS") or ("Documented" if row.get("HAS_DOCUMENTATION") else "Not Documented"),
                "Catalog": row.get("CATALOG_NAME"),
                "Schema": row.get("SCHEMA_NAME"),
                "Language": row.get("LANGUAGE"),
                "Returns": row.get("RETURNS_TYPE"),
                "Idempotency": row.get("IDEMPOTENCY_CLASSIFICATION"),
                "Dynamic SQL": row.get("USES_DYNAMIC_SQL"),
                "Risk Count": row.get("RISK_COUNT"),
                "Documented At": row.get("DOCUMENTED_AT"),
            }
        )

    st.dataframe(display_rows, use_container_width=True, hide_index=True)

    if display_rows:
        current_selected_label = st.session_state.get("selected_procedure_label")
        options = [row["Procedure"] for row in display_rows]
        index = 0
        if current_selected_label in options:
            index = options.index(current_selected_label)

        selected_label = st.selectbox(
            "Selected Procedure",
            options=options,
            index=index,
            key="selected_procedure_label",
        )
        st.session_state["selected_procedure_id"] = label_to_id[selected_label]


# ----------------------------
# Viewer and actions
# ----------------------------

def render_selected_procedure_panel() -> None:
    st.subheader("Documentation Viewer")

    selected_row = get_selected_catalog_row()

    if not selected_row:
        st.info("Select a procedure from the catalog.")
        return

    action_col1, action_col2, action_col3, action_col4 = st.columns(4)

    with action_col1:
        st.empty()
    with action_col2:
        st.empty()
    with action_col3:
        st.empty()
    with action_col4:
        markdown_doc = selected_row.get("MARKDOWN_DOC")
        if markdown_doc:
            try:
                pdf_bytes = build_pdf_bytes(selected_row["PROCEDURE_SIGNATURE"], markdown_doc)
                st.download_button(
                    "Export Results to PDF",
                    data=pdf_bytes,
                    file_name=f"{selected_row['PROCEDURE_NAME']}_documentation.pdf",
                    mime="application/pdf",
                    use_container_width=True,
                )
            except Exception as exc:
                st.error(f"Could not generate PDF: {exc}")

    st.markdown(f"### {selected_row['PROCEDURE_SIGNATURE']}")

    doc_available = bool(selected_row.get("HAS_DOCUMENTATION"))
    if doc_available:
        st.success("Documentation is available for the current procedure version.")
    else:
        st.warning("Documentation is not available for the current procedure version.")

    tabs = st.tabs(["Markdown", "JSON", "Metadata"])

    with tabs[0]:
        markdown_doc = selected_row.get("MARKDOWN_DOC")
        if markdown_doc:
            st.markdown(markdown_doc)
        else:
            st.info("No markdown documentation available for the current procedure version.")

    with tabs[1]:
        raw_json = normalize_json_value(selected_row.get("DOCUMENTATION_JSON"))
        if raw_json:
            st.json(raw_json)
        else:
            st.info("No JSON documentation available for the current procedure version.")

    with tabs[2]:
        st.json(
            {
                "PROCEDURE_SIGNATURE": selected_row.get("PROCEDURE_SIGNATURE"),
                "PROCEDURE_ID": selected_row.get("PROCEDURE_ID"),
                "CATALOG_NAME": selected_row.get("CATALOG_NAME"),
                "SCHEMA_NAME": selected_row.get("SCHEMA_NAME"),
                "PROCEDURE_NAME": selected_row.get("PROCEDURE_NAME"),
                "ARGUMENTS": selected_row.get("ARGUMENTS"),
                "OBJECT_TYPE": selected_row.get("OBJECT_TYPE"),
                "CHANGE_HASH": selected_row.get("CHANGE_HASH"),
                "VERSION_RANK": selected_row.get("VERSION_RANK"),
                "HAS_DOCUMENTATION": selected_row.get("HAS_DOCUMENTATION"),
                "DOCUMENTATION_STATUS": selected_row.get("DOCUMENTATION_STATUS"),
                "DOCUMENTED_AT": str(selected_row.get("DOCUMENTED_AT")),
                "LANGUAGE": selected_row.get("LANGUAGE"),
                "RETURNS_TYPE": selected_row.get("RETURNS_TYPE"),
                "IDEMPOTENCY_CLASSIFICATION": selected_row.get("IDEMPOTENCY_CLASSIFICATION"),
                "USES_DYNAMIC_SQL": selected_row.get("USES_DYNAMIC_SQL"),
                "RISK_COUNT": selected_row.get("RISK_COUNT"),
                "SUMMARY": selected_row.get("SUMMARY"),
            }
        )


def run_job_ui(
    toml_data: Dict[str, Any],
    config: Dict[str, Any],
) -> None:
    selected_row = get_selected_catalog_row()

    if not selected_row:
        st.info("Select a procedure before running documentation.")
        return

    if selected_row.get("HAS_DOCUMENTATION"):
        st.info("Documentation already exists for the selected procedure version. Use Replace or Delete below if needed.")
        return

    st.markdown("### Documentation Actions")

    if st.button("Generate Documentation", type="primary", use_container_width=True):
        selected_connection = config["selected_connection"]
        selected_procedure_id = selected_row["PROCEDURE_ID"]

        if not selected_connection:
            st.warning("Select a connection profile first.")
            return

        try:
            conn_params = get_connection_params(toml_data, selected_connection)
        except Exception as exc:
            st.error(f"Could not resolve connection parameters: {exc}")
            return

        progress_box = st.empty()
        logs: List[str] = []

        def progress_callback(message: str) -> None:
            logs.append(message)
            progress_box.code("\n".join(logs[-100:]))

        try:
            progress_callback("Starting documentation job...")
            progress_callback(f"Selected connection profile: {selected_connection}")
            progress_callback(f"Selected procedure id: {selected_procedure_id}")
            progress_callback(
                "If authentication uses externalbrowser and no browser opens automatically, "
                "check the Ubuntu terminal for a login URL and paste it into your Windows browser."
            )

            result = run_documentation_job(
                conn_params=conn_params,
                selected_connection_name=selected_connection,
                workdir=Path(config["workdir"]).expanduser().resolve(),
                model_name=config["model_name"],
                skill_name=config["skill_name"],
                documentation_model_label=config["documentation_model_label"],
                selected_procedure_id=selected_procedure_id,
                dry_run=config["dry_run"],
                progress_callback=progress_callback,
            )

            st.success("Documentation job completed.")
            st.json(
                {
                    "total_selected": result.total_selected,
                    "documented": result.documented,
                    "failed": result.failed,
                }
            )

            if result.failures:
                st.warning("Some procedures failed.")
                st.dataframe(result.failures, use_container_width=True, hide_index=True)

            refresh_catalog_and_selected_row(
                toml_data=toml_data,
                selected_connection_name=selected_connection,
                selected_procedure_id=selected_procedure_id,
            )
            st.rerun()

        except Exception as exc:
            st.error(f"Run failed: {exc}")


def replace_delete_ui(
    toml_data: Dict[str, Any],
    config: Dict[str, Any],
) -> None:
    selected_row = get_selected_catalog_row()

    if not selected_row:
        st.info("Select a procedure before replace/delete actions.")
        return

    st.markdown("### Replace / Delete Current Documentation")

    doc_available = bool(selected_row.get("HAS_DOCUMENTATION"))
    if not doc_available:
        st.info("No current documentation exists for this procedure version.")
        return

    confirm_replace = st.checkbox(
        "I understand that Replace will delete current documentation for this procedure version and regenerate it.",
        key="confirm_replace_checkbox",
    )
    confirm_delete = st.checkbox(
        "I understand that Delete will remove current documentation for this procedure version.",
        key="confirm_delete_checkbox",
    )

    r1, r2 = st.columns(2)

    with r1:
        if st.button("Replace Current Documentation", use_container_width=True):
            if not confirm_replace:
                st.warning("Please confirm the replace action first.")
                return

            selected_connection = config["selected_connection"]
            if not selected_connection:
                st.warning("Select a connection profile first.")
                return

            try:
                conn_params = get_connection_params(toml_data, selected_connection)
                conn = get_python_connection(conn_params)
                try:
                    deleted = delete_documentation_for_current_version(conn, selected_row)
                finally:
                    conn.close()

                st.info(f"Deleted {deleted} current documentation row(s). Starting regeneration...")

                progress_box = st.empty()
                logs: List[str] = []

                def progress_callback(message: str) -> None:
                    logs.append(message)
                    progress_box.code("\n".join(logs[-100:]))

                result = run_documentation_job(
                    conn_params=conn_params,
                    selected_connection_name=selected_connection,
                    workdir=Path(config["workdir"]).expanduser().resolve(),
                    model_name=config["model_name"],
                    skill_name=config["skill_name"],
                    documentation_model_label=config["documentation_model_label"],
                    selected_procedure_id=selected_row["PROCEDURE_ID"],
                    dry_run=config["dry_run"],
                    progress_callback=progress_callback,
                )

                st.success("Replace operation completed.")
                st.json(
                    {
                        "total_selected": result.total_selected,
                        "documented": result.documented,
                        "failed": result.failed,
                    }
                )

                refresh_catalog_and_selected_row(
                    toml_data=toml_data,
                    selected_connection_name=selected_connection,
                    selected_procedure_id=selected_row["PROCEDURE_ID"],
                )
                st.rerun()

            except Exception as exc:
                st.error(f"Replace failed: {exc}")

    with r2:
        if st.button("Delete Current Documentation", use_container_width=True):
            if not confirm_delete:
                st.warning("Please confirm the delete action first.")
                return

            selected_connection = config["selected_connection"]
            if not selected_connection:
                st.warning("Select a connection profile first.")
                return

            try:
                conn_params = get_connection_params(toml_data, selected_connection)
                conn = get_python_connection(conn_params)
                try:
                    deleted = delete_documentation_for_current_version(conn, selected_row)
                finally:
                    conn.close()

                st.success(f"Deleted {deleted} documentation row(s).")

                refresh_catalog_and_selected_row(
                    toml_data=toml_data,
                    selected_connection_name=selected_connection,
                    selected_procedure_id=selected_row["PROCEDURE_ID"],
                )
                st.rerun()

            except Exception as exc:
                st.error(f"Delete failed: {exc}")


# ----------------------------
# Main
# ----------------------------

def main() -> None:
    st.set_page_config(page_title=APP_TITLE, layout="wide")
    st.title(APP_TITLE)
    st.caption("Procedure browser and documentation generator for current Snowflake stored procedure versions.")

    st.session_state.setdefault("catalog_rows", [])
    st.session_state.setdefault("selected_procedure_id", None)

    skill_for_preflight = st.session_state.get("skill_name_override", DEFAULT_SKILL_NAME)
    conn_path, connection_names, toml_data, preferred_connection, preflight_checks = get_preflight_data(skill_for_preflight)

    if conn_path is None or not conn_path.exists():
        st.stop()

    config = render_sidebar(connection_names, preferred_connection)
    st.session_state["skill_name_override"] = config["skill_name"]

    preflight_col, test_col, instructions_col = st.columns(3)

    with preflight_col:
        render_preflight_panel(
            skill_name=skill_for_preflight,
            connection_names=connection_names,
            preferred_name=preferred_connection,
            checks=preflight_checks,
        )

    with test_col:
        render_test_connection_expander(
            toml_data=toml_data,
            selected_connection_name=config["selected_connection"],
        )

    with instructions_col:
        render_instructions_expander()

    st.divider()

    left, right = st.columns([1.15, 1.0], gap="large")

    with left:
        render_catalog_panel(
            toml_data=toml_data,
            selected_connection_name=config["selected_connection"],
        )

    with right:
        render_selected_procedure_panel()
        st.divider()
        run_job_ui(toml_data=toml_data, config=config)
        st.divider()
        replace_delete_ui(toml_data=toml_data, config=config)


if __name__ == "__main__":
    main()