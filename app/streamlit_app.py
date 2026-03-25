from __future__ import annotations

import json
import shutil
import subprocess
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


def load_available_connections(connections_file: Path) -> Tuple[List[str], Dict[str, Any], Optional[str], Optional[str]]:
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


def render_preflight(skill_name: str) -> Tuple[Optional[Path], List[str], Dict[str, Any], Optional[str]]:
    with st.expander("Preflight Diagnostics", expanded=False):
        python_ok, python_detail = check_python()
        cortex_ok, cortex_detail = check_cortex_details()
        conn_ok, conn_detail, conn_path = check_connections_file()
        skill_ok, skill_detail = check_skill_file(skill_name)
        interop_ok, interop_detail = check_wsl_interop()

        checks = [
            ("Python available", python_ok, python_detail),
            ("Cortex CLI available", cortex_ok, cortex_detail),
            ("connections.toml exists", conn_ok, conn_detail),
            (f"{skill_name} skill exists", skill_ok, skill_detail),
            ("WSL interoperability check", interop_ok, interop_detail),
        ]

        for label, ok, detail in checks:
            if ok:
                st.success(f"{label}: {detail}")
            else:
                st.error(f"{label}: {detail}")

        st.info(
            "Open Streamlit manually in your Windows browser at http://localhost:8501. "
            "If Snowflake login does not open automatically, copy the login URL from the Ubuntu terminal "
            "into your Windows browser."
        )

        connection_names: List[str] = []
        toml_data: Dict[str, Any] = {}
        preferred_name: Optional[str] = None

        if conn_ok and conn_path is not None:
            connection_names, toml_data, preferred_name, load_error = load_available_connections(conn_path)
            if load_error:
                st.error(load_error)
            elif connection_names:
                st.caption(f"Available connection profiles: {', '.join(connection_names)}")
                if preferred_name:
                    st.caption(f"Preferred profile: {preferred_name}")
        else:
            connection_names = []
            toml_data = {}
            preferred_name = None

    if conn_ok and conn_path is not None:
        connection_names, toml_data, preferred_name, load_error = load_available_connections(conn_path)
        if load_error:
            st.error(load_error)

    return conn_path, connection_names, toml_data, preferred_name


def render_configuration_form(
    connection_names: List[str],
    preferred_connection: Optional[str],
) -> Dict[str, Any]:
    st.subheader("Configuration")

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

    col1, col2 = st.columns(2)
    with col1:
        workdir = st.text_input("Workdir", value=str(Path.cwd()))
        model_name = st.text_input("Model Name", value=DEFAULT_MODEL_NAME)
    with col2:
        skill_name = st.text_input("Skill Name", value=DEFAULT_SKILL_NAME)
        documentation_model_label = st.text_input(
            "Documentation Model Label",
            value=DEFAULT_DOC_MODEL_LABEL,
        )

    col3, col4 = st.columns(2)
    with col3:
        dry_run = st.checkbox("Dry Run", value=True)
    with col4:
        log_level = st.selectbox("Log Level", options=["DEBUG", "INFO", "WARNING", "ERROR"], index=1)

    return {
        "selected_connection": selected_connection,
        "workdir": workdir,
        "model_name": model_name,
        "skill_name": skill_name,
        "documentation_model_label": documentation_model_label,
        "dry_run": dry_run,
        "log_level": log_level,
    }


def test_connection_ui(toml_data: Dict[str, Any], selected_connection_name: str) -> None:
    st.subheader("Test Connection")

    if st.button("Test Connection", use_container_width=True):
        if not selected_connection_name:
            st.warning("Select a connection profile first.")
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


def render_catalog_ui(
    toml_data: Dict[str, Any],
    selected_connection_name: str,
) -> None:
    st.subheader("Procedure Catalog")

    if st.button("Refresh Procedure Catalog", use_container_width=True):
        load_catalog(toml_data, selected_connection_name)

    rows = st.session_state.get("catalog_rows", [])

    if rows:
        label_to_id = {}
        for row in rows:
            doc_flag = "Documented" if row.get("HAS_DOCUMENTATION") else "Not Documented"
            label = f"{row['PROCEDURE_SIGNATURE']}  |  {doc_flag}"
            label_to_id[label] = row["PROCEDURE_ID"]

        selected_label = st.selectbox(
            "Select Procedure",
            options=list(label_to_id.keys()),
            key="selected_procedure_label",
        )

        selected_id = label_to_id[selected_label]
        st.session_state["selected_procedure_id"] = selected_id

        selected_row = next(
            (row for row in rows if row["PROCEDURE_ID"] == selected_id),
            None,
        )

        if selected_row:
            st.dataframe(
                [
                    {
                        "Procedure": selected_row.get("PROCEDURE_SIGNATURE"),
                        "Database": selected_row.get("CATALOG_NAME"),
                        "Schema": selected_row.get("SCHEMA_NAME"),
                        "Object Type": selected_row.get("OBJECT_TYPE"),
                        "Documented": selected_row.get("HAS_DOCUMENTATION"),
                        "Documentation Status": selected_row.get("DOCUMENTATION_STATUS"),
                        "Documented At": selected_row.get("DOCUMENTED_AT"),
                        "Language": selected_row.get("LANGUAGE"),
                        "Returns": selected_row.get("RETURNS_TYPE"),
                        "Idempotency": selected_row.get("IDEMPOTENCY_CLASSIFICATION"),
                        "Uses Dynamic SQL": selected_row.get("USES_DYNAMIC_SQL"),
                        "Risk Count": selected_row.get("RISK_COUNT"),
                    }
                ],
                use_container_width=True,
                hide_index=True,
            )


def render_selected_procedure_ui() -> None:
    st.subheader("Selected Procedure")

    rows = st.session_state.get("catalog_rows", [])
    selected_id = st.session_state.get("selected_procedure_id")

    if not rows or not selected_id:
        st.info("Refresh the procedure catalog and select a procedure.")
        return

    selected_row = next(
        (row for row in rows if row["PROCEDURE_ID"] == selected_id),
        None,
    )

    if not selected_row:
        st.info("Selected procedure is no longer available in the current catalog.")
        return

    with st.expander("Selected Procedure Metadata", expanded=False):
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
            }
        )

    if selected_row.get("HAS_DOCUMENTATION"):
        st.success("Documentation is available for the current procedure version.")
        tabs = st.tabs(["Markdown", "JSON"])
        with tabs[0]:
            markdown_doc = selected_row.get("MARKDOWN_DOC") or "No markdown available."
            st.markdown(markdown_doc)
        with tabs[1]:
            raw_json = selected_row.get("DOCUMENTATION_JSON")
            if isinstance(raw_json, str):
                try:
                    parsed = json.loads(raw_json)
                except Exception:
                    parsed = raw_json
                st.json(parsed)
            elif raw_json is not None:
                st.json(raw_json)
            else:
                st.info("No JSON documentation available.")
    else:
        st.warning("Documentation is not available for the current procedure version.")


def run_job_ui(toml_data: Dict[str, Any], config: Dict[str, Any]) -> None:
    st.subheader("Generate Documentation")

    selected_connection = config["selected_connection"]
    selected_procedure_id = st.session_state.get("selected_procedure_id")
    rows = st.session_state.get("catalog_rows", [])
    selected_row = next((row for row in rows if row.get("PROCEDURE_ID") == selected_procedure_id), None)

    if selected_row and selected_row.get("HAS_DOCUMENTATION"):
        st.info("Current procedure version is already documented. You can delete it below and regenerate if needed.")

    if st.button("Run Documentation Job", type="primary", use_container_width=True):
        if not selected_connection:
            st.warning("Select a connection profile first.")
            return

        if not selected_procedure_id:
            st.warning("Refresh the procedure catalog and select a procedure first.")
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

        except Exception as exc:
            st.error(f"Run failed: {exc}")


def delete_doc_ui(toml_data: Dict[str, Any], config: Dict[str, Any]) -> None:
    st.subheader("Delete Current Documentation")

    selected_connection = config["selected_connection"]
    selected_procedure_id = st.session_state.get("selected_procedure_id")
    rows = st.session_state.get("catalog_rows", [])
    selected_row = next((row for row in rows if row.get("PROCEDURE_ID") == selected_procedure_id), None)

    if not selected_row:
        st.info("Select a procedure first.")
        return

    if not selected_row.get("HAS_DOCUMENTATION"):
        st.info("No current documentation exists for the selected procedure version.")
        return

    st.warning(
        "This deletes documentation only for the selected procedure's current CHANGE_HASH "
        f"and documentation version {DEFAULT_DOC_MODEL_LABEL!r} is not used in the delete filter. "
        "The delete is scoped by procedure identity, current change hash, and documentation contract version."
    )

    if st.button("Delete Current Documentation", use_container_width=True):
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

            st.success(f"Deleted {deleted} documentation row(s). Refresh the procedure catalog to see updated status.")
        except Exception as exc:
            st.error(f"Delete failed: {exc}")


def main() -> None:
    st.set_page_config(page_title=APP_TITLE, layout="wide")
    st.title(APP_TITLE)
    st.caption("Procedure browser and documentation generator for current Snowflake stored procedure versions.")

    skill_for_preflight = st.session_state.get("skill_name_override", DEFAULT_SKILL_NAME)
    conn_path, connection_names, toml_data, preferred_connection = render_preflight(skill_for_preflight)

    if conn_path is None or not conn_path.exists():
        st.stop()

    config = render_configuration_form(connection_names, preferred_connection)
    st.session_state["skill_name_override"] = config["skill_name"]

    st.divider()

    left, right = st.columns(2)
    with left:
        test_connection_ui(toml_data=toml_data, selected_connection_name=config["selected_connection"])
    with right:
        render_catalog_ui(
            toml_data=toml_data,
            selected_connection_name=config["selected_connection"],
        )

    st.divider()
    render_selected_procedure_ui()

    st.divider()
    left2, right2 = st.columns(2)
    with left2:
        run_job_ui(toml_data=toml_data, config=config)
    with right2:
        delete_doc_ui(toml_data=toml_data, config=config)


if __name__ == "__main__":
    main()