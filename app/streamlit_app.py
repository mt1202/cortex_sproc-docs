from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import streamlit as st

from runner.connections import (
    get_connection_params,
    load_connections_toml,
    pick_connection_name,
)
from runner.job import (
    get_python_connection,
    run_documentation_job,
    select_source_rows,
)

APP_TITLE = "Stored Procedure Documentation Runner"
DEFAULT_SKILL_NAME = "sproc-documenter"
DEFAULT_MODEL_NAME = "auto"
DEFAULT_DOC_MODEL_LABEL = "cortex-cli:auto"


def check_wsl_interop() -> Tuple[bool, str]:
    marker = Path("/proc/sys/fs/binfmt_misc/WSLInterop")
    if not marker.exists():
        return False, "WSLInterop marker file not found"
    try:
        value = marker.read_text(encoding="utf-8").strip()
        if value == "1":
            return True, "Enabled"
        return False, f"Found marker but value is '{value}'"
    except Exception as exc:
        return False, f"Could not read marker: {exc}"


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


def check_cortex_cli() -> Tuple[bool, str]:
    cortex_path = shutil.which("cortex")
    if cortex_path:
        return True, cortex_path
    return False, "cortex CLI not found on PATH"


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
    st.subheader("Preflight Diagnostics")

    python_ok, python_detail = check_python()
    cortex_ok, cortex_detail = check_cortex_cli()
    conn_ok, conn_detail, conn_path = check_connections_file()
    skill_ok, skill_detail = check_skill_file(skill_name)
    interop_ok, interop_detail = check_wsl_interop()

    checks = [
        ("Python available", python_ok, python_detail),
        ("Cortex CLI available", cortex_ok, cortex_detail),
        ("connections.toml exists", conn_ok, conn_detail),
        (f"{skill_name} skill exists", skill_ok, skill_detail),
        ("WSL interoperability enabled", interop_ok, interop_detail),
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

    return conn_path, connection_names, toml_data, preferred_name


def render_configuration_form(
    connection_names: List[str],
    preferred_connection: Optional[str],
) -> Dict[str, Any]:
    st.subheader("Run Configuration")

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
        skill_name = st.text_input("Skill Name", value=DEFAULT_SKILL_NAME)
    with col2:
        documentation_model_label = st.text_input(
            "Documentation Model Label",
            value=DEFAULT_DOC_MODEL_LABEL,
        )
        max_rows = st.number_input("Max Rows", min_value=1, max_value=5000, value=25, step=1)
        log_level = st.selectbox("Log Level", options=["DEBUG", "INFO", "WARNING", "ERROR"], index=1)

    col3, col4, col5 = st.columns(3)
    with col3:
        call_backup_proc = st.checkbox("Call Backup Procedure First", value=False)
    with col4:
        dry_run = st.checkbox("Dry Run", value=True)
    with col5:
        keep_prompt_files = st.checkbox("Keep Prompt Files", value=False)

    return {
        "selected_connection": selected_connection,
        "workdir": workdir,
        "model_name": model_name,
        "skill_name": skill_name,
        "documentation_model_label": documentation_model_label,
        "max_rows": int(max_rows),
        "log_level": log_level,
        "call_backup_proc": call_backup_proc,
        "dry_run": dry_run,
        "keep_prompt_files": keep_prompt_files,
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


def candidate_selector_ui(
    toml_data: Dict[str, Any],
    selected_connection_name: str,
    max_rows: int,
) -> None:
    st.subheader("Load Candidate Procedures")

    if st.button("Load Candidate Procedures", use_container_width=True):
        if not selected_connection_name:
            st.warning("Select a connection profile first.")
            return

        status = st.status("Loading undocumented procedures...", expanded=True)
        conn = None
        try:
            status.write(f"Selected connection profile: {selected_connection_name}")
            conn_params = get_connection_params(toml_data, selected_connection_name)
            status.write(
                "If authentication uses externalbrowser and no browser opens automatically, "
                "check the Ubuntu terminal for a login URL and paste it into your Windows browser."
            )

            conn = get_python_connection(conn_params)
            rows = select_source_rows(conn, max_rows=max_rows)

            st.session_state["candidate_rows"] = rows
            st.session_state["selected_procedure_id"] = None

            status.update(label="Candidate procedures loaded", state="complete")
            st.success(f"Loaded {len(rows)} undocumented procedure candidate(s).")
        except Exception as exc:
            status.update(label="Candidate load failed", state="error")
            st.error(f"Candidate load failed: {exc}")
        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass

    rows = st.session_state.get("candidate_rows", [])

    if rows:
        options = {row["PROCEDURE_SIGNATURE"]: row["PROCEDURE_ID"] for row in rows}
        labels = list(options.keys())

        selected_label = st.selectbox(
            "Select Procedure to Document",
            options=labels,
            key="selected_procedure_label",
        )

        selected_id = options[selected_label]
        st.session_state["selected_procedure_id"] = selected_id

        selected_row = next(
            (row for row in rows if row["PROCEDURE_ID"] == selected_id),
            None,
        )

        if selected_row:
            with st.expander("Selected Procedure Details", expanded=False):
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
                    }
                )


def run_job_ui(toml_data: Dict[str, Any], config: Dict[str, Any]) -> None:
    st.subheader("Run Documentation Job")

    if st.button("Run Documentation Job", type="primary", use_container_width=True):
        selected_connection = config["selected_connection"]
        if not selected_connection:
            st.warning("Select a connection profile first.")
            return

        selected_procedure_id = st.session_state.get("selected_procedure_id")
        if not selected_procedure_id:
            st.warning("Load candidate procedures and select one first.")
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
            progress_box.code("\n".join(logs[-50:]))

        try:
            progress_callback("Starting documentation job...")
            progress_callback(f"Selected connection profile: {selected_connection}")
            progress_callback(f"Selected procedure id: {selected_procedure_id}")
            progress_callback(
                "If authentication uses externalbrowser and no browser opens automatically, "
                "check the Ubuntu terminal for a login URL and paste it into your Windows browser."
            )

            if config["dry_run"]:
                conn = get_python_connection(conn_params)
                try:
                    rows = select_source_rows(
                        conn,
                        max_rows=config["max_rows"],
                        selected_procedure_id=selected_procedure_id,
                    )
                finally:
                    conn.close()

                progress_callback(f"Dry run only. Selected {len(rows)} candidate row(s).")
                st.success("Dry run completed.")
                if rows:
                    st.dataframe(
                        [
                            {
                                "PROCEDURE_SIGNATURE": row.get("PROCEDURE_SIGNATURE"),
                                "PROCEDURE_ID": row.get("PROCEDURE_ID"),
                                "CHANGE_HASH": row.get("CHANGE_HASH"),
                            }
                            for row in rows
                        ],
                        use_container_width=True,
                        hide_index=True,
                    )
                return

            result = run_documentation_job(
                conn_params=conn_params,
                selected_connection_name=selected_connection,
                workdir=Path(config["workdir"]).expanduser().resolve(),
                model_name=config["model_name"],
                skill_name=config["skill_name"],
                documentation_model_label=config["documentation_model_label"],
                max_rows=config["max_rows"],
                call_backup_proc=config["call_backup_proc"],
                keep_prompt_files=config["keep_prompt_files"],
                selected_procedure_id=selected_procedure_id,
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


def main() -> None:
    st.set_page_config(page_title=APP_TITLE, layout="wide")
    st.title(APP_TITLE)
    st.caption("WSL-friendly control panel for Snowflake stored procedure documentation runs.")

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
        candidate_selector_ui(
            toml_data=toml_data,
            selected_connection_name=config["selected_connection"],
            max_rows=config["max_rows"],
        )

    st.divider()
    run_job_ui(toml_data=toml_data, config=config)


if __name__ == "__main__":
    main()