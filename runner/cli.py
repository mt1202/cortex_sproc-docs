from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from .config import DEFAULT_CONNECTIONS_FILE, DEFAULT_SKILL_NAME, DEFAULT_BACKUP_CALL, RunnerConfig
from .connections import get_connection_params, load_connections_toml, pick_connection_name
from .job import process_rows
from .snowflake_client import get_python_connection, maybe_call_backup_proc, select_source_rows


def setup_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )


def parse_args(argv=None) -> RunnerConfig:
    parser = argparse.ArgumentParser(description="Generate procedure documentation using Cortex Code CLI and write results to Snowflake.")
    parser.add_argument("--connection-name", default=None)
    parser.add_argument("--connections-file", default=str(DEFAULT_CONNECTIONS_FILE))
    parser.add_argument("--workdir", required=True)
    parser.add_argument("--model-name", default="auto")
    parser.add_argument("--documentation-model-label", default="cortex-cli:auto")
    parser.add_argument("--skill-name", default=DEFAULT_SKILL_NAME)
    parser.add_argument("--max-rows", type=int, default=None)
    parser.add_argument("--call-backup-proc", action="store_true")
    parser.add_argument("--backup-call-sql", default=DEFAULT_BACKUP_CALL)
    parser.add_argument("--no-bypass", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--keep-prompt-files", action="store_true")
    parser.add_argument("--log-level", default="INFO")
    parser.add_argument("--cortex-timeout-seconds", type=int, default=600)
    args = parser.parse_args(argv)
    return RunnerConfig(
        connection_name=args.connection_name,
        connections_file=Path(args.connections_file).expanduser().resolve(),
        workdir=Path(args.workdir).expanduser().resolve(),
        model_name=args.model_name,
        documentation_model_label=args.documentation_model_label,
        skill_name=args.skill_name,
        max_rows=args.max_rows,
        call_backup_proc=args.call_backup_proc,
        backup_call_sql=args.backup_call_sql,
        cortex_bypass=not args.no_bypass,
        dry_run=args.dry_run,
        keep_prompt_files=args.keep_prompt_files,
        log_level=args.log_level,
        cortex_timeout_seconds=args.cortex_timeout_seconds,
    )


def main(argv=None) -> int:
    config = parse_args(argv)
    setup_logging(config.log_level)
    logger = logging.getLogger(__name__)

    if not config.workdir.exists():
        logger.error("Workdir does not exist: %s", config.workdir)
        return 2

    try:
        toml_data = load_connections_toml(config.connections_file)
        selected_connection_name = pick_connection_name(config.connection_name, toml_data)
        conn_params = get_connection_params(toml_data, selected_connection_name)
    except Exception:
        logger.exception("Could not resolve Snowflake connection settings")
        return 3

    logger.info("Using Cortex/Snowflake connection: %s", selected_connection_name)
    logger.info("Using connections.toml: %s", config.connections_file)

    try:
        conn = get_python_connection(conn_params)
    except Exception:
        logger.exception("Could not connect to Snowflake")
        return 4

    try:
        if config.call_backup_proc:
            maybe_call_backup_proc(conn, config.backup_call_sql)
            conn.commit()
        rows = select_source_rows(conn, config.max_rows)
        if not rows:
            logger.info("No undocumented procedures found. Nothing to do.")
            return 0
        result = process_rows(conn, rows, config, selected_connection_name)
        logger.info("Selected=%s documented=%s failed=%s skipped=%s", result.selected, result.documented, result.failed, result.skipped)
        return 0 if result.failed == 0 else 5
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
