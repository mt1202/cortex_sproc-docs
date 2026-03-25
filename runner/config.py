from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional

DOCUMENTATION_VERSION = "v1.0"
TARGET_TABLE = "PROD_TELEMETRY_DB.BACKUP.PROCEDURE_DOCUMENTATION"
SOURCE_TABLE = "PROD_TELEMETRY_DB.BACKUP.DDL_HISTORY_PROCEDURES"
DEFAULT_BACKUP_CALL = "CALL PROD_TELEMETRY_DB.BACKUP.LOAD_DDL_HISTORY_PROCEDURES()"
DEFAULT_SKILL_NAME = "sproc-documenter"
DEFAULT_CONNECTIONS_FILE = Path.home() / ".snowflake" / "connections.toml"

PREFERRED_CONNECTION_NAMES = [
    "CORTEX_CODE_READ",
    "CORTEX_CODE",
]


@dataclass
class RunnerConfig:
    connection_name: Optional[str]
    connections_file: Path
    workdir: Path
    model_name: str = "auto"
    documentation_model_label: str = "cortex-cli:auto"
    skill_name: str = DEFAULT_SKILL_NAME
    max_rows: Optional[int] = None
    call_backup_proc: bool = False
    backup_call_sql: str = DEFAULT_BACKUP_CALL
    cortex_bypass: bool = True
    dry_run: bool = False
    keep_prompt_files: bool = False
    log_level: str = "INFO"
    cortex_timeout_seconds: int = 600
