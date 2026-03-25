from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional

try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib

from .config import PREFERRED_CONNECTION_NAMES


def load_connections_toml(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"connections.toml not found: {path}")
    with path.open("rb") as fh:
        return tomllib.load(fh)


def list_connection_names(toml_data: Dict[str, Any]) -> List[str]:
    return [key for key, value in toml_data.items() if isinstance(value, dict)]


def pick_connection_name(configured_name: Optional[str], toml_data: Dict[str, Any]) -> str:
    if configured_name:
        if configured_name not in toml_data:
            raise KeyError(f"Connection '{configured_name}' not found in connections.toml")
        return configured_name

    for preferred in PREFERRED_CONNECTION_NAMES:
        if preferred in toml_data:
            return preferred

    default_name = toml_data.get("default_connection_name")
    if isinstance(default_name, str) and default_name in toml_data:
        return default_name

    sections = list_connection_names(toml_data)
    if not sections:
        raise ValueError("No named connections found in connections.toml")
    return sections[0]


def get_connection_params(toml_data: Dict[str, Any], connection_name: str) -> Dict[str, Any]:
    section = toml_data.get(connection_name)
    if not isinstance(section, dict):
        raise KeyError(f"Connection section '{connection_name}' not found")

    required = ["account", "user", "authenticator", "warehouse"]
    missing = [k for k in required if not section.get(k)]
    if missing:
        raise ValueError(
            f"Connection '{connection_name}' is missing required keys: {', '.join(missing)}"
        )

    params = {
        "account": section["account"],
        "user": section["user"],
        "authenticator": section["authenticator"],
        "warehouse": section["warehouse"],
    }
    for key in ["password", "role", "database", "schema"]:
        if section.get(key) is not None:
            params[key] = section[key]
    return params
