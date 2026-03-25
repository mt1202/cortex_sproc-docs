from __future__ import annotations

import json
from typing import Any, Dict, List, Tuple


def extract_marked_sections(cortex_output: str) -> Tuple[str, str]:
    json_start = "===BEGIN_JSON==="
    json_end = "===END_JSON==="
    md_start = "===BEGIN_MARKDOWN==="
    md_end = "===END_MARKDOWN==="

    try:
        json_block = cortex_output.split(json_start, 1)[1].split(json_end, 1)[0].strip()
        md_block = cortex_output.split(md_start, 1)[1].split(md_end, 1)[0].strip()
    except IndexError as exc:
        raise ValueError(
            "Could not parse Cortex output markers. Raw output begins:\n"
            + cortex_output[:4000]
        ) from exc

    return json_block, md_block


def parse_cortex_response(cortex_output: str) -> Tuple[Dict[str, Any], str]:
    json_text, markdown_text = extract_marked_sections(cortex_output)

    try:
        payload = json.loads(json_text)
    except json.JSONDecodeError as exc:
        raise ValueError(
            "Cortex JSON block is not valid JSON. JSON block begins:\n"
            + json_text[:4000]
        ) from exc

    return payload, markdown_text


def safe_list(value: Any) -> List[Any]:
    return value if isinstance(value, list) else []