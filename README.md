# sproc-docs

A cross-platform toolkit for documenting Snowflake stored procedures with Cortex Code CLI.

## What is included

- A reusable Python runner backend
- A Streamlit UI for configuring and launching the runner
- A Cortex Code skill scaffold (`sproc-documenter`)
- SQL to create the target documentation table
- Shell launch scripts for Mac and WSL
- A Windows `.bat` convenience launcher that calls WSL

## Recommended team standard

- Mac users: run locally from Terminal
- Windows users: run from WSL
- Keep `~/.snowflake/connections.toml` in the local home directory on Mac or the WSL home directory on Windows

## Expected Snowflake connection behavior

The app and runner read `~/.snowflake/connections.toml` and prefer these connection names in order:

1. `CORTEX_CODE_READ`
2. `default_connection_name`
3. `CORTEX_CODE`
4. first available named connection

## Quick start

### 1. Create a virtual environment

Mac / WSL:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Put the skill in the global Cortex skills directory

```bash
mkdir -p ~/.snowflake/cortex/skills
cp skills/sproc-documenter.md ~/.snowflake/cortex/skills/
```

### 3. Create the documentation table

Run:

```sql
sql/create_procedure_documentation_table.sql
```

### 4. Start Streamlit

Mac / WSL:

```bash
bash scripts/launch_streamlit.sh
```

Windows convenience wrapper:

```bat
scripts\launch_streamlit.bat
```

## CLI usage

Example dry run:

```bash
python -m runner.cli --workdir . --max-rows 2 --dry-run --log-level DEBUG
```

Example live run:

```bash
python -m runner.cli --workdir . --max-rows 1 --log-level INFO
```

Example with backup procedure first:

```bash
python -m runner.cli --workdir . --call-backup-proc --max-rows 1 --log-level INFO
```

## Streamlit UI

The app provides:

- connection selection
- workdir selection
- model and skill settings
- dry-run and backup toggles
- candidate preview
- live run controls
- log output

## Project layout

```text
sproc-docs/
  README.md
  requirements.txt
  app/
    streamlit_app.py
  runner/
    __init__.py
    cli.py
    config.py
    connections.py
    cortex_client.py
    job.py
    parser.py
    snowflake_client.py
  skills/
    sproc-documenter.md
  sql/
    create_procedure_documentation_table.sql
  scripts/
    launch_streamlit.sh
    launch_streamlit.bat
    run_runner.sh
    run_runner.bat
```
