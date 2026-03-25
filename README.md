# Stored Procedure Documentation Browser

A Streamlit-based browser and runner for documenting Snowflake stored procedures using Snowflake Cortex Code CLI.

This project is designed for a workflow where a developer or analyst:

1. opens a browser-style catalog of current procedures,
2. selects one procedure,
3. checks whether documentation already exists for the current version,
4. generates documentation if needed,
5. optionally deletes current documentation and regenerates it.

## What this project uses

- Snowflake Python connector
- Snowflake Cortex Code CLI
- Streamlit
- A nightly backup/history table of procedure DDL:
  - `PROD_TELEMETRY_DB.BACKUP.DDL_HISTORY_PROCEDURES`
- A documentation output table:
  - `PROD_TELEMETRY_DB.BACKUP.PROCEDURE_DOCUMENTATION`

## Supported environments

### Mac
Run from a normal terminal.

### Windows
Run from **WSL Ubuntu**.

This project should be treated as a Linux-style tool:
- Mac runs it natively
- Windows runs it inside WSL

## Repository layout

```text
sproc-docs/
  app/
    streamlit_app.py
  runner/
    connections.py
    cortex_client.py
    job.py
    parser.py
  skills/
    sproc-documenter.md
  scripts/
    bootstrap_and_launch.sh
    launch_streamlit.sh
  sql/
    create_procedure_documentation_table.sql
  requirements.txt
  README.md

Prerequisites

You need:

Python 3
Snowflake Cortex Code CLI installed in WSL or Mac
a valid ~/.snowflake/connections.toml
the sproc-documenter skill file copied into:
~/.snowflake/cortex/skills/
Example connections.toml

Expected location:

~/.snowflake/connections.toml

Example:

default_connection_name = "CORTEX_CODE_READ"

[CORTEX_CODE]
account = "VOLTAGRID-PLATFORM"
user = "MICHAEL.TALBERT@VOLTAGRID.com"
authenticator = "EXTERNALBROWSER"
warehouse = "COMPUTE_WH"
role = "CORTEX_CODE"

[CORTEX_CODE_READ]
account = "VOLTAGRID-PLATFORM"
user = "MICHAEL.TALBERT@VOLTAGRID.com"
authenticator = "EXTERNALBROWSER"
warehouse = "COMPUTE_WH"
role = "CORTEX_CODE_READ"

[CORTEX_CODE_WRITE]
account = "VOLTAGRID-PLATFORM"
user = "MICHAEL.TALBERT@VOLTAGRID.com"
authenticator = "EXTERNALBROWSER"
warehouse = "COMPUTE_WH"
role = "CORTEX_CODE_WRITE"
First-time setup from a blank WSL terminal

Open Ubuntu / WSL, then run:

cd "/mnt/c/Users/<your-user>/path/to/sproc-docs"
chmod +x scripts/bootstrap_and_launch.sh
chmod +x scripts/launch_streamlit.sh
./scripts/bootstrap_and_launch.sh

This should:

create .venv
install Python dependencies
copy the skill file
check Snowflake config
launch Streamlit

Then open this manually in your Windows browser:

http://localhost:8501
Normal startup after setup

From a blank WSL terminal:

cd "/mnt/c/Users/<your-user>/path/to/sproc-docs"
./scripts/launch_streamlit.sh

Then open:

http://localhost:8501
Install dependencies manually if needed

If you want to install manually:

sudo apt update
sudo apt install python3-venv python3-pip -y

cd "/mnt/c/Users/<your-user>/path/to/sproc-docs"

python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

mkdir -p ~/.snowflake/cortex/skills
cp skills/sproc-documenter.md ~/.snowflake/cortex/skills/
chmod 600 ~/.snowflake/connections.toml
How the app works
Procedure catalog

The app shows all current procedures from:

PROD_TELEMETRY_DB.BACKUP.DDL_HISTORY_PROCEDURES

The catalog is scoped to:

current versions only
one row per current procedure identity/version
Documentation status

The app checks whether documentation exists for the current procedure version by matching:

procedure identity
CHANGE_HASH
DOCUMENTATION_VERSION

against:

PROD_TELEMETRY_DB.BACKUP.PROCEDURE_DOCUMENTATION
What counts as “documented”

A current procedure version is considered documented when a row exists in:

PROD_TELEMETRY_DB.BACKUP.PROCEDURE_DOCUMENTATION

for the same:

CATALOG_NAME
SCHEMA_NAME
PROCEDURE_NAME
ARGUMENTS
OBJECT_TYPE
CHANGE_HASH
DOCUMENTATION_VERSION
If a procedure changes, will it need documentation again?

Yes.

If the DDL changes, the nightly backup process produces a new CHANGE_HASH in:

PROD_TELEMETRY_DB.BACKUP.DDL_HISTORY_PROCEDURES

That new current version will appear as not documented until a new documentation row is created.

If PROCEDURE_DOCUMENTATION is truncated, what happens?

If the table is truncated, then current procedures will appear as undocumented again, because the matching documentation rows are gone.

Streamlit workflow
Select a connection profile
Test the Snowflake connection
Refresh the procedure catalog
Select a procedure
Review current documentation if available
Run documentation if needed
Optionally delete current documentation and regenerate
Dry Run

If Dry Run is enabled:

the app selects the procedure
validates that the documentation flow can run
does not insert a row into PROCEDURE_DOCUMENTATION
Delete Current Documentation

The app can delete the current documentation row(s) for the selected procedure version.

The delete is scoped to:

procedure identity
current CHANGE_HASH
DOCUMENTATION_VERSION

This is intended as a convenience for reruns if something goes wrong.

Log levels
DEBUG = most detailed troubleshooting output
INFO = normal runtime information
WARNING = only warnings and above
ERROR = only failures

Recommended default:

INFO
Authentication behavior

This project uses:

Python Snowflake connector with externalbrowser
Cortex Code CLI with your configured Snowflake connection

You may be asked to authenticate more than once, especially because:

the Python connector and Cortex CLI are separate clients
WSL browser auto-open may not work cleanly in all environments

If browser login does not open automatically:

check the Ubuntu terminal
copy the login URL
paste it into your Windows browser
Troubleshooting
Streamlit starts but browser does not open

Open manually:

http://localhost:8501
WSL interoperability warning

If preflight shows a WSL interoperability message, that is informational unless core functionality is broken. This project does not depend on auto-opening Windows executables from WSL.

Snowflake login hangs

If a button appears to hang:

look at the Ubuntu terminal
find the Microsoft/Snowflake login URL
paste it into your Windows browser
Permission denied on .sh scripts

Run:

chmod +x scripts/*.sh
Connections file permissions warning

Run:

chmod 600 ~/.snowflake/connections.toml
Notes for maintainers
Current documentation contract version

The project currently uses:

v1.0

If the JSON/markdown contract changes materially, bump the documentation version.

Current Cortex invocation mode

This project currently uses:

cortex -p

instead of -f, because the installed CLI may not support -f even if some docs mention it.

Nightly backup procedure

This app does not run the nightly backup loader manually. The expectation is that the backup/history process already runs on schedule.

Recommended team usage

This app is designed for ticket-driven work:

analyst/developer gets assigned a stored procedure update
they open the browser
find the procedure
review current docs if available
generate docs if missing or regenerate after deleting current docs

---

## What to do next

From WSL:

```bash
cd "/mnt/c/Users/MichaelTalbert/OneDrive - VoltaGrid/Desktop/sproc-docs"
source .venv/bin/activate
./scripts/launch_streamlit.sh

Then in the app:

Test Connection
Refresh Procedure Catalog
Select a procedure
View existing documentation if present
Run Documentation Job or Delete Current Documentation

One small note on the delete button: it deletes rows for the selected current procedure version and the current documentation contract version. That matches your intended convenience use case.