#!/usr/bin/env python
"""Deploy the RideFlow marketplace medallion as a native Snowflake Project.

Wires the Git-connected Workspace end-to-end:
  1. SECRET (GitHub PAT) + API INTEGRATION + GIT REPOSITORY  -> repo browsable in Snowsight
  2. COMPUTE POOL                                            -> container runtime for Notebooks
  3. CREATE NOTEBOOK (from the repo) x5                      -> provision/landing/bronze/silver/gold
  4. Task DAG from tasks/tasks.sql (EXECUTE NOTEBOOK)        -> run history in Task Graph
  5. (--run) EXECUTE TASK                                    -> kick off one live run

Prereqs (deploy/.env, gitignored):
  SNOWFLAKE_* (account/user/password|key, role, warehouse, database)  — same as deploy.py
  GITHUB_REPO_URL = https://github.com/<owner>/<repo>
  GITHUB_USER     = <github username>
  GITHUB_PAT      = <fine-grained PAT with repo read>

Usage:  python deploy/deploy_project.py [--branch main] [--run]
"""
from __future__ import annotations

import argparse
import importlib.util
import os
import re
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

# reuse connect()/load_env() from the sibling deploy.py
_spec = importlib.util.spec_from_file_location("_deploy", str(REPO / "deploy" / "deploy.py"))
_d = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_d)

NOTEBOOKS = [
    ("NB_PIPELINE", "pipeline_driver.ipynb"),   # one parameterized driver for every layer
]
TASK_RESUME_ORDER = [   # children first, root last, so the graph is armed
    "T_04_GOLD", "T_03_SILVER", "T_02_BRONZE", "T_01_LANDING",
    "T_00_PROVISION", "T_RIDEFLOW_MARKETPLACE",
]


def run(cur, stmt):
    head = next((l for l in stmt.strip().splitlines() if l.strip()), "")
    print(f"  → {head[:90]}")
    cur.execute(stmt)


def sql_statements(text):
    """Yield executable statements from a .sql file (strip -- comments, full-line + inline)."""
    lines = []
    for l in text.splitlines():
        i = l.find("--")           # our SQL has no -- inside strings/identifiers
        lines.append(l[:i] if i != -1 else l)
    for stmt in "\n".join(lines).split(";"):
        if stmt.strip():
            yield stmt.strip()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--branch", default="main")
    ap.add_argument("--run", action="store_true", help="EXECUTE TASK after setup")
    args = ap.parse_args()
    _d.load_env()

    db = os.getenv("SNOWFLAKE_DATABASE", "RIDEFLOW_DEV_DEMO")
    wh = os.getenv("SNOWFLAKE_WAREHOUSE", "RIDEFLOW_WH")
    pool = os.getenv("SNOWFLAKE_COMPUTE_POOL", "RIDEFLOW_POOL")
    repo_url = os.getenv("GITHUB_REPO_URL", "").strip()
    gh_user = os.getenv("GITHUB_USER", "").strip()
    pat = os.getenv("GITHUB_PAT", "").strip()
    if not (repo_url and gh_user and pat):
        sys.exit("Set GITHUB_REPO_URL, GITHUB_USER, GITHUB_PAT in deploy/.env")
    m = re.match(r"https://github\.com/([^/]+)/", repo_url)
    if not m:
        sys.exit(f"GITHUB_REPO_URL must look like https://github.com/<owner>/<repo>: {repo_url}")
    owner = m.group(1)
    os.environ["SNOWFLAKE_DATABASE"] = db

    conn = _d.connect(db)
    cur = conn.cursor()
    print("── Git integration ──────────────────────────────────────────")
    run(cur, f"USE DATABASE {db}")
    run(cur, "CREATE SCHEMA IF NOT EXISTS PIPELINE")
    run(cur, "USE SCHEMA PIPELINE")
    run(cur, f"CREATE OR REPLACE SECRET GIT_PAT TYPE=PASSWORD USERNAME='{gh_user}' PASSWORD='{pat}'")
    run(cur, f"""CREATE OR REPLACE API INTEGRATION GIT_API_RIDEFLOW
        API_PROVIDER = git_https_api
        API_ALLOWED_PREFIXES = ('https://github.com/{owner}')
        ALLOWED_AUTHENTICATION_SECRETS = ({db}.PIPELINE.GIT_PAT)
        ENABLED = TRUE""")
    run(cur, f"""CREATE OR REPLACE GIT REPOSITORY RIDEFLOW_REPO
        API_INTEGRATION = GIT_API_RIDEFLOW
        GIT_CREDENTIALS = {db}.PIPELINE.GIT_PAT
        ORIGIN = '{repo_url}'""")
    run(cur, f"ALTER GIT REPOSITORY RIDEFLOW_REPO FETCH")

    print("── Compute pool (container runtime for Notebooks) ───────────")
    run(cur, f"""CREATE COMPUTE POOL IF NOT EXISTS {pool}
        MIN_NODES = 1 MAX_NODES = 2 INSTANCE_FAMILY = CPU_X64_XS
        AUTO_SUSPEND_SECS = 300""")

    print("── Stage the LakeLogic wheelhouse (headless-run install) ────")
    # Headless EXECUTE NOTEBOOK task runs don't have Snowflake's internal PyPI mirror, so
    # they install LakeLogic (official 1.41.0) + pure-python deps from wheels staged here
    # with pip --no-index. The container runtime ships pandas/pydantic/pyyaml/etc.
    run(cur, f"CREATE STAGE IF NOT EXISTS {db}.PIPELINE.WHEELS")
    for whl in sorted((REPO / "wheelhouse").glob("*.whl")):
        uri = "file://" + str(whl).replace("\\", "/")
        run(cur, f"PUT '{uri}' @{db}.PIPELINE.WHEELS AUTO_COMPRESS = FALSE OVERWRITE = TRUE")

    print("── Notebooks (from Git repo) ────────────────────────────────")
    # Create FROM the repo ROOT (MAIN_FILE points into notebooks/) so the whole repo —
    # runtime/ and domains_rideflow/ — is available to the notebook at runtime, not just
    # the .ipynb. Otherwise `import runtime` / reading contracts fails.
    repo_stage = f"@{db}.PIPELINE.RIDEFLOW_REPO/branches/{args.branch}/"
    for name, fname in NOTEBOOKS:
        run(cur, f"""CREATE OR REPLACE NOTEBOOK {db}.PIPELINE.{name}
            FROM '{repo_stage}' MAIN_FILE = 'notebooks/{fname}'
            QUERY_WAREHOUSE = {wh}
            COMPUTE_POOL = {pool}
            RUNTIME_NAME = 'SYSTEM$BASIC_RUNTIME'""")
        run(cur, f"ALTER NOTEBOOK {db}.PIPELINE.{name} ADD LIVE VERSION FROM LAST")

    print("── Task DAG ─────────────────────────────────────────────────")
    # Suspend any existing tasks first so CREATE OR REPLACE won't fail on resumed
    # dependents (a task with active/resumed children can't be replaced).
    for t in TASK_RESUME_ORDER:
        try:
            cur.execute(f"ALTER TASK IF EXISTS {db}.PIPELINE.{t} SUSPEND")
        except Exception:
            pass
    tsql = (REPO / "tasks" / "tasks.sql").read_text(encoding="utf-8")
    tsql = tsql.replace("{{DB}}", db).replace("{{WH}}", wh)
    for stmt in sql_statements(tsql):
        run(cur, stmt)
    for t in TASK_RESUME_ORDER:
        run(cur, f"ALTER TASK {db}.PIPELINE.{t} RESUME")

    if args.run:
        print("── Kick off a live run ──────────────────────────────────────")
        run(cur, f"EXECUTE TASK {db}.PIPELINE.T_RIDEFLOW_MARKETPLACE")

    conn.commit()
    print("✅ Snowflake Project deployed. Snowsight → Projects (Notebooks), "
          "Monitoring → Task History for runs.")


if __name__ == "__main__":
    main()
