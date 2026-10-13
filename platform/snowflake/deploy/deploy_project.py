#!/usr/bin/env python
"""Deploy the RideFlow marketplace medallion as a native Snowflake Project.

Wires the Git-connected Workspace end-to-end:
  1. SECRET (GitHub PAT) + API INTEGRATION + GIT REPOSITORY  -> repo browsable in Snowsight
  2. COMPUTE POOL                                            -> container runtime for Notebooks
  3. CREATE NOTEBOOK NB_PIPELINE (from the repo)            -> one parameterized driver
  4. Task DAG from platform/snowflake/tasks/tasks.sql (EXECUTE NOTEBOOK w/args) -> run history in Task Graph
  5. (--run) EXECUTE TASK                                    -> kick off one live run

Prereqs (platform/snowflake/deploy/.env, gitignored):
  SNOWFLAKE_* (account/user/password|key, role, warehouse, database)  — same as deploy.py
  GITHUB_REPO_URL = https://github.com/<owner>/<repo>
  GITHUB_USER     = <github username>
  GITHUB_PAT      = <fine-grained PAT with repo read>

Usage:  python platform/snowflake/deploy/deploy_project.py [--branch main] [--run]
"""
from __future__ import annotations

import argparse
import importlib.util
import os
import re
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]      # contracts/ live at the repository root
PLATFORM = Path(__file__).resolve().parents[1]  # platform/snowflake
sys.path.insert(0, str(PLATFORM))

# reuse connect()/load_env() from the sibling deploy.py
_spec = importlib.util.spec_from_file_location("_deploy", str(PLATFORM / "deploy" / "deploy.py"))
_d = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_d)

NOTEBOOKS = [
    ("NB_PIPELINE", "pipeline_driver.ipynb"),   # one parameterized driver for every layer
]
TASK_RESUME_ORDER = [   # children first, root last, so the graph is armed
    "T_SHARED", "T_REFERENCE", "T_OPS_ZENDESK", "T_OPS_CHECKR", "T_OPS_TWILIO",
    "T_MKT_META", "T_MKT_HUBSPOT", "T_MKT_GA", "T_MKT_GOOGLE_ADS",
    "T_PAYMENTS", "T_MARKETPLACE", "T_RIDEFLOW_MESH",
]

# ── Data-mesh DAG definition (task, domain, system, FK-parent tasks) ─────────
# FK parents mirror the Fabric mesh: marketplace + reference are roots; payments /
# operations / marketing read marketplace ids; zendesk enriches payments charges;
# shared marts join every other system ("*").
ROOT = "T_RIDEFLOW_MESH"
MESH_SYSTEMS = [  # (task, domain, system, [fk-parent tasks])
    ("T_MARKETPLACE",     "marketplace", "rideflow",         []),
    ("T_REFERENCE",       "reference",   "internal",         []),
    ("T_PAYMENTS",        "payments",    "stripe",           ["T_MARKETPLACE"]),
    ("T_OPS_CHECKR",      "operations",  "checkr",           ["T_MARKETPLACE"]),
    ("T_OPS_TWILIO",      "operations",  "twilio",           ["T_MARKETPLACE"]),
    ("T_OPS_ZENDESK",     "operations",  "zendesk",          ["T_PAYMENTS"]),
    ("T_MKT_GOOGLE_ADS",  "marketing",   "google_ads",       ["T_MARKETPLACE"]),
    ("T_MKT_GA",          "marketing",   "google_analytics", ["T_MARKETPLACE"]),
    ("T_MKT_HUBSPOT",     "marketing",   "hubspot",          ["T_MARKETPLACE"]),
    ("T_MKT_META",        "marketing",   "meta_ads",         ["T_MARKETPLACE"]),
    ("T_SHARED",          "shared",      "shared",           ["*"]),  # * = all others
]


def _mesh_after(max_parallel):
    """Return {task: [predecessor tasks]} for the whole mesh.
    max_parallel <= 0  → NATURAL FK DAG (maximum concurrency; each task after its FK
                          parents only). Peak width ≈ 7.
    max_parallel  = N  → FK-ordered WAVES of ≤N (each task after the whole previous
                          wave), bounding concurrency to N."""
    tasks = [t for t, _, _, _ in MESH_SYSTEMS]
    # FK parents per task; shared's "*" expands to every other system task.
    fk = {}
    for t, _, _, ps in MESH_SYSTEMS:
        fk[t] = [x for x in tasks if x != t] if ps == ["*"] else list(ps)
    if max_parallel is not None and max_parallel <= 0:
        return {t: (ps or [ROOT]) for t, ps in fk.items()}
    # Wave packing over an FK-valid order (MESH_SYSTEMS is already topo-sorted).
    n = max(1, int(max_parallel)); waves, cur, curset = [], [], set()
    for t in tasks:
        if cur and (len(cur) >= n or (set(fk[t]) & curset)):
            waves.append(cur); cur, curset = [], set()
        cur.append(t); curset.add(t)
    if cur:
        waves.append(cur)
    after, prev = {}, [ROOT]
    for wave in waves:
        for t in wave:
            after[t] = list(prev)
        prev = wave
    return after


def gen_task_dag(db, wh, max_parallel):
    """Build the CREATE TASK statements for the mesh + return (sql, peak_concurrency)."""
    after = _mesh_after(max_parallel)
    lines = [f"USE DATABASE {db};", "USE SCHEMA PIPELINE;",
             f"CREATE OR REPLACE TASK {ROOT} WAREHOUSE = {wh} "
             f"SCHEDULE = 'USING CRON 0 3 * * * UTC' "
             f"COMMENT = 'RideFlow data-mesh root' AS SELECT 1;"]
    dom = {t: (d, s) for t, d, s, _ in MESH_SYSTEMS}
    for t, _, _, _ in MESH_SYSTEMS:
        d, s = dom[t]
        lines.append(
            f"CREATE OR REPLACE TASK {t} WAREHOUSE = {wh} AFTER {', '.join(after[t])} "
            f"AS EXECUTE NOTEBOOK {db}.PIPELINE.NB_PIPELINE('domain={d} system={s} layer=all');")
    # Peak concurrency = widest simultaneously-ready set (approx by dependency level).
    level = {ROOT: 0}
    for t, _, _, _ in MESH_SYSTEMS:
        level[t] = 1 + max(level.get(p, 0) for p in after[t])
    from collections import Counter
    peak = max(Counter(level[t] for t, _, _, _ in MESH_SYSTEMS).values())
    return "\n".join(lines), peak


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
    ap.add_argument("--mesh-max-parallel", type=int, default=0,
                    help="Max systems the Task DAG runs at once. 0 (default) = natural FK DAG "
                         "(maximum concurrency, peak ~7-8; pool auto-sizes to MAX_NODES=8 — "
                         "validated on the trial, whole mesh in ~9 min). N = FK-ordered waves "
                         "of N (e.g. 2 keeps a tighter node footprint); 1 = fully sequential.")
    args = ap.parse_args()
    _d.load_env()

    db = os.getenv("SNOWFLAKE_DATABASE", "RIDEFLOW_DEV_DEMO")
    wh = os.getenv("SNOWFLAKE_WAREHOUSE", "RIDEFLOW_WH")
    pool = os.getenv("SNOWFLAKE_COMPUTE_POOL", "RIDEFLOW_POOL")
    # Public repo → no credentials needed. GITHUB_USER + GITHUB_PAT are only required
    # if you point GITHUB_REPO_URL at a PRIVATE fork.
    repo_url = os.getenv("GITHUB_REPO_URL",
                         "https://github.com/LakeLogic/lakelogic-snowflake-data-mesh-lakehouse").strip()
    gh_user = os.getenv("GITHUB_USER", "").strip()
    pat = os.getenv("GITHUB_PAT", "").strip()
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
    if gh_user and pat:
        # Private fork → authenticate with a credential secret.
        print("  (authenticated: using GITHUB_USER/GITHUB_PAT for a private repo)")
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
    else:
        # Public repo → clone anonymously, no secret.
        print("  (public repo: cloning without credentials)")
        run(cur, f"""CREATE OR REPLACE API INTEGRATION GIT_API_RIDEFLOW
            API_PROVIDER = git_https_api
            API_ALLOWED_PREFIXES = ('https://github.com/{owner}')
            ENABLED = TRUE""")
        run(cur, f"""CREATE OR REPLACE GIT REPOSITORY RIDEFLOW_REPO
            API_INTEGRATION = GIT_API_RIDEFLOW
            ORIGIN = '{repo_url}'""")
    run(cur, f"ALTER GIT REPOSITORY RIDEFLOW_REPO FETCH")

    # Size the pool to the DAG's peak concurrency (+1 headroom), capped at 8 so a
    # trial can't be asked for an absurd number of nodes. A wider fan-out than the
    # pool can schedule throws 'unschedulable in full compute pool'.
    dag_sql, peak = gen_task_dag(db, wh, args.mesh_max_parallel)
    pool_nodes = min(8, peak + 1)
    print(f"── Compute pool (mesh_max_parallel={args.mesh_max_parallel}, peak≈{peak} "
          f"→ MAX_NODES={pool_nodes}) ───")
    run(cur, f"""CREATE COMPUTE POOL IF NOT EXISTS {pool}
        MIN_NODES = 1 MAX_NODES = {pool_nodes} INSTANCE_FAMILY = CPU_X64_XS
        AUTO_SUSPEND_SECS = 300""")
    # Ensure an already-existing pool is resized too (CREATE IF NOT EXISTS is a no-op).
    run(cur, f"ALTER COMPUTE POOL {pool} SET MAX_NODES = {pool_nodes}")

    print("── Stage the LakeLogic wheelhouse (headless-run install) ────")
    # Headless EXECUTE NOTEBOOK task runs don't have Snowflake's internal PyPI mirror, so
    # they install LakeLogic (official 1.41.0) + pure-python deps from wheels staged here
    # with pip --no-index. The container runtime ships pandas/pydantic/pyyaml/etc.
    run(cur, f"CREATE STAGE IF NOT EXISTS {db}.PIPELINE.WHEELS")
    for whl in sorted((PLATFORM / "wheelhouse").glob("*.whl")):
        uri = "file://" + str(whl).replace("\\", "/")
        run(cur, f"PUT '{uri}' @{db}.PIPELINE.WHEELS AUTO_COMPRESS = FALSE OVERWRITE = TRUE")

    print("── Notebooks (from Git repo) ────────────────────────────────")
    # Create FROM the repo ROOT (MAIN_FILE points into platform/snowflake/notebooks/) so the
    # whole repo — platform/snowflake/runtime/ and contracts/ — is available to the notebook at runtime, not just
    # the .ipynb. Otherwise `import runtime` / reading contracts fails.
    repo_stage = f"@{db}.PIPELINE.RIDEFLOW_REPO/branches/{args.branch}/"
    for name, fname in NOTEBOOKS:
        run(cur, f"""CREATE OR REPLACE NOTEBOOK {db}.PIPELINE.{name}
            FROM '{repo_stage}' MAIN_FILE = 'platform/snowflake/notebooks/{fname}'
            QUERY_WAREHOUSE = {wh}
            COMPUTE_POOL = {pool}
            RUNTIME_NAME = 'SYSTEM$BASIC_RUNTIME'""")
        run(cur, f"ALTER NOTEBOOK {db}.PIPELINE.{name} ADD LIVE VERSION FROM LAST")

    print("── Task DAG ─────────────────────────────────────────────────")
    # Clean slate: suspend + drop ALL existing tasks in PIPELINE (handles renamed/removed
    # tasks from earlier deploys), then recreate fresh from tasks.sql. Dropping a task with
    # successors fails, so retry a few passes (leaves drop first).
    cur.execute(f"SHOW TASKS IN SCHEMA {db}.PIPELINE")
    for t in [r[1] for r in cur.fetchall()]:
        try: cur.execute(f"ALTER TASK IF EXISTS {db}.PIPELINE.{t} SUSPEND")
        except Exception: pass
    for _ in range(4):
        cur.execute(f"SHOW TASKS IN SCHEMA {db}.PIPELINE")
        left = [r[1] for r in cur.fetchall()]
        if not left:
            break
        for t in left:
            try: cur.execute(f"DROP TASK IF EXISTS {db}.PIPELINE.{t}")
            except Exception: pass
    # DAG is generated from MESH_SYSTEMS per --mesh-max-parallel (dag_sql built above);
    # tasks/tasks.sql is kept as the human-readable cap-2 reference snapshot.
    for stmt in sql_statements(dag_sql):
        run(cur, stmt)
    for t in TASK_RESUME_ORDER:
        run(cur, f"ALTER TASK {db}.PIPELINE.{t} RESUME")

    if args.run:
        print("── Kick off a live run ──────────────────────────────────────")
        run(cur, f"EXECUTE TASK {db}.PIPELINE.T_RIDEFLOW_MESH")  # root task kicks the whole graph

    conn.commit()
    print("✅ Snowflake Project deployed. Snowsight → Projects (Notebooks), "
          "Monitoring → Task History for runs.")


if __name__ == "__main__":
    main()
