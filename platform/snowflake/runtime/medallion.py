"""Shared LakeLogic-on-Snowflake medallion runtime.

The in-Snowflake Notebooks import this module so every task runs the medallion identically. It runs each contract with
LakeLogic's `snowflake` engine (schema casts, row rules, quarantine, transforms, gold
SQL, SCD2) on a caller-supplied `snowflake.connector` connection and persists the
result — the engine computes but does not persist non-Spark `table:` targets, so this
writes good rows to `<DOMAIN>.<table>` and rejected rows to `<DOMAIN>.QUARANTINE_<table>`.

In a Snowflake Notebook:

    from snowflake.snowpark.context import get_active_session
    from lakelogic.engines.snowflake import SnowflakeAdapter
    from snowflake.project.medallion import run_layer

    session = get_active_session()
    SnowflakeAdapter.set_shared_connection(session.connection)   # run on this session
    run_layer(session.connection, "<DATABASE>",
              "_contracts/<domain>/<system>/_system.yaml", "bronze")
"""
from __future__ import annotations

import os
from datetime import datetime, timezone


def _coerce_datetimes(df, cd):
    """write_pandas maps pandas datetimes to NUMBER (epoch), which breaks Gold's
    CAST/TRY_CAST to DATE/TIMESTAMP. Write date/timestamp fields as ISO strings so
    Snowflake stores VARCHAR and parses them correctly in downstream SQL."""
    import pandas as pd
    if df is None or not len(df):
        return df
    ftypes = {f["name"].upper(): str(f.get("type", "")).lower()
              for f in (cd.get("model", {}) or {}).get("fields", []) if f.get("name")}
    for col in df.columns:
        t = ftypes.get(str(col).upper(), "")
        is_dt = pd.api.types.is_datetime64_any_dtype(df[col])
        if t == "date":
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.strftime("%Y-%m-%d")
        elif t in ("timestamp", "datetime") or is_dt:
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")
    return df


def _apply_scd2_first_load(df, cd):
    """SCD2 effective-dating is normally applied by the engine's write/merge path, which
    is skipped for non-Spark table: targets — so effective_from/to/is_current arrive
    all-NULL. On a first load every row is the current version, so synthesise them."""
    if df is None or not len(df):
        return df
    write = cd.get("materialization", cd.get("write", {})) or {}
    if str(write.get("strategy", "")).lower() != "scd2":
        return df
    s = write.get("scd2", {}) or {}
    ef = (s.get("effective_from_field") or "effective_from").upper()
    et = (s.get("effective_to_field") or "effective_to").upper()
    cf = (s.get("current_flag_field") or "is_current").upper()
    sk = (s.get("surrogate_key") or "").upper()
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    cols = {c.upper(): c for c in df.columns}
    if ef in cols: df[cols[ef]] = now
    # Open-ended high-date sentinel (valid timestamp string → VARCHAR, not an all-NULL
    # column write_pandas would mistype as NUMBER; year 9999 is fine for Snowflake).
    if et in cols: df[cols[et]] = "9999-12-31 23:59:59"
    if cf in cols: df[cols[cf]] = True
    if sk and sk in cols:
        # Surrogate key (normally computed by the engine's SCD2 write path, skipped here):
        # a stable hash of the business columns.
        import hashlib
        biz = [c for c in df.columns if c.upper() not in {ef, et, cf, sk} and not str(c).startswith("_")]
        df[cols[sk]] = df[biz].astype(str).agg("|".join, axis=1).map(
            lambda x: hashlib.md5(x.encode()).hexdigest())
    return df


#: Formats the engine cannot run as SQL: their files are read locally (pdfplumber / easyocr)
#: on the notebook, with the polars engine, then written like any other result.
_FILE_FORMATS = {"pdf", "image", "png", "jpg", "jpeg", "tiff", "docx"}

_FILE_FORMAT_SQL = {
    "csv": "TYPE = CSV PARSE_HEADER = TRUE FIELD_OPTIONALLY_ENCLOSED_BY = '\"' "
           "ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE",
    "json": "TYPE = JSON STRIP_OUTER_ARRAY = TRUE",
    "parquet": "TYPE = PARQUET",
}


def _split_root(root):
    """``db.schema`` (any quoting) -> (DB, SCHEMA), upper-cased as Snowflake stores them."""
    parts = [p.strip('`"') for p in str(root).split(".") if p]
    if len(parts) != 2:
        raise ValueError(f"expected <database>.<schema>, got {root!r}")
    return parts[0].upper(), parts[1].upper()


def _stage_location(path):
    """``@stage/dir/**/*.csv`` -> (``@stage/dir``, ``.*[.]csv``); a plain folder has no pattern."""
    path = str(path)
    if "*" not in path:
        return path.rstrip("/"), None
    head, tail = path.split("*", 1)
    last = tail.rsplit("/", 1)[-1]
    ext = last.rsplit(".", 1)[-1] if "." in last else None
    return head.rstrip("/"), (f".*[.]{ext}" if ext else None)


def _stage_to_table(conn, path, fmt, entity):
    """Load a landing folder on a stage into a temporary table and return its name.

    The snowflake engine reads TABLES; landing is files. Every column is loaded as text — the
    bronze contract casts — matched to the header by name, so a reordered file still lands.
    """
    fmt = (fmt or "csv").lower()
    if fmt not in _FILE_FORMAT_SQL:
        raise ValueError(f"landing format {fmt!r} is not loaded on Snowflake (csv, json or parquet)")
    loc, pattern = _stage_location(path)
    tmp = "LL_LANDING_" + "".join(ch if ch.isalnum() else "_" for ch in str(entity)).upper()
    cur = conn.cursor()
    try:
        cur.execute(f"CREATE OR REPLACE TEMPORARY FILE FORMAT {tmp}_FF {_FILE_FORMAT_SQL[fmt]}")
        cols = [r[0] for r in cur.execute(
            f"SELECT COLUMN_NAME FROM TABLE(INFER_SCHEMA(LOCATION => '{loc}', FILE_FORMAT => '{tmp}_FF'))"
        ).fetchall()]
        if not cols:
            raise FileNotFoundError(f"no {fmt} files under {loc}")
        coldefs = ", ".join(f'"{c.upper()}" VARCHAR' for c in cols)
        cur.execute(f"CREATE OR REPLACE TEMPORARY TABLE {tmp} ({coldefs})")
        copy = (f"COPY INTO {tmp} FROM '{loc}' FILE_FORMAT = (FORMAT_NAME = '{tmp}_FF') "
                "MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE ON_ERROR = CONTINUE")
        if pattern:
            copy += f" PATTERN = '{pattern}'"
        cur.execute(copy)
    finally:
        cur.close()
    return tmp


def _stage_to_local(conn, path):
    """Download a stage folder's files to a local temp dir; return the local glob."""
    import os
    import tempfile

    loc, _pattern = _stage_location(path)
    local = tempfile.mkdtemp(prefix="ll_landing_")
    cur = conn.cursor()
    try:
        cur.execute(f"GET '{loc}/' 'file://{local}/'")
    finally:
        cur.close()
    tail = str(path)[len(loc):].lstrip("/") or "**/*"
    return os.path.join(local, tail).replace("\\", "/")


def _stamped(conn, table, cd):
    """SELECT over a result table that writes the way the frame path does: dates and timestamps
    as ISO text (_coerce_datetimes) and the engine's load time when the row has none
    (_stamp_load_time). Downstream layers read these tables as they always have."""
    from lakelogic.core.constants import META_PROCESSED_AT
    from snowflake.connector.constants import FIELD_ID_TO_NAME

    cur = conn.cursor()
    try:
        cur.execute(f"SELECT * FROM {table} LIMIT 0")
        desc = list(cur.description)
    finally:
        cur.close()
    out = []
    for d in desc:
        kind = FIELD_ID_TO_NAME.get(d[1], "")
        if kind == "DATE":
            out.append(f"TO_VARCHAR(\"{d[0]}\", 'YYYY-MM-DD') AS \"{d[0]}\"")
        elif kind.startswith("TIMESTAMP"):
            out.append(f"TO_VARCHAR(\"{d[0]}\", 'YYYY-MM-DD HH24:MI:SS') AS \"{d[0]}\"")
        else:
            out.append(f'"{d[0]}"')
    if META_PROCESSED_AT.upper() not in {d[0].upper() for d in desc}:
        out.append(f"TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYY-MM-DD HH24:MI:SS') AS {META_PROCESSED_AT.upper()}")
    return f"SELECT {', '.join(out)} FROM {table}"


def _stamp_load_time(df):
    """Every written row carries the engine's load time (`_lakelogic_processed_at`), as the Spark
    write path stamps it on Databricks and Fabric. Silver deduplicates on it (latest wins), so a
    bronze table without it fails silver (live run 2026-10-18: invalid identifier
    '_LAKELOGIC_PROCESSED_AT'). An existing value is kept: it is the original load time."""
    from lakelogic.core.constants import META_PROCESSED_AT

    if df is None or not len(df):
        return df
    if META_PROCESSED_AT.upper() not in {str(c).upper() for c in df.columns}:
        df[META_PROCESSED_AT] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    return df


def _log_run(proc, error, log=print, good=0, quarantined=0):
    """One run-log row per contract, as LakeLogic's pipeline runner writes on Databricks and
    Fabric: this runtime calls the processor directly, so without it the domain's
    LAKELOGIC.lakelogic_run_log stayed empty (live run 2026-10-18). Core's own writer, so the
    row has the same columns everywhere. A log that cannot be written never fails the run."""
    report = getattr(proc, "last_report", None) if proc is not None else None
    if not report:
        return
    try:
        from lakelogic.core.run_log import capture_failure, write_run_log

        if error is not None:
            report["status"] = "failed"
            report.update(capture_failure(error))
        else:
            # The runner's own mapping (succeeded / partial when every row was quarantined),
            # so a Snowflake row means what a Databricks or Fabric row means.
            from lakelogic.pipeline.runner import run_log_status

            all_q = not good and quarantined > 0
            report["status"] = run_log_status("all_rows_quarantined" if all_q else "success")
            if all_q:
                report["all_rows_quarantined"] = True
        write_run_log(report, proc.contract, engine_name=proc.engine_name,
                      run_log_mode=getattr(proc, "_run_log_mode", None))
    except Exception as exc:  # noqa: BLE001 - logging must not fail the pipeline
        log(f"    (run log not written: {str(exc)[:160]})")


def _topo(contracts):
    """Order contracts within a layer by intra-layer depends_on (entity names)."""
    by_entity = {c.entity: c for c in contracts}
    ordered, seen = [], set()

    def visit(c):
        if c.entity in seen:
            return
        seen.add(c.entity)
        for dep in (c.depends_on or []):
            if dep in by_entity:
                visit(by_entity[dep])
        ordered.append(c)

    for c in contracts:
        visit(c)
    return ordered


def _container_connect():
    """A new session from inside Snowflake: a notebook on the container runtime runs as a
    service, which is given a login token at /snowflake/session/token."""
    import snowflake.connector

    with open("/snowflake/session/token") as f:
        token = f.read()
    return snowflake.connector.connect(
        host=os.environ["SNOWFLAKE_HOST"], account=os.environ["SNOWFLAKE_ACCOUNT"],
        authenticator="oauth", token=token, port=os.environ.get("SNOWFLAKE_PORT", 443),
        protocol="https")


def _can_connect(new_conn, log):
    """Whether worker sessions can be opened here; if not, the layer runs one contract at a time."""
    try:
        new_conn().close()
        return True
    except Exception as exc:  # noqa: BLE001
        log(f"    (contracts run one at a time: no extra session available: {str(exc)[:120]})")
        return False


def run_layer(conn, database, sys_yaml, layer, log=print, environment="dev", connect=None):
    """Run + persist a single medallion layer for one system on `conn`.

    Returns a list of (entity, status, good_count, quarantined_count) tuples.
    """
    from lakelogic.core.registry import DomainRegistry, table_root
    from lakelogic.core.processor import DataProcessor
    from lakelogic.engines.snowflake import SnowflakeAdapter
    from snowflake.connector.pandas_tools import write_pandas

    SnowflakeAdapter.set_shared_connection(conn)   # engine runs on the caller's session
    reg = DomainRegistry.from_yaml(str(sys_yaml), environment=environment, storage_mode="catalog")
    results = []
    # One session per worker: the engine's temp views are session-wide (see _worker_conn).
    new_conn = connect or _container_connect
    import threading
    _tl = threading.local()
    cur = conn.cursor()
    try:
        wh, cdb, csch = cur.execute("SELECT CURRENT_WAREHOUSE(), CURRENT_DATABASE(), CURRENT_SCHEMA()").fetchone()
    finally:
        cur.close()

    def _worker_conn():
        c = getattr(_tl, "c", None)
        if c is None:
            c = new_conn()
            for q in ([f"USE WAREHOUSE {wh}"] if wh else []) + ([f'USE DATABASE "{cdb}"'] if cdb else [])                     + ([f'USE SCHEMA "{cdb}"."{csch}"'] if cdb and csch else []):
                c.cursor().execute(q)
            _tl.c = c
            opened.append(c)
        SnowflakeAdapter.set_shared_connection(c, this_thread=True)
        return c

    opened = []

    def _one(rc):
        conn = _worker_conn() if parallel else outer_conn
        results = []
        cd = rc.contract_dict or {}
        info = cd.get("info", {})
        table = (info.get("table_name") or rc.entity)
        # Where this layer's tables live, from the registry: `<DB>.<DOMAIN>` (one database) or
        # `<PREFIX>_<DOMAIN>.<LAYER>` (a database per domain). `database` is the fallback.
        root = table_root(reg.storage, layer)
        db, schema = _split_root(root) if root else (database, (info.get("domain") or "").upper())
        q_root = getattr(reg.storage, "quarantine_root", None)
        q_db, q_schema = _split_root(q_root) if q_root else (db, schema)
        src = cd.get("source", {}) or {}
        source_ref = src.get("path", "")
        engine = "snowflake"
        if cd.get("external_logic"):
            # ROWS FROM ITS OWN GENERATOR (the date dimension). Its source is a landing seed
            # folder, which Snowflake has no volume for; the generator replaces the input rows
            # anyway, so it starts from zero rows carrying the model's columns.
            import pandas as _pd

            fields = (cd.get("model") or {}).get("fields") or []
            source_ref = _pd.DataFrame(columns=[f["name"] for f in fields])
        proc = None
        try:
            fmt = str(src.get("format") or "").lower()
            if isinstance(source_ref, str) and source_ref.startswith("@"):
                if fmt in _FILE_FORMATS:
                    source_ref, engine = _stage_to_local(conn, source_ref), "polars"
                else:
                    source_ref = _stage_to_table(conn, source_ref, fmt, rc.entity)
            proc = DataProcessor(cd, engine=engine)
            good, bad = proc.run(source_ref)
            strategy = str(((cd.get("materialization") or cd.get("write") or {}).get("strategy")) or "append").lower()
            adapter = getattr(proc, "adapter", None)
            if engine == "snowflake" and strategy != "scd2" and getattr(adapter, "good_table", None):
                # IN PLACE: the results are already tables in this session. Written by the
                # contract's own strategy (append / overwrite / merge on its primary key), and
                # rejected rows appended to quarantine. The rows used to come to the driver and
                # go back through write_pandas(overwrite=True): seven statements a table, and
                # every strategy turned into an overwrite (2026-11-02).
                ng = 0 if good is None else len(good)
                nb = 0 if bad is None else len(bad)
                if ng:
                    adapter.write_in_place(conn, _stamped(conn, adapter.good_table, cd),
                                           f'"{db}"."{schema}"."{table.upper()}"', strategy,
                                           merge_keys=list(cd.get("primary_key") or []))
                if nb:
                    _qname = str(getattr(reg.storage, "quarantine_table_name", None) or "quarantine_{table}")
                    adapter.write_in_place(conn, _stamped(conn, adapter.bad_table, cd),
                                           f'"{q_db}"."{q_schema}"."{_qname.format(table=table, domain=schema.lower()).upper()}"',
                                           "append")
                log(f"    {rc.entity:<42} {layer:<7} good={ng:<6} quarantined={nb}")
                results.append((rc.entity, "ok", ng, nb))
                _log_run(proc, None, log, good=ng, quarantined=nb)
                return results[0]
            if engine == "polars":
                good = None if good is None else good.to_pandas()
                bad = None if bad is None else bad.to_pandas()
            good = _coerce_datetimes(good, cd)
            good = _apply_scd2_first_load(good, cd)
            good = _stamp_load_time(good)
            ng = 0 if good is None else len(good)
            nb = 0 if bad is None else len(bad)
            if ng:
                # Unquoted, so the columns fold to UPPER like every other Snowflake name and
                # the next layer's contract (lowercase) matches them.
                write_pandas(conn, good, table_name=table.upper(), schema=schema, database=db,
                             auto_create_table=True, overwrite=True, quote_identifiers=False)
            if nb:
                # Rejected rows sit in the DOMAIN schema as quarantine_<table> (every platform).
                _qname = str(getattr(reg.storage, "quarantine_table_name", None) or "quarantine_{table}")
                write_pandas(conn, bad, table_name=_qname.format(table=table, domain=schema.lower()).upper(),
                             schema=q_schema, database=q_db, auto_create_table=True, overwrite=True,
                             quote_identifiers=False)
            log(f"    {rc.entity:<42} {layer:<7} good={ng:<6} quarantined={nb}")
            results.append((rc.entity, "ok", ng, nb))
            _log_run(proc, None, log, good=ng, quarantined=nb)
        except Exception as e:
            log(f"    {rc.entity:<42} {layer:<7} FAILED: {str(e)[:180]}")
            results.append((rc.entity, f"failed: {str(e)[:180]}", 0, 0))
            _log_run(proc, e, log)
        return results[0]

    # Contracts that do not depend on each other within the layer run at the same time, on
    # the one session. Each statement costs ~0.3 s whatever the row count, so run one by one a
    # 9-contract layer took ~65 s; in waves it takes about as long as its slowest contract.
    contracts = _topo(reg.get_active_contracts(layer=layer))
    names = {c.entity for c in contracts}
    level = {}
    for c in contracts:  # _topo order: a contract's dependencies come before it
        level[c.entity] = 1 + max([level[d] for d in (c.depends_on or []) if d in names] or [-1])
    workers = max(1, int(os.environ.get("LAKELOGIC_PARALLEL_CONTRACTS", "8")))
    outer_conn = conn
    parallel = workers > 1 and new_conn is not None and _can_connect(new_conn, log)
    from concurrent.futures import ThreadPoolExecutor
    results = []
    for wave in sorted(set(level.values())):
        batch = [c for c in contracts if level[c.entity] == wave]
        if not parallel:
            results += [_one(c) for c in batch]
            continue
        with ThreadPoolExecutor(max_workers=min(workers, len(batch))) as pool:
            results += list(pool.map(_one, batch))
    for c in opened:
        try:
            c.close()
        except Exception:  # noqa: BLE001 - closing a worker session never fails the layer
            pass
    return results
