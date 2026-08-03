"""Shared LakeLogic-on-Snowflake medallion runtime.

Both the local deploy (`snowflake/deploy/deploy.py`) and the in-Snowflake Notebooks
import this module so they run the medallion *identically*. It runs each contract with
LakeLogic's `snowflake` engine (schema casts, row rules, quarantine, transforms, gold
SQL, SCD2) on a caller-supplied `snowflake.connector` connection and persists the
result — the engine computes but does not persist non-Spark `table:` targets, so this
writes good rows to `<DOMAIN>.<table>` and rejected rows to `QUARANTINE`.

In a Snowflake Notebook:

    from snowflake.snowpark.context import get_active_session
    from lakelogic.engines.snowflake import SnowflakeAdapter
    from snowflake.project.medallion import run_layer

    session = get_active_session()
    SnowflakeAdapter.set_shared_connection(session.connection)   # run on this session
    run_layer(session.connection, "RIDEFLOW_DEV_DEMO",
              "domains_rideflow/marketplace/rideflow/_system.yaml", "bronze")
"""
from __future__ import annotations

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
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    cols = {c.upper(): c for c in df.columns}
    if ef in cols: df[cols[ef]] = now
    # Open-ended high-date sentinel (valid timestamp string → VARCHAR, not an all-NULL
    # column write_pandas would mistype as NUMBER; year 9999 is fine for Snowflake).
    if et in cols: df[cols[et]] = "9999-12-31 23:59:59"
    if cf in cols: df[cols[cf]] = True
    return df


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


def run_layer(conn, database, sys_yaml, layer, log=print):
    """Run + persist a single medallion layer for one system on `conn`.

    Returns a list of (entity, status, good_count, quarantined_count) tuples.
    """
    from lakelogic.core.registry import DomainRegistry
    from lakelogic.core.processor import DataProcessor
    from lakelogic.engines.snowflake import SnowflakeAdapter
    from snowflake.connector.pandas_tools import write_pandas

    SnowflakeAdapter.set_shared_connection(conn)   # engine runs on the caller's session
    reg = DomainRegistry.from_yaml(str(sys_yaml), environment="dev", storage_mode="uc")
    results = []
    for rc in _topo(reg.get_active_contracts(layer=layer)):
        cd = rc.contract_dict or {}
        info = cd.get("info", {})
        table = (info.get("table_name") or rc.entity)
        schema = (info.get("domain") or "").upper()
        source_ref = (cd.get("source", {}) or {}).get("path", "")
        try:
            good, bad = DataProcessor(cd, engine="snowflake").run(source_ref)
            good = _coerce_datetimes(good, cd)
            good = _apply_scd2_first_load(good, cd)
            ng = 0 if good is None else len(good)
            nb = 0 if bad is None else len(bad)
            if ng:
                write_pandas(conn, good, table_name=table.upper(), schema=schema, database=database,
                             auto_create_table=True, overwrite=True, quote_identifiers=True)
            if nb:
                write_pandas(conn, bad, table_name=f"{schema}_{table}".upper(), schema="QUARANTINE",
                             database=database, auto_create_table=True, overwrite=True, quote_identifiers=True)
            log(f"    {rc.entity:<42} {layer:<7} good={ng:<6} quarantined={nb}")
            results.append((rc.entity, "ok", ng, nb))
        except Exception as e:
            log(f"    {rc.entity:<42} {layer:<7} FAILED: {str(e)[:180]}")
            results.append((rc.entity, f"failed: {str(e)[:180]}", 0, 0))
    return results
