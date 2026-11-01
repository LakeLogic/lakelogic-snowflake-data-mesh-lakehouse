"""Operational jobs on Snowflake: SLO checks, retention check, erasure, maintenance.

The Snowflake form of the jobs every LakeLogic project ships (``platform_project_layout``):

* ``slo``         — monitor: freshness / row-count / quality objectives. ``fail_on_breach``.
* ``retention``   — privacy: oldest record vs the declared retention window. Check-only.
* ``erasure``     — privacy: find (dry run) or nullify a data subject's rows.
* ``maintenance`` — admin: Snowflake manages micro-partitions itself, so there is no
  OPTIMIZE/VACUUM to run; this job reports each table's rows and bytes so the step exists
  and says what it measured.

Each is run by its own notebook (``notebooks/nb_slo_checks.ipynb``, ``nb_compliance_erasure``, ...) from a task in
``tasks/<group>/``. Nothing here reads credentials; it runs on the notebook's session.
"""
from __future__ import annotations

import datetime
import uuid
from typing import Any, Dict, List, Optional

def _evidence_table(kind: str) -> str:
    """The engine-owned evidence table for ``kind`` — named by the engine
    (lakelogic/core/metadata_names), in the DOMAIN schema beside the run log."""
    from lakelogic.core.metadata_names import metadata_table_name

    return metadata_table_name(kind, backend="snowflake").upper()


def _truthy(v: Any) -> bool:
    return str(v or "").strip().lower() in ("1", "true", "yes")


def _registry(sys_yaml: str, environment: str):
    from lakelogic.core.registry import DomainRegistry

    return DomainRegistry.from_yaml(str(sys_yaml), environment=environment, storage_mode="catalog")


def _table_of(registry, contract) -> Optional[str]:
    info = (contract.contract_dict or {}).get("info") or {}
    table = str(info.get("table_name") or "").strip()
    if not table:
        return None
    return f"{registry.domain}.{table}".upper()


def _exists(conn, database: str, table: str) -> bool:
    schema, name = table.split(".", 1)
    cur = conn.cursor()
    cur.execute(
        f"SELECT COUNT(*) FROM {database}.INFORMATION_SCHEMA.TABLES "
        "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s", (schema, name),
    )
    return bool(cur.fetchone()[0])


def slo_checks(conn, database: str, sys_yaml: str, *, environment: str = "dev",
               fail_on_breach: bool = True, pipeline_run_id: Optional[str] = None, log=print) -> Dict[str, Any]:
    """Run the registry's service levels on Snowflake. Raises on a breach when
    ``fail_on_breach`` (so the task fails and alerts); otherwise records and returns."""
    from lakelogic.core.slo import SLOValidator

    registry = _registry(sys_yaml, environment)
    registry.retention = {}  # the retention job measures that
    report = SLOValidator(registry, snowflake_con=conn).run_checks(
        environment=environment, pipeline_run_id=pipeline_run_id or None,
    )
    for r in report.results:
        log(f"  {getattr(r, 'layer', '?'):<8} {getattr(r, 'entity', '?'):<40} {getattr(r, 'status', '?')}")
    if not report.results:
        log("No checks were evaluated. Nothing was measured, so nothing passed.")
    try:
        from lakelogic.core.run_log import emit_slo_report

        emit_slo_report(registry, report, environment=environment, pipeline_run_id=pipeline_run_id or None)
    except Exception as exc:  # telemetry is optional
        log(f"SLO report not sent to LakeLogic ({type(exc).__name__}).")
    if not report.passed and fail_on_breach:
        raise RuntimeError(f"Service level checks failed: {len(report.failures)} breach(es).")
    return {"checks": len(report.results), "passed": report.passed}


def retention_check(conn, database: str, sys_yaml: str, *, environment: str = "dev",
                    fail_on_breach: bool = True, log=print) -> Dict[str, Any]:
    """Compare each table's oldest record with its layer's retention window. Never deletes.
    Observations go to the domain's ``_LAKELOGIC_RETENTION_EVIDENCE``."""
    from lakelogic.core.slo import SLOValidator

    registry = _registry(sys_yaml, environment)
    if not registry.retention:
        log("No retention declared (not configured). Nothing to check.")
        return {"checks": 0, "breached": 0}
    results = SLOValidator(registry, snowflake_con=conn).check_retention()
    run_id, now = str(uuid.uuid4()), datetime.datetime.now(datetime.timezone.utc).isoformat()
    rows, breached = [], 0
    for r in results:
        is_breach = getattr(r, "passed", None) is False
        breached += int(is_breach)
        rows.append({
            "RUN_ID": run_id, "TIMESTAMP": now,
            "TABLE_NAME": f"{registry.domain}.{getattr(r, 'layer', '')}.{getattr(r, 'entity', '')}",
            "STATUS": "breached" if is_breach else "passed",
            "ROWS_EXPIRED": None,  # check-only: not measured, never 0
            "DETAIL": str(getattr(r, "message", None) or getattr(r, "status", "")),
        })
        log(f"  {rows[-1]['STATUS']:<9} {rows[-1]['TABLE_NAME']}")
    _write_evidence(conn, database, registry.domain, _evidence_table("retention_evidence"), rows, log)
    if breached and fail_on_breach:
        raise RuntimeError(f"Retention check: {breached} table(s) hold data past their retention period.")
    return {"checks": len(results), "breached": breached}


def erasure(conn, database: str, sys_yaml: str, *, subject_column: str, subject_ids: List[str],
            environment: str = "dev", strategy: str = "nullify", dry_run: bool = True,
            confirm: str = "", case_ref: str = "", log=print) -> Dict[str, Any]:
    """Find a data subject's rows in every table that has ``subject_column``; with
    ``dry_run=False`` AND ``confirm == "ERASE"`` AND a ``case_ref``, nullify the contract's
    PII columns in those rows. Only ``nullify`` is implemented on Snowflake — any other
    strategy is refused rather than approximated."""
    if not subject_column or not subject_ids:
        raise ValueError("erasure needs subject_column and at least one subject id")
    execute = (not dry_run) and confirm == "ERASE" and bool(case_ref.strip())
    if not dry_run and not execute:
        raise ValueError("erasing needs dry_run=false AND confirm=ERASE AND a case_ref")
    if execute and strategy != "nullify":
        raise ValueError(f"strategy {strategy!r} is not implemented on Snowflake; use nullify")
    registry = _registry(sys_yaml, environment)
    placeholders = ", ".join(["%s"] * len(subject_ids))
    out, evidence = [], []
    for c in registry.get_active_contracts():
        table = _table_of(registry, c)
        fields = ((c.contract_dict or {}).get("model") or {}).get("fields") or []
        names = {str(f.get("name", "")).lower() for f in fields}
        if not table or subject_column.lower() not in names or not _exists(conn, database, table):
            continue
        cur = conn.cursor()
        cur.execute(f'SELECT COUNT(*) FROM {database}.{table} WHERE "{subject_column.upper()}" IN ({placeholders})',
                    subject_ids)
        n = cur.fetchone()[0]
        pii = [str(f["name"]) for f in fields if f.get("pii") and str(f.get("name")).lower() != subject_column.lower()]
        if execute and n and pii:
            sets = ", ".join(f'"{p.upper()}" = NULL' for p in pii)
            cur.execute(f'UPDATE {database}.{table} SET {sets} WHERE "{subject_column.upper()}" IN ({placeholders})',
                        subject_ids)
        out.append({"table": table, "rows": n, "pii_columns": pii})
        evidence.append({
            "RUN_ID": case_ref or "dry-run", "TIMESTAMP": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "TABLE_NAME": table, "ROWS_MATCHED": n, "STRATEGY": strategy,
            "STATUS": "erased" if execute and n and pii else ("would_erase" if n else "no_match"),
        })
        log(f"  {table:<48} rows={n} {'ERASED' if execute and n and pii else 'dry run'}")
    if execute:
        _write_evidence(conn, database, registry.domain, _evidence_table("erasure_evidence"), evidence, log)
    return {"tables": out, "executed": execute}


def maintenance(conn, database: str, sys_yaml: str, *, environment: str = "dev", log=print) -> Dict[str, Any]:
    """Report each governed table's row count and bytes. Snowflake reclusters and purges on
    its own, so there is no OPTIMIZE/VACUUM equivalent to run here."""
    registry = _registry(sys_yaml, environment)
    out = []
    cur = conn.cursor()
    for c in registry.get_active_contracts():
        table = _table_of(registry, c)
        if not table:
            continue
        schema, name = table.split(".", 1)
        cur.execute(
            f"SELECT ROW_COUNT, BYTES FROM {database}.INFORMATION_SCHEMA.TABLES "
            "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s", (schema, name),
        )
        row = cur.fetchone()
        out.append({"table": table, "rows": row[0] if row else None, "bytes": row[1] if row else None})
        log(f"  {table:<48} {'not created' if row is None else f'{row[0]} rows, {row[1]} bytes'}")
    return {"tables": out}


def _write_evidence(conn, database: str, domain: str, table: str, rows: List[Dict[str, Any]], log) -> None:
    if not rows:
        return
    import pandas as pd
    from snowflake.connector.pandas_tools import write_pandas

    write_pandas(conn, pd.DataFrame(rows), table_name=table, schema=str(domain).upper(),
                 database=database, auto_create_table=True, overwrite=False, quote_identifiers=True)
    log(f"Recorded {len(rows)} row(s) in {database}.{str(domain).upper()}.{table}.")
