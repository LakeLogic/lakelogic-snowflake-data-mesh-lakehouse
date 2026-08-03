"""Landing ingestion for the RideFlow demo — shared by the local deploy and the
in-Snowflake landing Notebook.

LANDING is the raw drop zone: this generates FK-consistent synthetic RideFlow data with
LakeLogic's simulator and loads each entity into ``LANDING.<system>_<entity>`` as all-
VARCHAR tables (Bronze does the typed casts). In production these LANDING tables would
instead be fed by a managed connector / COPY INTO from a stage.
"""
from __future__ import annotations

import glob
import tempfile
from pathlib import Path


def generate_marketplace(landing_root):
    """FK-consistent marketplace landing via LakeLogic's native simulator."""
    from datetime import datetime, timedelta, timezone
    from lakelogic.core.streaming import StreamingSimulator
    sim = StreamingSimulator.rideflow_marketplace(
        landing_root=landing_root, window_minutes=60,
        start_time=datetime.now(timezone.utc) - timedelta(hours=6),
        seed=42, initial_riders=120, initial_drivers=60)
    for _ in sim.run(num_windows=6, micro_batches=6, up_to=datetime.now(timezone.utc), resume=True):
        pass


# domain/system -> generator function
GENERATORS = {("marketplace", "rideflow"): generate_marketplace}


def _stage_name(domain):
    # One internal stage per domain ("volume"), e.g. LANDING.LANDING_MARKETPLACE.
    return f"LANDING.LANDING_{domain.upper()}"


def _read_entity_frame(entity_dir):
    """Concat an entity's generated CSV/JSON files into one all-string DataFrame."""
    import pandas as pd
    frames = []
    for f in glob.glob(str(entity_dir / "**" / "*.csv"), recursive=True):
        try: frames.append(pd.read_csv(f, dtype=str, keep_default_na=False))
        except Exception: pass
    for f in glob.glob(str(entity_dir / "**" / "*.json"), recursive=True):
        try: frames.append(pd.read_json(f, dtype=False).astype(str))
        except Exception: pass
    if not frames:
        return None
    df = pd.concat(frames, ignore_index=True)
    df.columns = [c.upper() for c in df.columns]   # Snowflake-native casing
    return df


def _load_frame_direct(conn, db, table, df):
    """Fast path: write_pandas straight into LANDING.<table> (all VARCHAR)."""
    from snowflake.connector.pandas_tools import write_pandas
    write_pandas(conn, df, table_name=table, schema="LANDING", database=db,
                 auto_create_table=True, overwrite=True, quote_identifiers=True)


def _load_partitioned_via_stage(conn, db, domain, system, gen_dir, log):
    """Native 'volume' path that mirrors the Databricks/Fabric demos: PUT the simulator's
    files into the per-domain stage **preserving the Hive-style partition tree**
    (<entity>/y_YYYY/m_MM/d_DD/h_HH/batch_*.csv), then COPY INTO an all-VARCHAR
    LANDING.<system>_<entity> recursively. Files stay browsable in Snowsight + COPY_HISTORY."""
    stage = _stage_name(domain)
    cur = conn.cursor()
    cur.execute(f"CREATE STAGE IF NOT EXISTS {db}.{stage} DIRECTORY = (ENABLE = TRUE)")
    fmt = "(TYPE=CSV SKIP_HEADER=1 FIELD_OPTIONALLY_ENCLOSED_BY='\"' EMPTY_FIELD_AS_NULL=FALSE)"
    gen_root = Path(gen_dir)
    loaded = []
    for entity_dir in sorted(p for p in gen_root.iterdir() if p.is_dir()):
        entity = entity_dir.name
        files = sorted(entity_dir.rglob("*.csv"))
        if not files:
            continue
        # PUT each batch preserving its <entity>/y_/m_/d_/h_/ path (AUTO_COMPRESS off so
        # the .csv files stay visible in the stage, matching the Fabric/Databricks layout).
        for f in files:
            rel_dir = f.parent.relative_to(gen_root).as_posix()
            uri = "file://" + str(f).replace("\\", "/")
            cur.execute(f"PUT '{uri}' @{db}.{stage}/{rel_dir}/ AUTO_COMPRESS = FALSE OVERWRITE = TRUE")
        # Table: UPPERCASE columns from the batch header; COPY loads positionally.
        header = files[0].read_text(encoding="utf-8").splitlines()[0].split(",")
        cols = ", ".join(f'"{c.strip().upper()}" VARCHAR' for c in header)
        table = f"{system}_{entity}".upper()
        cur.execute(f"CREATE OR REPLACE TABLE {db}.LANDING.{table} ({cols})")
        cur.execute(f"COPY INTO {db}.LANDING.{table} FROM @{db}.{stage}/{entity}/ "
                    f"FILE_FORMAT = {fmt} PATTERN = '.*[.]csv' ON_ERROR = ABORT_STATEMENT")
        cur.execute(f"SELECT COUNT(*) FROM {db}.LANDING.{table}")
        n = cur.fetchone()[0]
        loaded.append((table, n))
        log(f"    LANDING.{table} (@{stage}/{entity}/y_/m_/d_/h_): {n} rows")
    if loaded:
        cur.execute(f"ALTER STAGE {db}.{stage} REFRESH")   # so files show in Snowsight
    return loaded


def load_landing(conn, db, domain, system, gen_dir, log=print, via_stage=False):
    """Load each generated entity into LANDING.<system>_<entity> (all VARCHAR).

    via_stage=False: direct write_pandas (fast; concatenates the partitions).
    via_stage=True:  PUT the partitioned files to LANDING_<DOMAIN> + COPY INTO (the native
                     volume flow, preserving the y_/m_/d_/h_ tree like Databricks/Fabric).
    """
    if via_stage:
        return _load_partitioned_via_stage(conn, db, domain, system, gen_dir, log)
    loaded = []
    for entity_dir in sorted(p for p in Path(gen_dir).iterdir() if p.is_dir()):
        df = _read_entity_frame(entity_dir)
        if df is None:
            continue
        table = f"{system}_{entity_dir.name}".upper()
        _load_frame_direct(conn, db, table, df)
        log(f"    LANDING.{table}: {len(df)} rows")
        loaded.append((table, len(df)))
    return loaded


def load_system_landing(conn, db, domain, system, log=print, via_stage=False):
    """Generate + load landing for one (domain, system). No-op if no generator yet."""
    gen = GENERATORS.get((domain, system))
    if gen is None:
        log(f"    no generator yet for {domain}/{system} — skipping")
        return []
    with tempfile.TemporaryDirectory() as tmp:
        gen(tmp)
        return load_landing(conn, db, domain, system, tmp, log=log, via_stage=via_stage)
