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


LANDING_STAGE = "LANDING.LANDING"   # internal stage (a "volume") with a directory table


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


def _load_frame_via_stage(conn, db, table, df, log):
    """Native 'volume' path: write CSV → PUT to the internal stage (a directory-table
    stage) → COPY INTO an all-VARCHAR LANDING.<table>. Mirrors real ingestion (files
    land, then load) and leaves the files browsable in Snowsight + a COPY_HISTORY."""
    import os, tempfile
    cur = conn.cursor()
    cur.execute(f"CREATE STAGE IF NOT EXISTS {db}.{LANDING_STAGE} DIRECTORY = (ENABLE = TRUE)")
    tmpdir = tempfile.mkdtemp()
    path = os.path.join(tmpdir, f"{table}.csv")
    df.to_csv(path, index=False)                       # header + same column order as the table
    uri = "file://" + path.replace("\\", "/")
    cur.execute(f"PUT '{uri}' @{db}.{LANDING_STAGE}/{table}/ AUTO_COMPRESS = TRUE OVERWRITE = TRUE")
    cols = ", ".join(f'"{c}" VARCHAR' for c in df.columns)
    cur.execute(f"CREATE OR REPLACE TABLE {db}.LANDING.{table} ({cols})")
    fmt = "(TYPE=CSV SKIP_HEADER=1 FIELD_OPTIONALLY_ENCLOSED_BY='\"' EMPTY_FIELD_AS_NULL=FALSE)"
    cur.execute(f"COPY INTO {db}.LANDING.{table} FROM @{db}.{LANDING_STAGE}/{table}/ "
                f"FILE_FORMAT = {fmt} ON_ERROR = ABORT_STATEMENT")
    log(f"    LANDING.{table} (@{LANDING_STAGE}): {len(df)} rows")


def load_landing(conn, db, system, gen_dir, log=print, via_stage=False):
    """Load each generated entity into LANDING.<system>_<entity> (all VARCHAR).

    via_stage=False: direct write_pandas (fast).
    via_stage=True:  PUT to the LANDING stage + COPY INTO (the native volume flow).
    """
    loaded = []
    for entity_dir in sorted(p for p in Path(gen_dir).iterdir() if p.is_dir()):
        df = _read_entity_frame(entity_dir)
        if df is None:
            continue
        table = f"{system}_{entity_dir.name}".upper()
        if via_stage:
            _load_frame_via_stage(conn, db, table, df, log)
        else:
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
        return load_landing(conn, db, system, tmp, log=log, via_stage=via_stage)
