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


def load_landing(conn, db, system, gen_dir, log=print):
    """Load each generated entity's files into LANDING.<system>_<entity> (all VARCHAR)."""
    import pandas as pd
    from snowflake.connector.pandas_tools import write_pandas
    loaded = []
    for entity_dir in sorted(p for p in Path(gen_dir).iterdir() if p.is_dir()):
        entity = entity_dir.name
        frames = []
        for f in glob.glob(str(entity_dir / "**" / "*.csv"), recursive=True):
            try: frames.append(pd.read_csv(f, dtype=str, keep_default_na=False))
            except Exception as e: log(f"    WARN read {f}: {e}")
        for f in glob.glob(str(entity_dir / "**" / "*.json"), recursive=True):
            try: frames.append(pd.read_json(f, dtype=False).astype(str))
            except Exception as e: log(f"    WARN read {f}: {e}")
        if not frames:
            continue
        df = pd.concat(frames, ignore_index=True)
        df.columns = [c.upper() for c in df.columns]   # Snowflake-native casing
        table = f"{system}_{entity}".upper()
        write_pandas(conn, df, table_name=table, schema="LANDING", database=db,
                     auto_create_table=True, overwrite=True, quote_identifiers=True)
        loaded.append((table, len(df)))
        log(f"    LANDING.{table}: {len(df)} rows")
    return loaded


def load_system_landing(conn, db, domain, system, log=print):
    """Generate + load landing for one (domain, system). No-op if no generator yet."""
    gen = GENERATORS.get((domain, system))
    if gen is None:
        log(f"    no generator yet for {domain}/{system} — skipping")
        return []
    with tempfile.TemporaryDirectory() as tmp:
        gen(tmp)
        return load_landing(conn, db, system, tmp, log=log)
