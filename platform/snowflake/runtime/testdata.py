"""RideFlow landing-zone test-data generators — Snowflake port.

Ported from the Databricks demo's ``test_data_driver.py`` per-(domain, system)
generators, adapted for a live ``snowflake.connector`` connection instead of Spark.

Each generator has the signature ``gen_<domain>_<system>(conn, db, landing_root)``:

  * ``conn``          — a live ``snowflake.connector`` connection (used only to read
                        real upstream FK id pools so cross-domain joins line up).
  * ``db``            — the Snowflake database name (e.g. ``'RIDEFLOW_DEV_DEMO'``).
  * ``landing_root``  — a local temp directory (str) into which Hive-partitioned CSV
                        seed files are written.

Cross-domain FK pools are read from ``<db>.LANDING.<SYSTEM>_<ENTITY>`` when those
upstream tables already exist; otherwise synthetic ids are generated so the
generators remain runnable in isolation.

Dependency-free (stdlib only). The marketplace/rideflow generator and its
trip-event derivations are intentionally NOT ported here — they live elsewhere.
"""

import csv
import json
import os
import random
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path


# ── helpers ──────────────────────────────────────────────────────────────────
def _partition_hour(now=None):
    """Hive hour-partition path fragment: y_YYYY/m_MM/d_DD/h_HH (current UTC)."""
    now = now or datetime.now(timezone.utc)
    return f"y_{now.strftime('%Y')}/m_{now.strftime('%m')}/d_{now.strftime('%d')}/h_{now.strftime('%H')}"


def _write_csv(landing_root, entity, rows):
    """Write ``rows`` (list of dicts) to
    ``{landing_root}/{entity}/y_/m_/d_/h_/<entity>_seed.csv`` with a lowercase
    header row and all values as strings."""
    out_dir = Path(landing_root) / entity / _partition_hour()
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / f"{entity}_seed.csv"
    with out.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)
    print(f"   {entity}: {len(rows)} rows -> {out}")


def _fk_ids(conn, db, system, entity, col, fallback_prefix, n=60):
    """Distinct FK values from ``<db>.LANDING.<SYSTEM>_<ENTITY>`` via Snowflake;
    synthetic fallback when the upstream table is missing/empty.

    Lets cross-domain generators (hubspot -> rider_id, zendesk -> charge_id, ...)
    key off REAL upstream ids when those LANDING tables already exist.
    """
    table = f"{db}.LANDING.{system.upper()}_{entity.upper()}"
    try:
        cur = conn.cursor()
        try:
            cur.execute(f'SELECT DISTINCT "{col.upper()}" FROM {table}')
            vals = [str(r[0]) for r in cur.fetchall() if r[0] is not None]
        finally:
            cur.close()
        if vals:
            return vals[:n]
    except Exception as e:  # table missing / not yet built
        print(f"   WARN could not read {table}.{col} ({e}); using synthetic IDs")
    return [f"{fallback_prefix}-{uuid.uuid4().hex[:10]}" for _ in range(n)]


# ── marketing wave ───────────────────────────────────────────────────────────
def gen_marketing_google_ads(conn, db, landing_root):
    random.seed(42)
    now = datetime.now(timezone.utc)
    campaign_pool = [("camp_" + uuid.uuid4().hex[:8], n) for n in ["RideFlow Brand UK", "Eats Delivery US", "Driver Recruit DE", "Surge Promo EU"]]
    rows = []
    for c_id, c_name in campaign_pool:
        for d_offset in range(14):
            date = (now - timedelta(days=d_offset)).date().isoformat()
            for ag in range(random.randint(2, 4)):
                rows.append({
                    "campaign_id": c_id, "campaign_name": c_name,
                    "ad_group_id": f"ag_{uuid.uuid4().hex[:8]}", "ad_group_name": f"{c_name} - AG{ag+1}",
                    "date": date, "impressions": str(random.randint(1000, 50000)), "clicks": str(random.randint(20, 2500)),
                    "cost": f"{random.uniform(50, 800):.2f}", "conversions": str(random.randint(0, 80)),
                    "conversion_value": f"{random.uniform(0, 1500):.2f}", "currency": random.choice(["GBP", "USD", "EUR"]),
                })
    _write_csv(landing_root, "campaigns", rows)


def gen_marketing_google_analytics(conn, db, landing_root):
    random.seed(43)
    now = datetime.now(timezone.utc)
    iso = lambda dt: dt.replace(microsecond=0, tzinfo=None).isoformat() + "Z"
    user_pool = [uuid.uuid4().hex for _ in range(80)]
    rider_ids = _fk_ids(conn, db, "rideflow", "rider_profiles", "rider_id", "R", 80)
    events = []
    for _ in range(400):
        ts = now - timedelta(minutes=random.randint(0, 60*24*7))
        events.append({
            "event_name": random.choice(["app_open", "trip_requested", "trip_completed", "payment_method_added", "signup"]),
            "event_timestamp": str(int(ts.timestamp() * 1_000_000)), "event_date": ts.date().isoformat(),
            "user_pseudo_id": random.choice(user_pool), "user_id": random.choice(rider_ids) if rider_ids else "",
            "device_id": uuid.uuid4().hex[:16], "ip_address": f"10.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,254)}",
            "device_category": random.choice(["mobile", "desktop", "tablet"]), "device_os": random.choice(["iOS", "Android", "Web"]),
            "geo_country": random.choice(["GB", "US", "DE", "FR", "JP"]), "geo_city": random.choice(["London", "NYC", "Berlin", "Paris", "Tokyo"]),
            "app_version": random.choice(["4.18.0", "4.18.1", "4.19.0"]), "event_params_json": '{"source":"seed"}',
        })
    _write_csv(landing_root, "app_events", events)
    sessions = []
    for _ in range(200):
        start = now - timedelta(minutes=random.randint(0, 60*24*7))
        sessions.append({
            "session_id": uuid.uuid4().hex, "user_pseudo_id": random.choice(user_pool), "user_id": random.choice(rider_ids) if rider_ids else "",
            "ip_address": f"10.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,254)}", "session_start": iso(start),
            "session_duration_seconds": str(random.randint(15, 1800)), "page_views": str(random.randint(1, 20)),
            "traffic_source": random.choice(["google", "facebook", "direct", "email"]), "traffic_medium": random.choice(["cpc", "organic", "referral", "email"]),
            "traffic_campaign": random.choice(["summer-2026", "london-brand", "", "driver-recruit"]),
            "device_category": random.choice(["mobile", "desktop", "tablet"]), "geo_country": random.choice(["GB", "US", "DE", "FR", "JP"]),
            "landing_page": random.choice(["/home", "/signup", "/drivers", "/eats", "/pricing"]),
        })
    _write_csv(landing_root, "sessions", sessions)


def gen_marketing_hubspot(conn, db, landing_root):
    random.seed(44)
    now = datetime.now(timezone.utc)
    iso = lambda dt: dt.replace(microsecond=0, tzinfo=None).isoformat() + "Z"
    rider_ids = _fk_ids(conn, db, "rideflow", "rider_profiles", "rider_id", "R", 80)
    campaigns_pool = [(uuid.uuid4().hex[:12], n) for n in ["Weekly Digest", "Driver Bonus", "Surge Alert", "Welcome Series"]]
    email_rows = []
    for _ in range(180):
        c_id, c_name = random.choice(campaigns_pool)
        sent = now - timedelta(hours=random.randint(0, 24*14))
        opened = sent + timedelta(hours=random.randint(1, 24)) if random.random() < 0.45 else None
        clicked = (opened or sent) + timedelta(minutes=random.randint(1, 90)) if random.random() < 0.25 else None
        email_rows.append({
            "campaign_id": c_id, "recipient_email": f"user{random.randint(1,9999)}@example.com",
            "recipient_name": random.choice(["Aisha", "Carlos", "Mei", "Femi", "Liam"]) + " " + random.choice(["Patel", "Smith", "Chen", "Okafor", "Brown"]),
            "campaign_name": c_name, "subject_line": random.choice(["Your weekly RideFlow update", "Earn 2x this weekend", "Surge alert near you", "Welcome aboard"]),
            "sent_at": iso(sent), "opened_at": iso(opened) if opened else "", "clicked_at": iso(clicked) if clicked else "",
            "bounced": "true" if random.random() < 0.03 else "false", "unsubscribed": "true" if random.random() < 0.02 else "false",
        })
    _write_csv(landing_root, "email_campaigns", email_rows)
    push_rows = []
    for _ in range(220):
        sent = now - timedelta(hours=random.randint(0, 24*7))
        delivered = sent + timedelta(seconds=random.randint(1, 30)) if random.random() < 0.95 else None
        opened = (delivered or sent) + timedelta(minutes=random.randint(1, 120)) if random.random() < 0.30 else None
        push_rows.append({
            "notification_id": uuid.uuid4().hex, "rider_id": random.choice(rider_ids), "device_token": uuid.uuid4().hex,
            "campaign_name": random.choice([n for _, n in campaigns_pool]), "sent_at": iso(sent),
            "delivered_at": iso(delivered) if delivered else "", "opened_at": iso(opened) if opened else "", "platform": random.choice(["ios", "android"]),
        })
    _write_csv(landing_root, "push_notifications", push_rows)


def gen_marketing_meta_ads(conn, db, landing_root):
    random.seed(45)
    now = datetime.now(timezone.utc)
    campaign_pool = [("meta_" + uuid.uuid4().hex[:8], n) for n in ["FB Reels Brand", "IG Stories Driver", "Audience Network Eats", "Lookalike Riders"]]
    rows = []
    for c_id, c_name in campaign_pool:
        for d_offset in range(14):
            date = (now - timedelta(days=d_offset)).date().isoformat()
            for adset in range(random.randint(2, 4)):
                impressions = random.randint(2000, 90000)
                clicks = int(impressions * random.uniform(0.005, 0.04))
                conversions = int(clicks * random.uniform(0.01, 0.08))
                rows.append({
                    "campaign_id": c_id, "campaign_name": c_name, "adset_id": f"as_{uuid.uuid4().hex[:8]}", "adset_name": f"{c_name} - AS{adset+1}",
                    "date": date, "impressions": str(impressions), "clicks": str(clicks), "spend": f"{random.uniform(80, 1200):.2f}",
                    "actions_json": f"link_click|{clicks}",  # pipe, not JSON: CSV reader has no escape opt
                    "conversions": str(conversions), "currency": random.choice(["GBP", "USD", "EUR"]),
                })
    _write_csv(landing_root, "campaigns", rows)


# ── payments wave ────────────────────────────────────────────────────────────
def gen_payments_stripe(conn, db, landing_root):
    random.seed(42)
    landing_p = Path(landing_root)
    rider_ids  = _fk_ids(conn, db, "rideflow", "rider_profiles",  "rider_id",  "R", 80)
    driver_ids = _fk_ids(conn, db, "rideflow", "driver_profiles", "driver_id", "D", 60)
    trip_ids   = _fk_ids(conn, db, "rideflow", "trip_completed",  "trip_id",   "T", 200)
    print(f"   marketplace pool: {len(rider_ids)} riders / {len(driver_ids)} drivers / {len(trip_ids)} trips")
    now = datetime.now(timezone.utc)
    part = f"y_{now.strftime('%Y')}/m_{now.strftime('%m')}/d_{now.strftime('%d')}"
    iso = lambda dt: dt.replace(microsecond=0, tzinfo=None).isoformat() + "Z"

    def _write(entity, rows):
        out_dir = landing_p / entity / part
        out_dir.mkdir(parents=True, exist_ok=True)
        out = out_dir / f"{entity}_seed.csv"
        with out.open("w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            w.writeheader()
            w.writerows(rows)
        print(f"   > {entity}: {len(rows)} rows -> {out}")

    CCYS = ["GBP", "USD", "EUR"]
    PM = ["card", "apple_pay", "google_pay"]
    charges = [{
        "charge_id": f"ch_{uuid.uuid4().hex[:24]}", "rider_id": random.choice(rider_ids), "trip_id": random.choice(trip_ids) if trip_ids else "",
        "amount": str(random.randint(500, 8500)), "currency": random.choice(CCYS),
        "status": random.choices(["succeeded", "failed", "pending"], weights=[92, 5, 3])[0],
        "payment_method_type": random.choice(PM), "card_last_four": f"{random.randint(0,9999):04d}",
        "billing_email": f"user{random.randint(1,9999)}@example.com", "billing_address": f"{random.randint(1,200)} Demo St, London",
        "created_at": iso(now - timedelta(minutes=random.randint(0, 60*24*14))), "metadata_json": '{"source":"seed"}',
    } for _ in range(min(800, max(200, len(trip_ids))))]
    _write("charges", charges)
    payouts = [{
        "payout_id": f"po_{uuid.uuid4().hex[:24]}", "driver_id": random.choice(driver_ids),
        "amount": str(random.randint(2000, 35000)), "currency": random.choice(CCYS),
        "status": random.choices(["paid", "pending", "failed"], weights=[88, 9, 3])[0],
        "payout_email": f"driver{random.randint(1,9999)}@example.com", "bank_account_last_four": f"{random.randint(0,9999):04d}",
        "arrival_date": (now - timedelta(days=random.randint(0,14)) + timedelta(days=2)).date().isoformat(),
        "created_at": iso(now - timedelta(days=random.randint(0,14))),
    } for _ in range(min(300, max(50, len(driver_ids)*3)))]
    _write("payouts", payouts)
    refunds = []
    for ch in random.sample(charges, k=max(20, len(charges)//16)):
        ts = datetime.fromisoformat(ch["created_at"].rstrip("Z")) + timedelta(hours=random.randint(1, 72))
        refunds.append({
            "refund_id": f"re_{uuid.uuid4().hex[:24]}", "charge_id": ch["charge_id"], "rider_id": ch["rider_id"],
            "amount": str(int(int(ch["amount"]) * random.uniform(0.3, 1.0))), "currency": ch["currency"],
            "status": random.choices(["succeeded", "pending", "failed"], weights=[90, 7, 3])[0],
            "reason": random.choice(["requested_by_customer", "duplicate", "fraudulent"]),
            "refund_reason": "Customer reported double-charge", "created_at": iso(ts),
        })
    _write("refunds", refunds)


# ── operations wave ──────────────────────────────────────────────────────────
def gen_operations_twilio(conn, db, landing_root):
    random.seed(46)
    now = datetime.now(timezone.utc)
    iso = lambda dt: dt.replace(microsecond=0, tzinfo=None).isoformat() + "Z"
    out_dir = Path(landing_root) / "sms_logs" / _partition_hour(now)
    out_dir.mkdir(parents=True, exist_ok=True)
    twilio_number = "+15558675309"
    bodies = {
        "outbound": ["RideFlow: Your driver is 3 mins away.", "RideFlow: Driver has arrived at pickup.", "RideFlow Eats: Order on its way!", "Driver dispatch: New trip available, surge 1.4x.", "Verification code: 482910"],
        "inbound": ["Driver was late but courteous", "I left my phone in the car, can you help?", "STOP", "Why was I charged twice?", "Great trip, 5 stars"],
    }
    rows = []
    for _ in range(300):
        direction = random.choices(["outbound", "inbound"], weights=[70, 30])[0]
        sent = now - timedelta(minutes=random.randint(0, 60*24*7))
        delivered = sent + timedelta(seconds=random.randint(1, 30)) if random.random() < 0.95 else None
        user_phone = f"+1{random.randint(2000000000, 9999999999)}"
        rows.append({
            "message_sid": "SM" + uuid.uuid4().hex[:30],
            "recipient_phone": user_phone if direction == "outbound" else twilio_number,
            "sender_phone": twilio_number if direction == "outbound" else user_phone,
            "message_body": random.choice(bodies[direction]),
            "status": random.choices(["delivered", "sent", "failed", "undelivered"], weights=[88, 7, 3, 2])[0],
            "direction": direction, "num_segments": str(random.choice([1, 1, 1, 2])),
            "price": f"-{random.uniform(0.005, 0.025):.4f}", "price_unit": "USD",
            "sent_at": iso(sent), "delivered_at": iso(delivered) if delivered else "", "error_code": "30003" if random.random() < 0.03 else "",
        })
    out = out_dir / "sms_logs_seed.csv"
    with out.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)
    print(f"   sms_logs: {len(rows)} rows -> {out}")


def gen_operations_zendesk(conn, db, landing_root):
    random.seed(47)
    now = datetime.now(timezone.utc)
    iso = lambda dt: dt.replace(microsecond=0, tzinfo=None).isoformat() + "Z"
    out_dir = Path(landing_root) / "support_tickets" / _partition_hour(now)
    out_dir.mkdir(parents=True, exist_ok=True)
    rider_ids  = _fk_ids(conn, db, "rideflow", "rider_profiles",  "rider_id",  "R", 80)
    driver_ids = _fk_ids(conn, db, "rideflow", "driver_profiles", "driver_id", "D", 60)
    trip_ids   = _fk_ids(conn, db, "rideflow", "trip_completed",  "trip_id",   "T", 200)
    charge_ids = _fk_ids(conn, db, "stripe",   "charges",         "charge_id", "ch", 200)
    subjects = {
        "trip": ["Trip never arrived", "Driver took a longer route", "Lost item in vehicle", "Driver cancelled at last minute"],
        "payment": ["Double charge on my card", "Surge pricing seems wrong", "Refund request", "Promo code didn't apply"],
        "safety": ["Safety concern with driver", "Inappropriate behaviour", "Vehicle was unsafe"],
        "account": ["Can't log in to app", "Phone number change", "Update payment method"],
        "general": ["Other feedback", "App crashing on launch", "Where is my receipt?"],
    }
    rows = []
    for _ in range(220):
        topic = random.choices(list(subjects), weights=[40, 30, 5, 15, 10])[0]
        created = now - timedelta(hours=random.randint(0, 24*14))
        status = random.choices(["new", "open", "pending", "solved", "closed"], weights=[5, 12, 8, 55, 20])[0]
        priority = random.choices(["low", "normal", "high", "urgent"], weights=[30, 50, 18, 2])[0]
        solved_at = created + timedelta(hours=random.randint(1, 96)) if status in ("solved", "closed") else None
        rows.append({
            "ticket_id": "zd_" + uuid.uuid4().hex[:24], "entry_point": random.choice(["in_app_help", "email", "web_form", "twitter"]),
            "rider_id": random.choice(rider_ids) if rider_ids else "",
            "trip_id": random.choice(trip_ids) if trip_ids and topic in ("trip", "safety") else "",
            "driver_id": random.choice(driver_ids) if driver_ids and topic in ("trip", "safety") else "",
            "charge_id": random.choice(charge_ids) if charge_ids and topic == "payment" else "",
            "requester_email": f"user{random.randint(1, 9999)}@example.com",
            "requester_name": random.choice(["Aisha", "Carlos", "Mei", "Femi", "Liam"]) + " " + random.choice(["Patel", "Smith", "Chen", "Okafor", "Brown"]),
            "requester_phone": f"+1{random.randint(2000000000, 9999999999)}", "subject": random.choice(subjects[topic]),
            "ticket_description": random.choice(subjects[topic]) + " — additional context added by rider.",
            "status": status, "priority": priority, "channel": random.choice(["chat", "email", "phone"]),
            "assignee": random.choice(["support_t1_01", "support_t1_02", "support_t2_01", ""]),
            "tags_json": f"{topic}|{priority}", "created_at": iso(created),  # pipe, not JSON (CSV-safe)
            "updated_at": iso(created + timedelta(minutes=random.randint(5, 60))), "solved_at": iso(solved_at) if solved_at else "",
            "satisfaction_rating": random.choice(["good", "bad", ""]) if status in ("solved", "closed") else "",
        })
    out = out_dir / "support_tickets_seed.csv"
    with out.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)
    print(f"   support_tickets: {len(rows)} rows -> {out}")


def gen_operations_checkr(conn, db, landing_root):
    random.seed(48)
    now = datetime.now(timezone.utc)
    iso = lambda dt: dt.replace(microsecond=0, tzinfo=None).isoformat() + "Z"
    driver_ids = _fk_ids(conn, db, "rideflow", "driver_profiles", "driver_id", "D", 60)
    first_names = ["Aisha", "Carlos", "Dmitri", "Elena", "Femi", "Hiroshi", "Isla", "Jamal", "Kira", "Liam", "Mei", "Noor"]
    last_names  = ["Patel", "Smith", "Nakamura", "Okafor", "Rodriguez", "Chen", "Kowalski", "Garcia", "Singh", "Brown", "Adeyemi", "Volkov"]
    # background_checks CSV (FK -> driver_id)
    part = f"y_{now.strftime('%Y')}/m_{now.strftime('%m')}/d_{now.strftime('%d')}"
    bc_dir = Path(landing_root) / "background_checks" / part
    bc_dir.mkdir(parents=True, exist_ok=True)
    bc_rows = []
    for driver_id in driver_ids:
        completed = now - timedelta(days=random.randint(0, 365))
        result = random.choices(["clear", "consider", "suspended"], weights=[88, 10, 2])[0]
        adjud = {"clear": "eligible", "consider": "manual_review", "suspended": "ineligible"}[result]
        bc_rows.append({
            "report_id": "chk_" + uuid.uuid4().hex[:24], "driver_id": driver_id,
            "full_name": f"{random.choice(first_names)} {random.choice(last_names)}",
            "date_of_birth": f"{random.randint(1960,2002)}-{random.randint(1,12):02d}-{random.randint(1,28):02d}",
            "ssn_last_four": f"{random.randint(0,9999):04d}", "licence_number": f"DL-{uuid.uuid4().hex[:8].upper()}",
            "address": f"{random.randint(1,9999)} {random.choice(['Main','Oak','High','Park'])} St, London",
            "status": "complete", "result": result, "completed_at": iso(completed),
            "created_at": iso(completed - timedelta(days=random.randint(1, 7))), "adjudication": adjud,
        })
    if bc_rows:
        out = bc_dir / "background_checks_seed.csv"
        with out.open("w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(bc_rows[0].keys()))
            w.writeheader()
            w.writerows(bc_rows)
        print(f"   background_checks: {len(bc_rows)} rows -> {out}")
    # driver_licence PDFs (parsed by the pdfplumber extraction contract). reportlab is
    # an optional dependency — mirror the source and skip gracefully when unavailable.
    try:
        from reportlab.pdfgen import canvas
        from reportlab.lib.pagesizes import letter
    except ImportError:
        print("   WARN reportlab not installed; skipping licence PDFs")
        return
    pdf_dir = Path(landing_root) / "driver_licences" / part
    pdf_dir.mkdir(parents=True, exist_ok=True)
    classes_pool = ["B", "C1", "C+E", "D1", "PCO"]
    for idx in range(30):
        licence_no = f"{random.choice(['CHK','UK','DL'])}-{idx:04d}{random.randint(1000,9999):04d}"
        licence_text = "MISSING" if random.random() < 0.05 else licence_no
        c = canvas.Canvas(str(pdf_dir / f"licence_{licence_no}.pdf"), pagesize=letter)
        c.setFont("Helvetica-Bold", 16)
        c.drawString(72, 720, "UK DRIVER LICENCE (Synthetic - Test Data)")
        c.setFont("Helvetica", 12)
        c.drawString(72, 680, f"Name: {random.choice(first_names)} {random.choice(last_names)}")
        c.drawString(72, 660, f"Licence No: {licence_text}")
        c.drawString(72, 640, f"DOB: {random.randint(1960,2002)}-{random.randint(1,12):02d}-{random.randint(1,28):02d}")
        c.drawString(72, 620, f"EXP: {random.randint(2024,2032)}-{random.randint(1,12):02d}-{random.randint(1,28):02d}")
        c.drawString(72, 600, f"Classes: {', '.join(random.sample(classes_pool, k=random.randint(1,3)))}")
        c.save()
    print(f"   30 synthetic licence PDFs -> {pdf_dir}")


# ── reference wave — static lookup/seed CSVs ─────────────────────────────────
def gen_reference_internal(conn, db, landing_root):
    """Synthetic reference-data seed CSVs (cities, fx rates, gold-from-landing seed
    dimensions). One dir per entity so the bronze/gold contracts that read
    {landing_root}/<entity> pick them up."""
    import calendar
    root = Path(landing_root)

    def _w(entity, rows):
        out_dir = root / entity
        out_dir.mkdir(parents=True, exist_ok=True)
        out = out_dir / f"{entity}_seed.csv"
        with out.open("w", encoding="utf-8", newline="") as f:
            wr = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            wr.writeheader()
            wr.writerows(rows)
        print(f"   {entity}: {len(rows)} rows -> {out}")

    cities = [("LON","London","GB","Europe/London","GBP","2018-04-01"),("NYC","New York","US","America/New_York","USD","2017-06-15"),
              ("BER","Berlin","DE","Europe/Berlin","EUR","2019-02-01"),("PAR","Paris","FR","Europe/Paris","EUR","2019-05-20"),
              ("TYO","Tokyo","JP","Asia/Tokyo","JPY","2020-01-10"),("SYD","Sydney","AU","Australia/Sydney","AUD","2018-09-12"),
              ("DXB","Dubai","AE","Asia/Dubai","AED","2021-03-01"),("SAO","Sao Paulo","BR","America/Sao_Paulo","BRL","2019-11-04")]
    _w("city_master", [{"city_code":c,"city_name":n,"country_code":cc,"timezone":tz,"default_currency":ccy,
        "geofence_wkt":"POLYGON((-0.5 51.3,-0.5 51.7,0.3 51.7,0.3 51.3,-0.5 51.3))","launched_at":la,"sunset_at":""}
        for c,n,cc,tz,ccy,la in cities])

    now = datetime.now(timezone.utc)
    fx = []
    for d in range(30):
        rd = (now-timedelta(days=d)).date().isoformat()
        for q, mid in {"USD":1.27,"EUR":1.17,"JPY":191.0,"AUD":1.93}.items():
            drift = ((d*7)%11-5)*0.001
            fx.append({"rate_date":rd,"base_currency":"GBP","quote_currency":q,"rate":round(mid+mid*drift,4),"source_feed":"ecb_synthetic"})
    _w("fx_rates_daily", fx)

    _w("dim_country_seed", [{"country_alpha3":a3,"country_alpha2":a2,"country_name":n,"country_numeric":num,"region":rg,"sub_region":sr}
        for a3,a2,n,num,rg,sr in [("GBR","GB","United Kingdom",826,"Europe","Northern Europe"),("USA","US","United States",840,"Americas","Northern America"),
        ("DEU","DE","Germany",276,"Europe","Western Europe"),("FRA","FR","France",250,"Europe","Western Europe"),("JPN","JP","Japan",392,"Asia","Eastern Asia"),
        ("AUS","AU","Australia",36,"Oceania","Australia and New Zealand"),("ARE","AE","United Arab Emirates",784,"Asia","Western Asia"),
        ("BRA","BR","Brazil",76,"Americas","South America"),("IND","IN","India",356,"Asia","Southern Asia"),("CAN","CA","Canada",124,"Americas","Northern America")]])
    _w("dim_currency_seed", [{"currency_code":c,"currency_numeric":num,"currency_name":n,"symbol":s,"minor_units":mu}
        for c,num,n,s,mu in [("GBP",826,"Pound Sterling","GBP",2),("USD",840,"US Dollar","USD",2),("EUR",978,"Euro","EUR",2),("JPY",392,"Yen","JPY",0),
        ("AUD",36,"Australian Dollar","AUD",2),("AED",784,"UAE Dirham","AED",2),("BRL",986,"Brazilian Real","BRL",2),("INR",356,"Indian Rupee","INR",2),("CAD",124,"Canadian Dollar","CAD",2)]])
    _w("dim_payment_method_seed", [{"payment_method_code":c,"payment_method_name":n,"rail":r,"requires_strong_auth":sca,"settles_instantly":si}
        for c,n,r,sca,si in [("card","Card (Visa/MC/Amex)","card_network","true","false"),("apple_pay","Apple Pay","card_network","true","false"),
        ("google_pay","Google Pay","card_network","true","false"),("cash","Cash","cash","false","true"),("sepa","SEPA Direct Debit","bank","false","false"),("pix","Pix (BR)","instant_bank","true","true")]])
    _w("dim_surge_tier_seed", [{"surge_tier_code":c,"multiplier_min":mn,"multiplier_max":mx,"display_label":l,"notify_rider":nt}
        for c,mn,mx,l,nt in [("NONE",1.0,1.0,"No surge","false"),("LOW",1.1,1.5,"Mild surge","false"),("MED",1.5,2.5,"High demand","true"),("HIGH",2.5,4.0,"Surge pricing","true"),("PEAK",4.0,6.0,"Peak surge","true")]])
    _w("dim_vehicle_type_seed", [{"vehicle_type_code":c,"vehicle_type_name":n,"passenger_capacity":cap,"product_tier":t,"requires_inspection":ins}
        for c,n,cap,t,ins in [("standard","RideFlow Standard",4,"economy","false"),("comfort","RideFlow Comfort",4,"select","true"),("xl","RideFlow XL",6,"premium","true"),("black","RideFlow Black",4,"premium","true"),("eats","Eats Delivery",1,"delivery","false")]])
    _w("dim_cancellation_reason_seed", [{"reason_code":c,"reason_label":l,"actor":a,"charge_cancellation_fee":fee,"counts_against_driver_rating":pen,"severity":sv}
        for c,l,a,fee,pen,sv in [("rider_changed_plans","Rider changed plans","rider","true","false","low"),("rider_no_show","Rider no-show","rider","true","false","medium"),
        ("driver_no_show","Driver no-show","driver","false","true","high"),("driver_long_eta","Driver took too long","driver","false","true","medium"),
        ("system_unable_to_match","System unable to match","system","false","false","low"),("safety_concern","Safety concern raised","rider","false","false","high")]])

    date_rows = []
    year = now.year
    for od in range(1, 366):
        dd = datetime(year, 1, 1, tzinfo=timezone.utc) + timedelta(days=od-1)
        if dd.year != year:
            break
        date_rows.append({"date_key":int(dd.strftime("%Y%m%d")),"full_date":dd.date().isoformat(),"year":dd.year,"quarter":(dd.month-1)//3+1,
            "month":dd.month,"month_name":calendar.month_name[dd.month],"day":dd.day,"day_of_week":dd.isoweekday(),"day_name":calendar.day_name[dd.weekday()],
            "is_weekend":"true" if dd.weekday()>=5 else "false","iso_week":dd.isocalendar()[1],"is_holiday_us":"false","is_holiday_uk":"false"})
    _w("dim_date_seed", date_rows)


GENERATORS = {
    ("marketing", "google_ads"): gen_marketing_google_ads,
    ("marketing", "google_analytics"): gen_marketing_google_analytics,
    ("marketing", "hubspot"): gen_marketing_hubspot,
    ("marketing", "meta_ads"): gen_marketing_meta_ads,
    ("payments", "stripe"): gen_payments_stripe,
    ("operations", "twilio"): gen_operations_twilio,
    ("operations", "zendesk"): gen_operations_zendesk,
    ("operations", "checkr"): gen_operations_checkr,
    ("reference", "internal"): gen_reference_internal,
}
