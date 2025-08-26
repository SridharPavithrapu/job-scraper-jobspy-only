#!/usr/bin/env python3
"""
automation_runner.py
Run saved profiles (Shazia / Yoshitha / Ruthvej) headlessly and email results.

Examples:
  # Dry run (no email), default 24h window
  ./.venv/bin/python automation_runner.py --profile shazia --dry-run

  # Override hours
  ./.venv/bin/python automation_runner.py --profile shazia --dry-run --hours 48

  # Send email now
  ./.venv/bin/python automation_runner.py --profile shazia --send

  # Run all three and send (debug artifacts on)
  ./.venv/bin/python automation_runner.py --all --send --debug
"""
from __future__ import annotations
import os, sys, argparse, traceback
from datetime import datetime
from typing import List, Dict, Any, Optional
import pandas as pd

# --- load .env if present ---
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

# --- import your app code (no Streamlit import!) ---
from src.services.search_service import search_jobs

# Optional: use your filtering if available (keeps unknown experience)
try:
    from src.utils.filtering import filter_by_experience
except Exception:
    filter_by_experience = None  # not critical

# --- SMTP helper (robust sender, no Streamlit deps) ---
import smtplib, ssl
from email.message import EmailMessage

def send_email_with_attachment(
    smtp_server: str,
    smtp_port: int,
    username: str,
    password: str,
    from_email: str,
    to_emails: list[str],
    subject: str,
    body: str,
    attachment_bytes: bytes,
    attachment_filename: str,
    try_both_ports: bool = True,
) -> None:
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = from_email
    msg["To"] = ", ".join(to_emails)
    msg.set_content(body)

    if attachment_bytes:
        msg.add_attachment(
            attachment_bytes,
            maintype="text",
            subtype="csv",
            filename=attachment_filename,
        )

    def _send(port: int, use_ssl: bool) -> None:
        if use_ssl:
            ctx = ssl.create_default_context()
            with smtplib.SMTP_SSL(smtp_server, port, context=ctx, timeout=30) as server:
                if username:
                    server.login(username, password)
                server.send_message(msg)
        else:
            with smtplib.SMTP(smtp_server, port, timeout=30) as server:
                server.ehlo()
                server.starttls(context=ssl.create_default_context())
                server.ehlo()
                if username:
                    server.login(username, password)
                server.send_message(msg)

    try:
        if smtp_port == 465:
            _send(465, use_ssl=True)
        else:
            _send(587, use_ssl=False)
    except Exception as e1:
        if not try_both_ports:
            raise
        try:
            if smtp_port == 465:
                _send(587, use_ssl=False)
            else:
                _send(465, use_ssl=True)
        except Exception as e2:
            raise RuntimeError(f"Tried {smtp_port} and fallback, both failed: {e1} | {e2}")

# --- Automation defaults ---
COMMON = dict(
    hours_old=int(os.getenv("AUTOMATION_HOURS", "24")),  # default 24h; override via env or --hours
    strict_titles=True,
    linkedin_fetch_description=False,
    results_wanted=100,
    jobspy_proxies=None,          # can override via --proxies
    jobspy_user_agent=None,       # can override via --ua
    jobspy_verbose=2,
    sequential_mode=True,
    per_site_delay=2.5,
    debug_enabled=False,
    quiet_logs=True,
)

# --- Profiles (ZipRecruiter intentionally excluded) ---
PROFILES: Dict[str, Dict[str, Any]] = {
    "shazia": {
        "to": ["shaziaiqra1@gmail.com"],
        "titles": [
            "Data analyst",
            "Business analyst",
            "Business intelligence engineer",
            "Power BI developer",
            "Data governance analyst",
        ],
        "locations": ["New York", "New Jersey", "Connecticut"],
        "boards": ["indeed", "glassdoor", "google", "linkedin"],  # exclude zip_recruiter
        "work_mode": "any",
        "max_experience": 6,
    },
    "yoshitha": {
        "to": ["ymudulodu@gmail.com"],
        "titles": [
            "Data analyst",
            "Business analyst",
            "Business intelligence engineer",
            "Power BI developer",
            "Data governance analyst",
        ],
        "locations": ["California", "Texas", "Remote (USA)"],
        "boards": ["indeed", "glassdoor", "google", "linkedin"],
        "work_mode": "any",
        "max_experience": 6,
    },
    "ruthvej": {
        "to": ["ruthvej111@gmail.com"],
        "titles": [
            "Senior Supply Chain Analyst",
            "Supply Chain Manager",
            "Logistics Analyst",
            "Logistics Manager",
            "Transportation Manager",
            "Warehouse Operations Manager",
            "Distribution Center Manager",
            "Inventory Control Manager",
        ],
        "locations": ["All US states"],
        "boards": ["indeed", "glassdoor", "google", "linkedin"],
        "work_mode": "any",
        "max_experience": 8,
    },
}

# same state→metro logic as service (safe for other boards too)
_GD_STATE_FALLBACKS = {
    "New Jersey":   ["Newark, NJ", "Jersey City, NJ", "Edison, NJ"],
    "Connecticut":  ["Hartford, CT", "Stamford, CT", "New Haven, CT"],
    "New York":     ["New York, NY", "Buffalo, NY", "Albany, NY"],
    "California":   ["Los Angeles, CA", "San Francisco, CA", "San Jose, CA", "San Diego, CA", "Sacramento, CA"],
    "Texas":        ["Dallas, TX", "Houston, TX", "Austin, TX", "San Antonio, TX"],
}

def _flatten_locations(locs):
    out = []
    for l in locs:
        if str(l).strip().lower() == "all us states":
            out.extend([
                "Alabama","Alaska","Arizona","Arkansas","California","Colorado","Connecticut",
                "Delaware","District of Columbia","Florida","Georgia","Hawaii","Idaho","Illinois",
                "Indiana","Iowa","Kansas","Kentucky","Louisiana","Maine","Maryland","Massachusetts",
                "Michigan","Minnesota","Mississippi","Missouri","Montana","Nebraska","Nevada",
                "New Hampshire","New Jersey","New Mexico","New York","North Carolina","North Dakota",
                "Ohio","Oklahoma","Oregon","Pennsylvania","Rhode Island","South Carolina","South Dakota",
                "Tennessee","Texas","Utah","Vermont","Virginia","Washington","West Virginia",
                "Wisconsin","Wyoming"
            ])
        else:
            out.append(l)
    # de-dup
    seen=set(); ret=[]
    for x in out:
        if x not in seen:
            seen.add(x); ret.append(x)
    return ret

def _expand_for_glassdoor(locs):
    out = list(locs)
    for l in locs:
        exp = _GD_STATE_FALLBACKS.get(l, [])
        for x in exp:
            if x not in out:
                out.append(x)
    # de-dup keep order
    seen = set(); ret = []
    for x in out:
        if x not in seen:
            seen.add(x); ret.append(x)
    return ret

# --- Run a single profile ---
def run_profile(profile_key: str, send_email: bool=False, out_dir: str="automation_out", hours_old: int | None = None,
                proxies: list[str] | None = None, user_agent: str | None = None, debug_enabled: bool = False) -> str:
    prof = PROFILES[profile_key]
    
    # ✅ Allow GitHub Actions matrix to force a single board via env JOB_BOARDS
    #    e.g., JOB_BOARDS=indeed  (from the workflow’s matrix)
    env_boards = os.getenv("JOB_BOARDS")
    if env_boards:
        prof["boards"] = [b.strip() for b in env_boards.split(",") if b.strip()]
    
    titles = prof["titles"]
    # Expand "All US states", then append Glassdoor-friendly metros for state inputs
    locations = _expand_for_glassdoor(_flatten_locations(prof["locations"]))
    boards = [b for b in prof["boards"] if b != "zip_recruiter"]  # enforce skip
    work_mode = prof.get("work_mode","any")
    max_exp = prof.get("max_experience")

    os.makedirs(out_dir, exist_ok=True)
    now = datetime.now()
    stamp = now.strftime("%Y%m%d_%H%M")
    hours_val = (hours_old if hours_old is not None else COMMON["hours_old"])
    fname = f"jobs_{profile_key}_{'-'.join(boards)}_{stamp}_{work_mode.replace(' ','_')}_{hours_val}h.csv"
    path = os.path.join(out_dir, fname)

    # Run scrape
    df = search_jobs(
        titles=titles,
        locations=locations,
        boards=boards,
        hours_old=hours_val,
        work_mode=work_mode,
        min_experience=None,
        max_experience=max_exp,
        strict_titles=COMMON["strict_titles"],
        employment_type="any",
        results_wanted=COMMON["results_wanted"],
        country_indeed="USA",
        linkedin_fetch_description=False,
        linkedin_easy_apply=False,
        jobspy_proxies=(proxies or COMMON["jobspy_proxies"]),
        jobspy_user_agent=(user_agent or COMMON["jobspy_user_agent"]),
        jobspy_verbose=COMMON["jobspy_verbose"],
        sequential_mode=COMMON["sequential_mode"],
        per_site_delay=COMMON["per_site_delay"],
        # this is the supported debug hook in your service:
        debug_run_name=f"{profile_key}_{stamp}"
    )

    # Optional experience filter if available; keep unknown years
    if filter_by_experience is not None and max_exp is not None:
        df = filter_by_experience(df, min_years=None, max_years=max_exp, keep_unknown=True)

    # Save CSV
    df = df if df is not None else pd.DataFrame()
    df.to_csv(path, index=False)

    print(f"[info] profile={profile_key} rows={len(df)} file={path}")

    # Email?
    if send_email:
        smtp_server = os.getenv("SMTP_SERVER", "smtp-relay.brevo.com")
        smtp_port   = int(os.getenv("SMTP_PORT", "587"))
        smtp_user   = os.getenv("SMTP_USER", "")
        smtp_pass   = os.getenv("SMTP_PASS", "")
        from_email  = os.getenv("SMTP_FROM", smtp_user or "jobs@example.com")

        # attachment
        csv_bytes = df.to_csv(index=False).encode("utf-8")
        subject = f"Job scrape — {profile_key} — {stamp} — {hours_val}h — {len(df)} rows"
        body = (
            f"Daily scrape for {profile_key}\n\n"
            f"Boards: {', '.join(boards)}\n"
            f"Locations: {', '.join(locations)}\n"
            f"Titles: {', '.join(titles)}\n"
            f"Rows: {len(df)}\n"
        )
        send_email_with_attachment(
            smtp_server=smtp_server,
            smtp_port=smtp_port,
            username=smtp_user,
            password=smtp_pass,
            from_email=from_email,
            to_emails=prof["to"],
            subject=subject,
            body=body,
            attachment_bytes=csv_bytes,
            attachment_filename=fname,
            try_both_ports=True,
        )
    return path

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--profile", choices=list(PROFILES.keys()))
    ap.add_argument("--all", action="store_true", help="Run all profiles")
    ap.add_argument("--send", action="store_true", help="Actually send email")
    ap.add_argument("--dry-run", action="store_true", help="Skip email send")
    ap.add_argument("--hours", type=int, help="Override hours_old (default 24 via env AUTOMATION_HOURS)")
    ap.add_argument("--debug", action="store_true", help="Write debug CSV/JSON snapshots from the search service")
    ap.add_argument("--proxies", type=str, help="Comma-separated proxies (user:pass@host:port,host2:port2,...)")
    ap.add_argument("--ua", type=str, help="Custom User-Agent string")
    args = ap.parse_args()

    hours_cli = args.hours if hasattr(args, "hours") else None
    # Prefer CLI flags; otherwise fall back to env (used by GitHub Actions secrets)
    if args.proxies:
        proxies = [p.strip() for p in args.proxies.split(",") if p.strip()]
    elif os.getenv("JOBSPY_PROXIES"):
        proxies = [p.strip() for p in os.getenv("JOBSPY_PROXIES","").split(",") if p.strip()]
    else:
        proxies = None
    
    ua = args.ua or os.getenv("JOBSPY_USER_AGENT")

    if not args.profile and not args.all:
        print("Choose --profile {shazia|yoshitha|ruthvej} or --all", file=sys.stderr)
        sys.exit(2)

    try:
        if args.all:
            for key in PROFILES.keys():
                path = run_profile(
                    key,
                    send_email=bool(args.send and not args.dry_run),
                    hours_old=hours_cli,
                    proxies=proxies,
                    user_agent=ua,
                    debug_enabled=args.debug,
                )
                print(f"[ok] {key}: wrote {path}")
        else:
            path = run_profile(
                args.profile,
                send_email=bool(args.send and not args.dry_run),
                hours_old=hours_cli,
                proxies=proxies,
                user_agent=ua,
                debug_enabled=args.debug,
            )
            print(f"[ok] {args.profile}: wrote {path}")
    except Exception:
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
