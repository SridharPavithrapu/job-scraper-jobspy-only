import sys, os
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
import streamlit as st
import yaml
import smtplib, ssl
from email.message import EmailMessage
from datetime import datetime

from src.services.search_service import search_jobs
from src.utils.locations import expand_locations
from datetime import datetime

import os
try:
    from dotenv import load_dotenv
    load_dotenv()  # loads .env from project root
except Exception:
    pass

st.set_page_config(page_title="Job Scraper – JobSpy Only", layout="wide")
st.title("🔎 Multi-Board Job Scraper (JobSpy Only)")
st.caption("Indeed • Glassdoor • Google Jobs • ZipRecruiter • LinkedIn — filtered by title, location, and posting age (via JobSpy)")

CFG = dict(boards=["indeed","glassdoor","google","zip_recruiter","linkedin"], hours_old=24, results_wanted=100, country_indeed="USA")

default_titles = ["Data Analyst","Business Intelligence Analyst","Business Intelligence"]
default_locations = ["San Francisco, CA","Boston, MA","Seattle, WA"]

with st.sidebar:
    st.header("Search criteria")

    titles_text = st.text_area(
        "Job titles (one per line)",
        value="\n".join(default_titles),
        height=120,
    )

    st.subheader("Locations")

    # Preset lists
    US_STATES = [
        "Alabama, US", "Alaska, US", "Arizona, US", "Arkansas, US", "California, US",
        "Colorado, US", "Connecticut, US", "Delaware, US", "District of Columbia, US",
        "Florida, US", "Georgia, US", "Hawaii, US", "Idaho, US", "Illinois, US",
        "Indiana, US", "Iowa, US", "Kansas, US", "Kentucky, US", "Louisiana, US",
        "Maine, US", "Maryland, US", "Massachusetts, US", "Michigan, US", "Minnesota, US",
        "Mississippi, US", "Missouri, US", "Montana, US", "Nebraska, US", "Nevada, US",
        "New Hampshire, US", "New Jersey, US", "New Mexico, US", "New York, US",
        "North Carolina, US", "North Dakota, US", "Ohio, US", "Oklahoma, US", "Oregon, US",
        "Pennsylvania, US", "Rhode Island, US", "South Carolina, US", "South Dakota, US",
        "Tennessee, US", "Texas, US", "Utah, US", "Vermont, US", "Virginia, US",
        "Washington, US", "West Virginia, US", "Wisconsin, US", "Wyoming, US",
    ]

    CITY_PRESETS = [
        "San Francisco, CA", "San Jose, CA", "Oakland, CA", "Fremont, CA", "Sunnyvale, CA",
        "Palo Alto, CA", "Seattle, WA", "Bellevue, WA", "Boston, MA", "New York, NY",
        "Austin, TX", "Remote (USA)"
    ]

    # Toggle between states and cities
    preset_mode = st.radio("Preset list", ["US States", "US Cities"], horizontal=True)

    if preset_mode == "US States":
        preset_options = US_STATES
        default_presets = ["California, US", "New York, US"]
    else:
        preset_options = CITY_PRESETS
        default_presets = ["San Francisco, CA", "Boston, MA", "Seattle, WA"]

    chosen_presets = st.multiselect(
        "Choose preset locations",
        options=preset_options,
        default=default_presets,
    )

    locations_text = st.text_area(
        "Additional locations (one per line)",
        value="",
        height=80,
        help="Optional: Add locations not in the dropdown (e.g., cities, other states, countries).",
    )

    expand_aliases = st.checkbox(
        "Expand metro/region aliases (helps Glassdoor)",
        value=True,
    )

    st.subheader("Filters")

    strict_titles = st.checkbox(
        "Strict title match (post-filter)",
        value=True,
        help="Keep only rows whose TITLE matches one of your title phrases.",
    )

    hours_old = st.number_input(
        "Posted within last N hours",
        min_value=1,
        max_value=720,
        value=int(CFG.get("hours_old", 24)),
        step=1,
    )

    results_wanted = st.slider(
        "Max results per site",
        min_value=10,
        max_value=1000,
        value=int(CFG.get("results_wanted", 100)),
        step=10,
    )

    all_boards = ["indeed", "glassdoor", "google", "zip_recruiter", "linkedin"]
    boards_default = CFG.get("boards") or ["indeed", "google"]
    boards = st.multiselect(
        "Boards (JobSpy)",
        all_boards,
        default=[b for b in boards_default if b in all_boards],
        help="Pick at least one. LinkedIn uses JobSpy's LI mode (no spinlud).",
    )

    work_mode = st.selectbox(
        "Work mode",
        ["Any", "Remote only", "On-site only", "Hybrid only"],
        index=0,
    )

    employment_type = st.selectbox(
        "Employment type",
        ["Any", "Full-time", "Contract", "W2"],
        index=0,
        help="Post-filter by job type keywords in the title/description."
    )
    
    min_exp = st.number_input("Minimum experience (years, 0=ignore)", 0, 50, 0, 1)
    max_exp = st.number_input("Maximum experience (years, 0=ignore)", 0, 50, 0, 1)
    
    country_indeed = st.text_input("country_indeed (for Indeed/Glassdoor)", value=CFG.get("country_indeed","USA"))

    with st.expander("LinkedIn (JobSpy) — board-specific options"):
        if "linkedin" in boards:
            linkedin_fetch_description = st.checkbox("Fetch LinkedIn description (slower)", value=False)
            linkedin_easy_apply = st.checkbox("Prefer Easy Apply (LinkedIn only — disables hours filter for LI)", value=False)
        else:
            st.info("Select the **linkedin** board above to enable LinkedIn-specific options.")
            linkedin_fetch_description = False
            linkedin_easy_apply = False

    st.subheader("JobSpy network")
    jobspy_proxies_text = st.text_area("Proxies (one per line: host:port or user:pass@host:port)", value="", height=90)
    jobspy_user_agent = st.text_input("Custom User-Agent", value="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0 Safari/537.36")
    jobspy_verbose = st.selectbox("JobSpy verbosity", [0,1,2], index=2)

    st.subheader("Request pacing")
    sequential_mode = st.checkbox("Gentle mode (sequential per site, small delay)", value=True)
    per_site_delay = st.number_input("Delay between sites (seconds)", 0.0, 10.0, 2.0, 0.5)

    # NEW: Debug & Logging controls
    st.subheader("Debug / Logging")
    debug_enabled = st.checkbox("Save debug snapshots (query/raw/filtered)", value=False,
                                help="Writes files under ./debug_runs/<timestamp> or a custom folder below.")
    quiet_logs = st.checkbox("Quiet JobSpy logs (suppress INFO)", value=True)
    debug_dir = st.text_input("Debug folder (optional)", value="", help="Override path; leave blank for timestamped folder.")

    run_btn = st.button("🚀 Search jobs")

def parse_list(text):
    return [x.strip() for x in text.splitlines() if x.strip()]

def _unused_normalize_remote(sel: str):
    if sel == "Remote only":
        return True
    if sel == "On-site only":
        return False
    return None

def send_email_with_attachment(
    smtp_server: str,
    smtp_port: int | None,
    username: str,
    password: str,
    from_email: str,
    to_emails: list[str],
    subject: str,
    body: str,
    attachment_bytes: bytes,
    attachment_filename: str,
    try_both_ports: bool = False,   # NEW
):
    import smtplib, ssl
    from email.message import EmailMessage

    if not to_emails:
        raise ValueError("At least one recipient is required")

    from_email = (from_email or username or "").strip()
    if not from_email:
        raise ValueError("From email is required")

    # Build message
    msg = EmailMessage()
    msg["Subject"] = subject or "Job results"
    msg["From"] = from_email
    msg["To"] = ", ".join(to_emails)
    msg.set_content(body or "")
    msg.add_attachment(
        attachment_bytes or b"", maintype="text", subtype="csv",
        filename=attachment_filename or "jobs_scraped.csv"
    )

    context = ssl.create_default_context()

    def _send_tls(port: int):
        with smtplib.SMTP(smtp_server, port, timeout=30) as s:
            s.ehlo()
            s.starttls(context=context)
            s.ehlo()
            if username and password:
                s.login(username, password)
            s.send_message(msg, from_addr=from_email, to_addrs=to_emails)

    def _send_ssl(port: int):
        with smtplib.SMTP_SSL(smtp_server, port, context=context, timeout=30) as s:
            if username and password:
                s.login(username, password)
            s.send_message(msg, from_addr=from_email, to_addrs=to_emails)

    errors = []
    if try_both_ports:
        for port, mode in [(587, "tls"), (465, "ssl")]:
            try:
                (_send_tls if mode == "tls" else _send_ssl)(port)
                return
            except Exception as e:
                errors.append(f"{port}/{mode}: {e}")
        raise RuntimeError("Tried 587 and 465, both failed: " + " | ".join(map(str, errors)))
    else:
        # Honor the chosen port
        port = int(smtp_port or 587)
        if port == 465:
            _send_ssl(port)
        else:
            _send_tls(port)



# Ensure state keys exist (so the email panel can render before any search)
if "csv_bytes" not in st.session_state:
    st.session_state["csv_bytes"] = None
if "last_rows" not in st.session_state:
    st.session_state["last_rows"] = 0
if "email_status" not in st.session_state:
    st.session_state["email_status"] = "Not sent yet"

if run_btn:
    titles = parse_list(titles_text)
    raw_locations = chosen_presets + parse_list(locations_text)
    locations = expand_locations(raw_locations, enable=expand_aliases)

    if not titles or not locations or not boards:
        st.warning("Please provide at least one title, one location, and select at least one JobSpy board.")
        st.stop()

    boards_str = ", ".join(boards) if boards else "none"
    easy_apply_str = "ON" if linkedin_easy_apply else "OFF"
    st.write(
        f"**Boards (JobSpy):** {boards_str} • "
        f"**Hours:** {hours_old} • "
        f"**Work mode:** {work_mode} • "
        f"**Easy Apply (LI):** {easy_apply_str}"
    )

    proxies_list = [p.strip() for p in jobspy_proxies_text.splitlines() if p.strip()] or None

    with st.spinner("Scraping via JobSpy..."):
        try:
            df = search_jobs(
                titles=titles,
                locations=locations,
                boards=boards,
                hours_old=int(hours_old),
                results_wanted=int(results_wanted),
                country_indeed=country_indeed,
                work_mode=work_mode.lower().replace(" only",""),
                min_experience=(None if int(min_exp)==0 else int(min_exp)),
                max_experience=(None if int(max_exp)==0 else int(max_exp)),
                strict_titles=bool(strict_titles),
                employment_type=employment_type.lower(),
                linkedin_fetch_description=linkedin_fetch_description,
                jobspy_proxies=proxies_list,
                jobspy_user_agent=jobspy_user_agent.strip() or None,
                jobspy_verbose=int(jobspy_verbose),
                linkedin_easy_apply=bool(linkedin_easy_apply),
                sequential_mode=bool(sequential_mode),
                per_site_delay=float(per_site_delay),
                # NEW debug controls
                debug_enabled=bool(debug_enabled),
                debug_dir=(debug_dir.strip() or None),
                quiet_logs=bool(quiet_logs),
            )
        except Exception as e:
            st.error(f"Error: {e}")
            df = pd.DataFrame()

    if df is None or df.empty:
        st.warning("No results returned. Try widening the time window, changing titles, or reducing boards/locations.")
    else:
        st.success(f"Found {len(df):,} unique postings.")
        st.dataframe(df, use_container_width=True)

        # Create CSV once and store in session so the email panel can use it
        csv_bytes = df.to_csv(index=False).encode("utf-8")
        st.session_state["csv_bytes"] = csv_bytes
        st.session_state["last_rows"] = len(df)

        st.download_button(
            "💾 Download CSV",
            data=csv_bytes,
            file_name="jobs_scraped.csv",
            mime="text/csv",
        )

# 📧 Email results (SMTP)
with st.expander("📧 Email results"):
    # Status
    csv_ready = st.session_state.get("csv_bytes") is not None
    rows = st.session_state.get("last_rows", 0)
    st.markdown(
        f"**CSV:** {'Ready' if csv_ready else 'Not generated yet'}"
        f"{' (' + str(rows) + ' rows)' if rows else ''}"
    )
    st.markdown(f"**Email status:** {st.session_state.get('email_status', 'Not sent yet')}")

    st.caption("Tip: For Gmail, create an **App password** (2FA required) and use smtp.gmail.com:587.")
    default_server = os.getenv("SMTP_SERVER", "smtp.gmail.com")
    default_port   = int(os.getenv("SMTP_PORT", "587"))
    default_user   = os.getenv("SMTP_USER", "")
    default_from   = os.getenv("SMTP_FROM", default_user)
    default_pass   = os.getenv("SMTP_PASS", "")

    c1, c2 = st.columns(2)
    with c1:
        smtp_server = st.text_input("SMTP server", value=default_server)
        smtp_port   = st.number_input("SMTP port", min_value=1, max_value=65535, value=default_port, step=1)
        smtp_user   = st.text_input("SMTP username", value=default_user)
        from_email  = st.text_input("From email", value=default_from or smtp_user)

    with c2:
        to_field   = st.text_input("To (comma-separated)")
        smtp_pass  = st.text_input("SMTP password / app password", value=default_pass, type="password")
        subject    = st.text_input("Subject", value=f"Job scrape results — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
        body       = st.text_area("Message", value="Attached are the latest job results.", height=80)

    try_both_ports = st.checkbox("Try both ports automatically (587 → 465)", value=True,
                                 help="Attempts 587 (STARTTLS) first, then 465 (SSL) if it fails.")

    # derived
    disabled = not bool(csv_ready)
    csv_bytes = st.session_state.get("csv_bytes")

    def _send_safe(to_emails: list[str]):
        # Validate before trying to send
        missing = []
        if not smtp_server: missing.append("SMTP server")
        if not int(smtp_port): missing.append("SMTP port")
        if not (from_email or smtp_user): missing.append("From email / SMTP username")
        if not to_emails: missing.append("recipient")

        if missing:
            st.session_state["email_status"] = f"❌ Missing: {', '.join(missing)}"
            st.error(st.session_state["email_status"])
            return

        try:
            send_email_with_attachment(
                smtp_server=smtp_server,
                smtp_port=int(smtp_port),
                username=smtp_user,
                password=smtp_pass,
                from_email=(from_email or smtp_user),
                to_emails=to_emails,
                subject=subject,
                body=body,
                attachment_bytes=csv_bytes,
                attachment_filename="jobs_scraped.csv",
                try_both_ports=try_both_ports,
            )
            st.session_state["email_status"] = f"✅ Sent to: {', '.join(to_emails)} at {datetime.now().strftime('%H:%M:%S')}"
            st.success(st.session_state["email_status"])
        except Exception as e:
            st.session_state["email_status"] = f"❌ Error: {e}"
            st.error(st.session_state["email_status"])

    # --- Quick-send buttons (3 fixed recipients) ---
    colA, colB, colC = st.columns(3)

    with colA:
        if st.button("Send to Shazia (📧 shaziaiqra1@gmail.com)", disabled=disabled):
            _send_safe(["shaziaiqra1@gmail.com"])

    with colB:
        if st.button("Send to Y M (📧 ymudulodu@gmail.com)", disabled=disabled):
            _send_safe(["ymudulodu@gmail.com"])

    with colC:
        if st.button("Send to Ruth (📧 ruthvej111@gmail.com)", disabled=disabled):
            _send_safe(["ruthvej111@gmail.com"])

    # --- Custom recipients (full-width) ---
    if st.button("Send to custom recipients", disabled=disabled):
        tos = [t.strip() for t in to_field.split(",") if t.strip()]
        _send_safe(tos)
