#!/usr/bin/env python3
"""
Patch the JobSpy-only Streamlit app with:
- Email results panel (quick send to shaziaiqra1@gmail.com & ymudulodu@gmail.com + custom)
- Location dropdown presets + freeform locations textarea
- LinkedIn-only options grouped in a single expander
- Safer summary line (no f-string nesting issues)
- JobSpy Pydantic fix: omit is_remote when None
- Work mode filter (Any/Remote/On-site/Hybrid) + min/max experience filters

Run from project root:
    python3 patch_jobspy_app.py
"""

from pathlib import Path
import re
import textwrap
import sys

ROOT = Path(__file__).resolve().parent

def read(p: Path) -> str:
    return p.read_text(encoding="utf-8")

def write(p: Path, s: str):
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(s, encoding="utf-8")

def backup(p: Path):
    b = p.with_suffix(p.suffix + ".bak")
    if not b.exists():
        b.write_text(p.read_text(encoding="utf-8"), encoding="utf-8")

def patch_app(app_path: Path):
    txt = read(app_path)
    backup(app_path)

    # ---------- Imports for email ----------
    if "from email.message import EmailMessage" not in txt:
        txt = txt.replace(
            "import yaml",
            "import yaml\nimport smtplib, ssl\nfrom email.message import EmailMessage\nfrom datetime import datetime"
        )

    # ---------- Email helper ----------
    if "def send_email_with_attachment" not in txt:
        helper = textwrap.dedent("""
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
        ):
            msg = EmailMessage()
            msg["Subject"] = subject
            msg["From"] = from_email
            msg["To"] = ", ".join(to_emails)
            msg.set_content(body)

            msg.add_attachment(
                attachment_bytes,
                maintype="text",
                subtype="csv",
                filename=attachment_filename,
            )

            context = ssl.create_default_context()
            import smtplib
            with smtplib.SMTP(smtp_server, int(smtp_port)) as server:
                server.ehlo()
                try:
                    server.starttls(context=context)
                    server.ehlo()
                except Exception:
                    pass
                if username and password:
                    server.login(username, password)
                server.send_message(msg)
        """).strip("\n") + "\n"
        insert_idx = txt.find("\nif run_btn:")
        if insert_idx == -1:
            insert_idx = len(txt)
        txt = txt[:insert_idx] + "\n" + helper + "\n" + txt[insert_idx:]

    # ---------- Locations UI -> presets + extra ----------
    loc_pat = re.compile(
        r'locations_text\s*=\s*st\.text_area\("Locations \(one per line\)".+?\)\s*?\n\s*expand_aliases\s*=\s*st\.checkbox\("Expand metro/region aliases \(helps Glassdoor\)".*?\)',
        re.S
    )
    loc_repl = textwrap.dedent("""
        st.subheader("Locations")
        preset_locations = [
            "San Francisco, CA","San Jose, CA","Oakland, CA","Fremont, CA","Sunnyvale, CA","Palo Alto, CA",
            "Seattle, WA","Bellevue, WA","Boston, MA","New York, NY","Austin, TX","Remote (USA)"
        ]
        chosen_presets = st.multiselect(
            "Choose preset locations",
            options=preset_locations,
            default=["San Francisco, CA","Boston, MA","Seattle, WA"]
        )
        locations_text = st.text_area(
            "Additional locations (one per line)",
            value="",
            height=80,
            help="Optional: Add locations not in the dropdown."
        )
        expand_aliases = st.checkbox("Expand metro/region aliases (helps Glassdoor)", value=True)
    """).strip("\n")
    if loc_pat.search(txt):
        txt = loc_pat.sub(loc_repl, txt)

    txt = txt.replace(
        'raw_locations = parse_list(locations_text)\n    locations = expand_locations(raw_locations, enable=expand_aliases)',
        'raw_locations = chosen_presets + parse_list(locations_text)\n    locations = expand_locations(raw_locations, enable=expand_aliases)'
    )

    # ---------- LinkedIn options grouped ----------
    # Remove scattered checkboxes if present
    txt = txt.replace('linkedin_fetch_description = st.checkbox("Fetch LinkedIn description (slower)", value=False)', '')
    txt = txt.replace('linkedin_easy_apply = st.checkbox("Prefer Easy Apply (LinkedIn only — disables hours filter for LI)", value=False)', '')

    # Insert expander after country_indeed input
    if "LinkedIn (JobSpy) — board-specific options" not in txt:
        txt = txt.replace(
            'country_indeed = st.text_input("country_indeed (for Indeed/Glassdoor)", value=CFG.get("country_indeed","USA"))',
            'country_indeed = st.text_input("country_indeed (for Indeed/Glassdoor)", value=CFG.get("country_indeed","USA"))\n\n'
            '    with st.expander("LinkedIn (JobSpy) — board-specific options"):\n'
            '        if "linkedin" in boards:\n'
            '            linkedin_fetch_description = st.checkbox("Fetch LinkedIn description (slower)", value=False)\n'
            '            linkedin_easy_apply = st.checkbox("Prefer Easy Apply (LinkedIn only — disables hours filter for LI)", value=False)\n'
            '        else:\n'
            '            st.info("Select the **linkedin** board above to enable LinkedIn-specific options.")\n'
            '            linkedin_fetch_description = False\n'
            '            linkedin_easy_apply = False'
        )

    # ---------- Work mode + experience UI (if not present) ----------
    if "Work mode" not in txt:
        # Replace any old "Remote filter" select with Work mode
        txt = txt.replace(
            'is_remote_sel = st.selectbox("Remote filter", ["Auto (don\'t force)","Remote only","On-site only"], index=0)',
            'work_mode = st.selectbox("Work mode", ["Any","Remote only","On-site only","Hybrid only"], index=0)'
        )
        # Add experience inputs near existing filters (after any "Fetch LinkedIn description" removal above)
        if 'min_exp = st.number_input("Minimum experience (years, 0=ignore)"' not in txt:
            txt = txt.replace(
                'country_indeed = st.text_input("country_indeed (for Indeed/Glassdoor)", value=CFG.get("country_indeed","USA"))',
                'country_indeed = st.text_input("country_indeed (for Indeed/Glassdoor)", value=CFG.get("country_indeed","USA"))\n'
                '    min_exp = st.number_input("Minimum experience (years, 0=ignore)", 0, 50, 0, 1)\n'
                '    max_exp = st.number_input("Maximum experience (years, 0=ignore)", 0, 50, 0, 1)'
            )

    # ---------- Safe summary line ----------
    if "Boards (JobSpy):" in txt and "boards_str = " not in txt:
        summary_pat = re.compile(r'st\.write\(.+Boards \(JobSpy\).+\)', re.S)
        summary_repl = (
            'boards_str = ", ".join(boards) if boards else "none"\n'
            '    try:\n'
            '        easy_apply_str = "ON" if linkedin_easy_apply else "OFF"\n'
            '    except NameError:\n'
            '        easy_apply_str = "OFF"\n'
            '    try:\n'
            '        wm = work_mode\n'
            '    except NameError:\n'
            '        wm = "Any"\n'
            '    st.write(f"**Boards (JobSpy):** {boards_str} • **Hours:** {hours_old} • **Work mode:** {wm} • **Easy Apply (LI):** {easy_apply_str}")'
        )
        txt = summary_pat.sub(summary_repl, txt)

    # ---------- Email panel after CSV download ----------
    if "📧 Email results (SMTP)" not in txt:
        email_block = textwrap.dedent("""
                # 📧 Email results (SMTP)
                with st.expander("📧 Email results"):
                    st.caption("Tip: For Gmail, create an **App password** (2FA required) and use smtp.gmail.com:587.")
                    default_server = os.getenv("SMTP_SERVER", "smtp.gmail.com")
                    default_port = int(os.getenv("SMTP_PORT", "587"))
                    default_user = os.getenv("SMTP_USER", "")
                    default_from = os.getenv("SMTP_FROM", default_user)

                    c1, c2 = st.columns(2)
                    with c1:
                        smtp_server = st.text_input("SMTP server", value=default_server)
                        smtp_port = st.number_input("SMTP port", min_value=1, max_value=65535, value=default_port, step=1)
                        smtp_user = st.text_input("SMTP username", value=default_user)
                        from_email = st.text_input("From email", value=default_from or smtp_user)
                    with c2:
                        to_field = st.text_input("To (comma-separated)")
                        smtp_pass = st.text_input("SMTP password / app password", type="password")
                        subject = st.text_input("Subject", value=f"Job scrape results — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
                        body = st.text_area("Message", value="Attached are the latest job results.", height=80)

                    colA, colB, colC = st.columns(3)
                    with colA:
                        if st.button("Send to Shazia (📧 shaziaiqra1@gmail.com)"):
                            try:
                                send_email_with_attachment(smtp_server, smtp_port, smtp_user, smtp_pass, from_email,
                                                           ["shaziaiqra1@gmail.com"], subject, body, csv, "jobs_scraped.csv")
                                st.success("Sent to shaziaiqra1@gmail.com")
                            except Exception as e:
                                st.error(f"Email error: {e}")
                    with colB:
                        if st.button("Send to Y M (📧 ymudulodu@gmail.com)"):
                            try:
                                send_email_with_attachment(smtp_server, smtp_port, smtp_user, smtp_pass, from_email,
                                                           ["ymudulodu@gmail.com"], subject, body, csv, "jobs_scraped.csv")
                                st.success("Sent to ymudulodu@gmail.com")
                            except Exception as e:
                                st.error(f"Email error: {e}")
                    with colC:
                        if st.button("Send to custom recipients"):
                            try:
                                tos = [t.strip() for t in to_field.split(',') if t.strip()]
                                if not tos:
                                    st.warning("Please enter at least one recipient in 'To'.")
                                else:
                                    send_email_with_attachment(smtp_server, smtp_port, smtp_user, smtp_pass, from_email,
                                                               tos, subject, body, csv, "jobs_scraped.csv")
                                    st.success(f"Sent to: {', '.join(tos)}")
                            except Exception as e:
                                st.error(f"Email error: {e}")
        """).rstrip()
        txt = txt.replace(
            'st.download_button("💾 Download CSV", data=csv, file_name="jobs_scraped.csv", mime="text/csv")',
            'st.download_button("💾 Download CSV", data=csv, file_name="jobs_scraped.csv", mime="text/csv")\n' + email_block
        )

    write(app_path, txt)

def patch_provider(provider_path: Path):
    ptxt = read(provider_path)
    backup(provider_path)
    # Ensure kwargs omits is_remote when None
    if "kwargs = dict(" in ptxt:
        # Try a broad replacement to include only when not None
        new = re.sub(
            r"kwargs = dict\([^\)]*\)",
            "kwargs = dict(\n        site_name=site_name,\n        search_term=search_term,\n        location=location,\n        results_wanted=results_wanted,\n        country_indeed=country_indeed,\n        linkedin_fetch_description=linkedin_fetch_description,\n        proxies=proxies,\n        user_agent=user_agent,\n        verbose=verbose,\n    )\n    if is_remote is not None:\n        kwargs['is_remote'] = bool(is_remote)",
            ptxt,
            flags=re.S
        )
        if new != ptxt:
            write(provider_path, new)

def patch_filtering_and_service():
    """Add work mode & experience filters if missing."""
    # filtering.py
    f = list(ROOT.glob("**/src/utils/filtering.py"))
    if not f:
        return
    fpath = f[0]
    ftxt = read(fpath)
    backup(fpath)

    addon = textwrap.dedent("""
    import re

    _WORK_MODE_PATTERNS = {
        "hybrid": re.compile(r"\\bhybrid\\b", re.I),
        "remote": re.compile(r"\\bremote\\b|\\bwork\\s*from\\s*home\\b|\\bW\\s*F\\s*H\\b", re.I),
        "onsite": re.compile(r"\\bon[-\\s]?site\\b", re.I),
    }

    _EXP_RE = re.compile(r\"\"\"(?<!\\d)(\\d{1,2})\\s*\\+?\\s*(?:years?|yrs?)\\s+(?:of\\s+)?(?:experience|exp)\"\"\", re.I)

    def infer_years_required(text: str) -> int | None:
        if not text:
            return None
        mlist = _EXP_RE.findall(text)
        if not mlist:
            return None
        try:
            nums = [int(x) for x in mlist]
            if not nums:
                return None
            return max(nums)
        except Exception:
            return None

    def _combine_text(row):
        parts = []
        for col in ("TITLE","DESCRIPTION","title","description"):
            if col in row and row[col]:
                parts.append(str(row[col]))
        return " \\n ".join(parts)

    def filter_by_work_mode(df: pd.DataFrame, mode: str) -> pd.DataFrame:
        if df is None or df.empty or not mode or mode.lower() == "any":
            return df
        m = mode.lower()
        if m == "hybrid":
            pat = _WORK_MODE_PATTERNS["hybrid"]
        elif m == "remote":
            pat = _WORK_MODE_PATTERNS["remote"]
        elif m == "onsite":
            pat = _WORK_MODE_PATTERNS["onsite"]
        else:
            return df
        mask = df.apply(lambda r: bool(pat.search(_combine_text(r))), axis=1)
        return df[mask].reset_index(drop=True)

    def filter_by_experience(df: pd.DataFrame, min_years: int | None = None, max_years: int | None = None, keep_unknown: bool = True) -> pd.DataFrame:
        if df is None or df.empty or (min_years is None and max_years is None):
            return df
        yrs = df.apply(lambda r: infer_years_required(_combine_text(r)), axis=1)
        def ok(val):
            if val is None:
                return keep_unknown
            if min_years is not None and val < min_years:
                return False
            if max_years is not None and val > max_years:
                return False
            return True
        mask = [ok(v) for v in yrs]
        out = df[mask].reset_index(drop=True)
        out["YEARS_REQ_EST"] = yrs
        return out
    """).strip("\n")

    if "filter_by_work_mode" not in ftxt:
        # Make sure pandas is imported at top
        if "import pandas as pd" not in ftxt:
            ftxt = "import pandas as pd\n" + ftxt
        ftxt = ftxt.rstrip() + "\n\n" + addon + "\n"
        write(fpath, ftxt)

    # search_service.py
    s = list(ROOT.glob("**/src/services/search_service.py"))
    if not s:
        return
    spath = s[0]
    stxt = read(spath)
    backup(spath)

    # Add new parameters
    if "work_mode:" not in stxt:
        stxt = stxt.replace(
            "def search_jobs(\n    titles: List[str],\n    locations: List[str],\n    boards: List[str],\n    hours_old: Optional[int] = 24,",
            "def search_jobs(\n    titles: List[str],\n    locations: List[str],\n    boards: List[str],\n    hours_old: Optional[int] = 24,\n    work_mode: str | None = 'any',\n    min_experience: int | None = None,\n    max_experience: int | None = None,"
        )

    # Map is_remote for non-linkedin sites
    stxt = stxt.replace(
        'is_remote=is_remote if site != "linkedin" else None,',
        'is_remote=(True if work_mode and work_mode.lower()=="remote" else (False if work_mode and work_mode.lower()=="onsite" else None)) if site != "linkedin" else None,'
    )

    # Post-filters before return
    if "filter_by_work_mode" not in stxt or "filter_by_experience" not in stxt:
        stxt = stxt.replace(
            "    return cleaned\n",
            textwrap.dedent("""
                # Work-mode & experience post-filters
                try:
                    from src.utils.filtering import filter_by_work_mode, filter_by_experience
                    cleaned = filter_by_work_mode(cleaned, work_mode or "any")
                    cleaned = filter_by_experience(cleaned, min_years=min_experience, max_years=max_experience, keep_unknown=True)
                except Exception:
                    pass
                return cleaned
            """)
        )

    write(spath, stxt)

def patch_app_call_signature(app_path: Path):
    # Ensure app calls search_jobs with the new args
    txt = read(app_path)
    backup(app_path)

    # Insert work_mode/min/max pass-through in the call
    txt = txt.replace(
        "results_df = search_jobs(",
        "results_df = search_jobs("
    )
    # Replace is_remote=normalize_remote(...) if present
    txt = txt.replace(
        "is_remote=normalize_remote(is_remote_sel),",
        'work_mode=work_mode.lower().replace(" only",""),\n                min_experience=(None if int(min_exp)==0 else int(min_exp)),\n                max_experience=(None if int(max_exp)==0 else int(max_exp)),'
    )
    # If the call doesn't have these, append them at end before ')'
    if "min_experience=" not in txt:
        txt = re.sub(
            r"(results_df\s*=\s*search_jobs\([^\)]*)\)",
            r"\1,\n                work_mode=work_mode.lower().replace(' only',''),\n                min_experience=(None if int(min_exp)==0 else int(min_exp)),\n                max_experience=(None if int(max_exp)==0 else int(max_exp))\n            )",
            txt,
            flags=re.S
        )

    write(app_path, txt)

def main():
    # Locate files
    app_files = list(ROOT.glob("**/app/app.py"))
    if not app_files:
        print("ERROR: app/app.py not found. Run this script from your project root.")
        sys.exit(1)
    app_path = app_files[0]

    provider_files = list(ROOT.glob("**/src/providers/jobspy_provider.py"))
    filtering_files = list(ROOT.glob("**/src/utils/filtering.py"))
    service_files = list(ROOT.glob("**/src/services/search_service.py"))

    print(f"Found app at: {app_path}")
    if provider_files: print(f"Found provider at: {provider_files[0]}")
    if filtering_files: print(f"Found filtering at: {filtering_files[0]}")
    if service_files: print(f"Found search_service at: {service_files[0]}")

    patch_app(app_path)
    if provider_files: patch_provider(provider_files[0])
    patch_filtering_and_service()
    patch_app_call_signature(app_path)

    print("\n✅ Patch applied. Restart Streamlit and test:")
    print("   ./.venv/bin/python -m streamlit run app/app.py")

if __name__ == "__main__":
    main()
