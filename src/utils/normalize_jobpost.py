from __future__ import annotations
import re
from urllib.parse import urlparse
import pandas as pd

# Canonical order used downstream (emails/CSV, filtering, etc.)
CANONICAL_ORDER = [
    "site_name","title","company","company_url","company_industry","company_logo",
    "job_url","location","city","state","country","is_remote",
    "job_type","job_level",
    "interval","min_amount","max_amount","currency","salary_source",
    "date_posted","emails",
    # Provenance from our search service
    "SEARCH_TITLE","SEARCH_LOCATION","NORMALIZED_LOCATION",
    # Indeed extras we want to preserve if present
    "company_country","company_addresses","company_employees_label","company_revenue_label","company_description",
    # Long text last
    "description",
]

# Fallbacks for boards that use different names or cases
ALT_MAP = {
    # board/site
    "site_name": ["site", "board", "source", "SITE_NAME", "SITE"],

    # basic job fields (include uppercase variants JobSpy sometimes uses)
    "title": ["TITLE","job_title","position"],
    "company": ["COMPANY","employer","company_name"],
    "company_url": ["COMPANY_URL","employer_url","company_link","company_site"],
    "company_industry": ["COMPANY_INDUSTRY","industry"],
    "company_logo": ["COMPANY_LOGO","logo"],
    "job_url": ["URL","url","link"],
    "location": ["LOCATION","full_location","job_location"],
    "city": ["job_city","CITY"],
    "state": ["job_state","region","STATE"],
    "country": ["job_country","COUNTRY"],
    "is_remote": ["IS_REMOTE","remote","work_from_home"],

    "job_type": ["JOB_TYPE","employment_type","EMPLOYMENT_TYPE","type","TYPE"],
    "job_level": ["linkedin_level","seniority","LEVEL"],

    "interval": ["INTERVAL","salary_interval","pay_interval"],
    "min_amount": ["MIN_AMOUNT","salary_min","min_salary","min_pay"],
    "max_amount": ["MAX_AMOUNT","salary_max","max_salary","max_pay"],
    "currency": ["CURRENCY","salary_currency","pay_currency"],
    "salary_source": ["SALARY_SOURCE","comp_source"],

    "date_posted": ["DATE_POSTED","posted_at","POSTED_AT","published_on","date","DATE"],

    "emails": ["EMAILS","contact_emails"],
    "description": ["DESCRIPTION","desc","job_description"],

    # provenance (what our service writes)
    "SEARCH_TITLE": ["SEARCH_TERM","QUERY","search_term"],
    "SEARCH_LOCATION": ["search_location","SEARCH_CITY","SEARCH_REGION"],
    "NORMALIZED_LOCATION": ["normalized_location","NORMALIZED_LOC"],

    # Indeed-specific extras — accept both already-normalized and prefixed names
    "company_country": ["indeed_company_country","COMPANY_COUNTRY"],
    "company_addresses": ["indeed_company_addresses","COMPANY_ADDRESSES"],
    "company_employees_label": ["indeed_company_employees_label","COMPANY_EMPLOYEES_LABEL"],
    "company_revenue_label": ["indeed_company_revenue_label","COMPANY_REVENUE_LABEL"],
    "company_description": ["indeed_company_description","COMPANY_DESCRIPTION"],
}

_US_ST_ABBR = {
    "AL","AK","AZ","AR","CA","CO","CT","DE","DC","FL","GA","HI","ID","IL","IN","IA","KS","KY",
    "LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ","NM","NY","NC","ND","OH",
    "OK","OR","PA","RI","SC","SD","TN","TX","UT","VT","VA","WA","WV","WI","WY"
}

_EMAIL_RE = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.I)


def _infer_site_from_url(url):
    if not url: return None
    host = urlparse(str(url)).netloc.lower()
    if "indeed" in host: return "indeed"
    if "glassdoor" in host: return "glassdoor"
    if "ziprecruiter" in host: return "zip_recruiter"
    if "linkedin" in host: return "linkedin"
    if "google" in host: return "google"
    return None


def _coalesce(df: pd.DataFrame, target: str) -> pd.Series:
    if target in df.columns:
        return df[target]
    for alt in ALT_MAP.get(target, []):
        if alt in df.columns:
            return df[alt]
    return pd.Series([None]*len(df), index=df.index)


def _parse_location(s: str | None) -> tuple[str | None, str | None, str | None]:
    """Very light parser: 'City, ST, Country' or 'City, ST' or 'Remote'."""
    if not s:
        return None, None, None
    text = str(s).strip()
    if not text or text.lower() == "remote":
        return None, None, None
    parts = [p.strip() for p in text.split(",")]
    city = parts[0] if parts else None
    state = None
    country = None
    if len(parts) >= 2:
        st = parts[1].split()[0].upper()
        if st in _US_ST_ABBR:
            state = st
    if len(parts) >= 3:
        country = parts[2].upper()
    return city or None, state, country


def normalize_jobpost_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize board-specific / case-variant columns to a canonical schema.
    Keeps all extra columns to avoid data loss.
    """
    if df is None or df.empty:
        return df

    out = pd.DataFrame(index=df.index)

    # 1) Map canonical columns (coalesce preserves existing columns)
    out["site_name"] = _coalesce(df, "site_name")
    for col in CANONICAL_ORDER:
        if col == "site_name":
            continue
        out[col] = _coalesce(df, col)

    # 2) site_name: infer from URL if missing, lower-case, and normalize ziprecruiter spelling
    if out["site_name"].isna().any():
        inferred = out["job_url"].apply(_infer_site_from_url)
        out["site_name"] = out["site_name"].fillna(inferred)
    out["site_name"] = out["site_name"].astype(str).str.strip().str.lower()
    out.loc[out["site_name"].eq("ziprecruiter"), "site_name"] = "zip_recruiter"

    # 3) Basic cleanup for job_url/title/company
    if "job_url" in out.columns:
        out["job_url"] = out["job_url"].astype(str).str.strip().replace({"": None})
    if "title" in out.columns:
        out["title"] = out["title"].astype(str).str.strip()
    if "company" in out.columns:
        out["company"] = out["company"].astype(str).str.strip()

    # 4) If city/state/country are empty, try to derive from 'location'
    if "location" in out.columns:
        need_city = ("city" in out.columns) and out["city"].isna().all()
        need_state = ("state" in out.columns) and out["state"].isna().all()
        need_country = ("country" in out.columns) and out["country"].isna().all()
        if need_city or need_state or need_country:
            parsed = out["location"].apply(_parse_location)
            if need_city:
                out["city"] = [p[0] for p in parsed]
            if need_state:
                out["state"] = [p[1] for p in parsed]
            if need_country:
                out["country"] = [p[2] for p in parsed]

    # 5) Ensure emails is list-like if provided as string
    if "emails" in out.columns:
        def _fix_emails(v):
            if v is None or (isinstance(v, float) and pd.isna(v)):
                return None
            if isinstance(v, list):
                return v
            text = str(v)
            found = _EMAIL_RE.findall(text)
            return found or None
        out["emails"] = out["emails"].apply(_fix_emails)

    # 6) Preserve any extra columns
    for col in df.columns:
        if col not in out.columns:
            out[col] = df[col]

    # 7) Numeric salary
    for c in ("min_amount","max_amount"):
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce")

    # 8) Dates → naive datetime
    if "date_posted" in out.columns:
        out["date_posted"] = pd.to_datetime(out["date_posted"], errors="coerce").dt.tz_localize(None)

    # 9) Final column order
    canonical = [c for c in CANONICAL_ORDER if c in out.columns]
    rest = [c for c in out.columns if c not in canonical]
    return out[canonical + rest]
