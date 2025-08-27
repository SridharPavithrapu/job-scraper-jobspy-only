# src/services/search_service.py
from __future__ import annotations

import os
import json
import time
import random
import logging
from typing import Iterable, List, Optional, Dict, Any

import pandas as pd

# Providers / utils
try:
    from src.providers.jobspy_provider import scrape_with_jobspy
except Exception:
    # Fallback import path if your layout differs
    from providers.jobspy_provider import scrape_with_jobspy

# Filtering / normalization helpers (import-guarded so this file works in CLI too)
try:
    from src.utils.filtering import (
        filter_by_hours,
        filter_by_work_mode,
        filter_by_experience,
        filter_by_employment_type,
        filter_by_titles,
    )
except Exception:
    def filter_by_hours(df: pd.DataFrame, hours_old: int) -> pd.DataFrame: return df
    def filter_by_work_mode(df: pd.DataFrame, work_mode: str) -> pd.DataFrame: return df
    def filter_by_experience(df: pd.DataFrame, min_years=None, max_years=None, keep_unknown=True) -> pd.DataFrame: return df
    def filter_by_employment_type(df: pd.DataFrame, employment_type: str|None) -> pd.DataFrame: return df
    def filter_by_titles(df: pd.DataFrame, titles: List[str]) -> pd.DataFrame: return df

try:
    from src.utils.dedupe import dedupe_jobs
except Exception:
    def dedupe_jobs(df: pd.DataFrame) -> pd.DataFrame: return df

try:
    from src.utils.normalize_jobpost import normalize_jobpost_df
except Exception:
    def normalize_jobpost_df(df: pd.DataFrame) -> pd.DataFrame: return df


# ---------- Debug sink (writes queries/raws/counts for you to inspect) ----------
class DebugSink:
    def __init__(self, root: str = "debug_runs"):
        self.root = root
        os.makedirs(self.root, exist_ok=True)

    @staticmethod
    def _safe(s: str) -> str:
        return "".join(ch if ch.isalnum() or ch in "-_." else "_" for ch in str(s))

    def write_json(self, name: str, obj: dict):
        path = os.path.join(self.root, self._safe(name))
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(obj, f, indent=2, ensure_ascii=False)

    def write_df(self, name: str, df: pd.DataFrame):
        path = os.path.join(self.root, self._safe(name))
        os.makedirs(os.path.dirname(path), exist_ok=True)
        df.to_csv(path, index=False)


# ---------- Helpers ----------
def _safe(s: str) -> str:
    return "".join(ch if ch.isalnum() or ch in "-_." else "_" for ch in str(s))


def _coerce_bool(x: Any) -> Optional[bool]:
    """Return True/False/None; never return a random truthy/falsy non-bool."""
    if x is True or x is False:
        return x
    if x is None:
        return None
    if isinstance(x, str):
        lx = x.strip().lower()
        if lx in {"true", "t", "1", "yes", "y"}:
            return True
        if lx in {"false", "f", "0", "no", "n"}:
            return False
    return None


def _no_comma_variant(loc: str) -> str:
    """Glassdoor sometimes accepts 'City ST' instead of 'City, ST'."""
    if "," in loc:
        parts = [p.strip() for p in loc.split(",", 1)]
        if len(parts) == 2 and len(parts[1]) <= 3:
            return f"{parts[0]} {parts[1]}"
    return ""


def _gd_locations_for(loc: str) -> List[str]:
    """
    Glassdoor rejects 'Remote' and many raw state-only inputs.
    We expect city/state style inputs like 'New York, NY'.
    If we get a pure state (e.g. 'New York', 'California'), we skip for GD.
    """
    s = (loc or "").strip()
    if not s:
        return []
    low = s.lower()

    # Skip generic remote for GD
    if low in {"remote", "remote (usa)", "all remote jobs", "anywhere"}:
        return []

    # If user passed "NY" or "New York" without a city, GD often 400s.
    # Prefer city+state inputs for GD. If not available, just skip and let Indeed/Google/LinkedIn cover it.
    if "," not in s:
        return []

    return [s]


def _normalize_location_for_site(site: str, loc: str) -> str:
    """
    Site-specific location normalization:
      - LinkedIn: constrain 'Remote' to 'United States' to avoid global country parse errors.
      - Indeed: 'United States' is a sane national scope for remote searches.
      - Glassdoor: don't pass 'Remote' at all (handled upstream with _gd_locations_for).
    """
    s = (loc or "").strip()
    if not s:
        return s

    low = s.lower()
    if low in {"remote", "remote (usa)", "all remote jobs", "anywhere"}:
        if site == "linkedin":
            return "United States"
        if site == "indeed":
            return "United States"
        # Glassdoor handled by _gd_locations_for; return empty to avoid accidental pass-through
        if site == "glassdoor":
            return ""
        return s

    return s


def _site_results_cap(site: str, results_wanted: int) -> int:
    # You can tune per-site caps here if needed
    return int(max(1, results_wanted))


def _build_site_passes(
    site: str,
    hours_old: Optional[int],
    work_mode: str,
    employment_type: Optional[str],
    linkedin_easy_apply: bool,
) -> List[Dict[str, Any]]:
    """
    Build multiple passes per site to comply with these JobSpy constraints:
      - Indeed: only one of {hours_old} OR {job_type/is_remote} OR {easy_apply}
      - LinkedIn: only one of {hours_old} OR {easy_apply}
    """
    passes: List[Dict[str, Any]] = []

    wm = (work_mode or "any").strip().lower()
    is_remote_flag = None
    if wm.startswith("remote"):
        is_remote_flag = True
    elif wm.startswith("on-site") or wm.startswith("onsite"):
        is_remote_flag = False

    job_type = None
    if employment_type:
        et = employment_type.strip().lower()
        if et in {"fulltime", "parttime", "internship", "contract"}:
            job_type = et

    if site == "indeed":
        # Split passes to comply with constraints
        # 1) hours pass
        passes.append({"hours_old": hours_old, "is_remote": None, "job_type": None, "easy_apply": False})
        # 2) job_type/is_remote pass (only if one/both are set)
        if is_remote_flag is not None or job_type:
            passes.append({"hours_old": None, "is_remote": is_remote_flag, "job_type": job_type, "easy_apply": False})
        # 3) (optional) easy_apply-only pass
        # JobSpy notes: Indeed easy_apply is supported as board-hosted filter
        # Uncomment if you really want it:
        # passes.append({"hours_old": None, "is_remote": None, "job_type": None, "easy_apply": True})

    elif site == "linkedin":
        # 1) hours pass
        passes.append({"hours_old": hours_old, "is_remote": is_remote_flag, "job_type": None, "easy_apply": False})
        # 2) easy apply pass (if user asked)
        if linkedin_easy_apply:
            passes.append({"hours_old": None, "is_remote": is_remote_flag, "job_type": None, "easy_apply": True})

    else:
        # google / glassdoor default single pass with hours
        passes.append({"hours_old": hours_old, "is_remote": is_remote_flag, "job_type": job_type, "easy_apply": False})

    # Make sure booleans are booleans/None
    for p in passes:
        p["is_remote"] = _coerce_bool(p.get("is_remote"))

    return passes


def _scrape_with_retry(site: str, call_kwargs: dict, max_retries: int = 3, proxies_iter=None) -> Optional[pd.DataFrame]:
    """
    Thin retry wrapper around scrape_with_jobspy with a LinkedIn-specific fallback
    for 'Invalid country string' on remote searches.
    """
    for attempt in range(1, max_retries + 1):
        try:
            return scrape_with_jobspy(**call_kwargs)
        except Exception as e:
            msg = str(e)
            # Special case: LinkedIn remote sometimes yields unknown countries → retry limiting to US
            if site == "linkedin" and "Invalid country string" in msg:
                alt = dict(call_kwargs)
                alt["location"] = "United States"
                logging.warning("LinkedIn country parse error; retrying with location=United States")
                try:
                    return scrape_with_jobspy(**alt)
                except Exception as e2:
                    logging.error(f"LinkedIn retry also failed: {e2}")

            logging.warning(
                f"JobSpy scrape failed (sites={call_kwargs.get('site_name')}, "
                f"location={call_kwargs.get('location')}), attempt {attempt}/{max_retries}: {e}"
            )
            time.sleep(1.1 * attempt)
    return None


# ---------- Core search ----------
def search_jobs(
    titles: List[str],
    locations: List[str],
    boards: List[str],
    *,
    strict_titles: bool = True,
    hours_old: Optional[int] = None,
    results_wanted: int = 100,
    work_mode: str = "any",                 # Any | Remote only | On-site only | Hybrid only
    employment_type: Optional[str] = None,  # fulltime | parttime | internship | contract
    min_experience: Optional[int] = None,
    max_experience: Optional[int] = None,
    country_indeed: str = "USA",
    linkedin_fetch_description: bool = False,
    linkedin_easy_apply: bool = False,
    jobspy_user_agent: Optional[str] = None,
    jobspy_proxies: Optional[List[str]] = None,
    jobspy_verbose: int = 2,
    per_site_delay: float = 0.6,
    sequential_mode: bool = True,
    debug_run_name: Optional[str] = None,
) -> pd.DataFrame:
    """
    Main orchestrator. Builds compliant calls per-site (JobSpy constraints), runs them,
    then normalizes / dedupes / filters / hours trimming.
    """
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("selenium").setLevel(logging.WARNING)

    sink = DebugSink(root=os.path.join("debug_runs", _safe(debug_run_name) if debug_run_name else "latest"))

    # Normalize inputs
    titles = [t for t in (titles or []) if str(t).strip()]
    locations = [l for l in (locations or []) if str(l).strip()]
    boards = [b.strip().lower() for b in (boards or []) if str(b).strip()]
    if not titles or not locations or not boards:
        return pd.DataFrame()

    frames: List[pd.DataFrame] = []

    # Iterate titles × locations × boards
    for title_term in titles:
        for loc_for_all in locations:

            if sequential_mode:
                for site in boards:
                    # Build valid passes per site
                    passes = _build_site_passes(site, hours_old, work_mode, employment_type, linkedin_easy_apply)
                    per_site_requested = _site_results_cap(site, results_wanted)
                    loc_for_site = _normalize_location_for_site(site, loc_for_all)
                    proxies_to_use = jobspy_proxies or []

                    # ---- Glassdoor special handling: DO NOT pass location; use hours + search_term only ----
                    if site == "glassdoor":
                        for pidx, pconf in enumerate(passes, start=1):
                            req = {
                                "site": site,
                                "title": title_term,
                                "location_original": loc_for_all,   # kept only for debugging context
                                "location_normalized": None,        # we are not sending a location to GD
                                "pass": pidx,
                                "hours_old": pconf.get("hours_old"),
                                "easy_apply": False,
                                "is_remote": None,                  # ignored for GD in this mode
                                "job_type": None,                   # ignored for GD in this mode
                                "results_wanted": per_site_requested,
                                "country_indeed": country_indeed,
                                "linkedin_fetch_description": False,
                                "proxies": proxies_to_use or [],
                                "user_agent": jobspy_user_agent or "",
                                "verbose": jobspy_verbose,
                            }
                            sink.write_json(f"query_{_safe(site)}_{pidx}_{_safe(title_term)}_no_loc.json", req)

                            # No 'location' or 'distance' for GD; just hours + search_term
                            call_kwargs = dict(
                                site_name=["glassdoor"],
                                search_term=title_term,
                                results_wanted=per_site_requested,
                                country_indeed=country_indeed,
                                hours_old=req["hours_old"],
                                easy_apply=False,
                                linkedin_fetch_description=False,
                                proxies=proxies_to_use,
                                user_agent=jobspy_user_agent,
                                verbose=jobspy_verbose,
                            )

                            df = _scrape_with_retry("glassdoor", call_kwargs, max_retries=4)

                            if df is not None and not df.empty:
                                df = df.copy()
                                df["SEARCH_TITLE"] = title_term
                                df["SEARCH_LOCATION"] = loc_for_all
                                df["NORMALIZED_LOCATION"] = ""  # explicitly blank because we didn't send one
                                sink.write_df(f"raw_{_safe(site)}_{pidx}_{_safe(title_term)}_no_loc.csv", df)
                                frames.append(df)

                            time.sleep(max(0.0, float(per_site_delay) + random.uniform(0, 0.6)))

                        # Done with GD for this location
                        continue
                    # ----------------- end Glassdoor special case -----------------

                    for pidx, pconf in enumerate(passes, start=1):
                        # LinkedIn & others use normalized location
                        req_loc = loc_for_site if site != "google" else loc_for_all

                        req = {
                            "site": site,
                            "title": title_term,
                            "location_original": loc_for_all,
                            "location_normalized": req_loc,
                            "pass": pidx,
                            "hours_old": pconf.get("hours_old"),
                            "easy_apply": pconf.get("easy_apply"),
                            "is_remote": pconf.get("is_remote"),
                            "job_type": pconf.get("job_type"),
                            "results_wanted": per_site_requested,
                            "country_indeed": country_indeed,
                            "linkedin_fetch_description": (linkedin_fetch_description if site == "linkedin" else False),
                            "proxies": proxies_to_use or [],
                            "user_agent": jobspy_user_agent or "",
                            "verbose": jobspy_verbose,
                        }
                        sink.write_json(f"query_{_safe(site)}_{pidx}_{_safe(title_term)}_{_safe(req_loc)}.json", req)

                        call_kwargs = dict(
                            site_name=[site],
                            location=req_loc,
                            results_wanted=per_site_requested,
                            country_indeed=country_indeed,
                            is_remote=req["is_remote"],
                            linkedin_fetch_description=(linkedin_fetch_description if site == "linkedin" else False),
                            proxies=proxies_to_use,
                            user_agent=jobspy_user_agent,
                            verbose=jobspy_verbose,
                            easy_apply=req["easy_apply"] or False,
                        )

                        # Google: must use google_search_term
                        if site == "google":
                            call_kwargs["google_search_term"] = f"{title_term} {loc_for_all}"
                        else:
                            call_kwargs["search_term"] = title_term

                        # Per-pass time constraint
                        call_kwargs["hours_old"] = req["hours_old"]

                        # Indeed: only send job_type in the non-hours pass
                        if site == "indeed" and req.get("job_type"):
                            call_kwargs["job_type"] = req["job_type"]

                        df = _scrape_with_retry(site, call_kwargs, max_retries=4)

                        # Google fallback phrasing
                        if site == "google" and (df is None or df.empty):
                            alt_kwargs = call_kwargs.copy()
                            alt_kwargs["google_search_term"] = f"{title_term} jobs in {loc_for_all}"
                            df = _scrape_with_retry(site, alt_kwargs, max_retries=2)

                        if df is not None and not df.empty:
                            df = df.copy()
                            df["SEARCH_TITLE"] = title_term
                            df["SEARCH_LOCATION"] = loc_for_all
                            df["NORMALIZED_LOCATION"] = req_loc
                            sink.write_df(f"raw_{_safe(site)}_{pidx}_{_safe(title_term)}_{_safe(req_loc)}.csv", df)
                            frames.append(df)

                        time.sleep(max(0.0, float(per_site_delay) + random.uniform(0, 0.6)))

            else:
                # If ever used: non-sequential simple pass
                for site in boards:
                    passes = _build_site_passes(site, hours_old, work_mode, employment_type, linkedin_easy_apply)
                    per_site_requested = _site_results_cap(site, results_wanted)
                    proxies_to_use = jobspy_proxies or []

                    for pidx, pconf in enumerate(passes, start=1):
                        loc_for_site = _normalize_location_for_site(site, loc_for_all)
                        call_kwargs = dict(
                            site_name=[site],
                            location=loc_for_site if site != "google" else loc_for_all,
                            results_wanted=per_site_requested,
                            country_indeed=country_indeed,
                            proxies=proxies_to_use,
                            user_agent=jobspy_user_agent,
                            verbose=jobspy_verbose,
                            easy_apply=pconf.get("easy_apply") or False,
                            hours_old=pconf.get("hours_old"),
                        )
                        wm = (work_mode or "any").strip().lower()
                        is_remote_flag = None
                        if site != "linkedin":
                            if wm.startswith("remote"):
                                is_remote_flag = True
                            elif wm.startswith("on-site") or wm.startswith("onsite"):
                                is_remote_flag = False
                        call_kwargs["is_remote"] = _coerce_bool(pconf.get("is_remote", is_remote_flag))

                        if pconf.get("job_type"):
                            call_kwargs["job_type"] = pconf["job_type"]

                        if site == "google":
                            call_kwargs["google_search_term"] = f"{title_term} {loc_for_all}"
                        else:
                            call_kwargs["search_term"] = title_term

                        df = _scrape_with_retry(site, call_kwargs, max_retries=3)

                        if site == "google" and (df is None or df.empty):
                            alt_kwargs = call_kwargs.copy()
                            alt_kwargs["google_search_term"] = f"{title_term} jobs in {loc_for_all}"
                            df = _scrape_with_retry(site, alt_kwargs, max_retries=2)

                        if df is not None and not df.empty:
                            df = df.copy()
                            df["SEARCH_TITLE"] = title_term
                            df["SEARCH_LOCATION"] = loc_for_all
                            df["NORMALIZED_LOCATION"] = loc_for_site
                            sink.write_df(f"raw_{_safe(site)}_{pidx}_{_safe(title_term)}_{_safe(loc_for_site)}.csv", df)
                            frames.append(df)

    # Avoid pandas concat warning: drop empties
    frames = [f for f in frames if f is not None and not f.empty]
    if not frames:
        return pd.DataFrame()

    merged = pd.concat(frames, ignore_index=True, sort=False)
    sink.write_df("merged_all.csv", merged)

    # Normalize columns to a canonical schema
    try:
        merged = normalize_jobpost_df(merged)
    except Exception:
        pass

    # Deduplicate
    cleaned = dedupe_jobs(merged)

    # Hours window (best effort if boards rounded differently)
    try:
        if hours_old is not None:
            cleaned = filter_by_hours(cleaned, hours_old)
    except Exception:
        pass

    # Post-filters
    try:
        cleaned = filter_by_work_mode(cleaned, work_mode or "any")
        cleaned = filter_by_experience(cleaned, min_years=min_experience, max_years=max_experience, keep_unknown=True)
        cleaned = filter_by_employment_type(cleaned, employment_type)
        if strict_titles:
            cleaned = filter_by_titles(cleaned, titles)
    except Exception:
        pass

    # Final debug
    try:
        def _site_counts(df: pd.DataFrame) -> Dict[str, int]:
            col = "site_name" if "site_name" in df.columns else ("site" if "site" in df.columns else None)
            if not col:
                return {}
            s = (df[col].fillna("").astype(str).str.lower()
                 .replace({"linkedin": "linkedin", "glassdoor": "glassdoor", "google": "google", "indeed": "indeed"}))
            out = s.value_counts(dropna=False)
            return {str(k): int(v) for k, v in out.items()}

        with_counts = _site_counts(cleaned)
        json_counts = {k: int(v) for k, v in with_counts.items()}
        sink.write_json("counts_final.json", json_counts)
        sink.write_df("filtered_all.csv", cleaned)
    except Exception:
        pass

    return cleaned
