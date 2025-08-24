# src/services/search_service.py
from __future__ import annotations

import os
import json
import time
import random
import logging
from pathlib import Path
from datetime import datetime
from typing import Any, Dict, Iterable, Iterator, List, Optional

import pandas as pd

# --- Local imports (with fallbacks where safe) ---
try:
    from src.providers.jobspy_provider import scrape_with_jobspy
except Exception as e:
    raise RuntimeError("Missing provider: src/providers/jobspy_provider.py") from e

try:
    from src.utils.filtering import (
        filter_by_hours,
        filter_by_work_mode,
        filter_by_employment_type,
        filter_by_experience,
        filter_title_contains_any,
    )
except Exception:
    # Minimal fallbacks if filtering.py isn't present (keeps pipeline running)
    def filter_by_hours(df: pd.DataFrame, hours_old: int, keep_unknown: bool = True) -> pd.DataFrame:
        return df

    def filter_by_work_mode(df: pd.DataFrame, work_mode: str) -> pd.DataFrame:
        return df

    def filter_by_employment_type(df: pd.DataFrame, employment_type: str) -> pd.DataFrame:
        return df

    def filter_by_experience(df: pd.DataFrame, min_years=None, max_years=None, keep_unknown=True) -> pd.DataFrame:
        return df

    def filter_title_contains_any(df: pd.DataFrame, titles: List[str], include_abbrevs: bool = True) -> pd.DataFrame:
        if df is None or df.empty:
            return df
        patt = "|".join([pd.re.escape(t.strip()) for t in titles if t.strip()])  # type: ignore[attr-defined]
        if not patt:
            return df
        title_col = "title" if "title" in df.columns else ("TITLE" if "TITLE" in df.columns else None)
        if not title_col:
            return df
        return df[df[title_col].astype(str).str.contains(patt, case=False, regex=True, na=False)]

try:
    from src.utils.normalize_jobpost import normalize_jobpost_df
except Exception:
    def normalize_jobpost_df(df: pd.DataFrame) -> pd.DataFrame:
        return df

try:
    from src.utils.dedupe import dedupe_jobs
except Exception:
    # Safe fallback dedupe by job_url if present
    def dedupe_jobs(df: pd.DataFrame) -> pd.DataFrame:
        if df is None or df.empty:
            return df
        if "job_url" in df.columns:
            return df.drop_duplicates(subset=["job_url"])
        return df.drop_duplicates()

# -----------------------------------------------------------------------------
# Debug sink (writes artifacts into debug_runs/<timestamp>/)
# -----------------------------------------------------------------------------

class DebugSink:
    def __init__(self, enabled: bool = False, base_dir: str | Path = "debug_runs"):
        self.enabled = bool(enabled)
        self.base_dir = Path(base_dir)
        self.run_dir: Optional[Path] = None
        if self.enabled:
            stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
            self.run_dir = self.base_dir / stamp
            self.run_dir.mkdir(parents=True, exist_ok=True)

    def _p(self, name: str) -> Path:
        assert self.run_dir is not None
        return self.run_dir / name

    def write_df(self, name: str, df: pd.DataFrame):
        if not self.enabled or df is None:
            return
        try:
            self._p(name).parent.mkdir(parents=True, exist_ok=True)
            df.to_csv(self._p(name), index=False)
        except Exception:
            pass

    def write_json(self, name: str, obj: Any):
        if not self.enabled:
            return
        try:
            self._p(name).parent.mkdir(parents=True, exist_ok=True)

            # ensure numpy types serialize
            import numpy as np
            def _default(o):
                if isinstance(o, (np.integer,)): return int(o)
                if isinstance(o, (np.floating,)): return float(o)
                if isinstance(o, (np.ndarray,)): return o.tolist()
                return str(o)

            with open(self._p(name), "w", encoding="utf-8") as f:
                json.dump(obj, f, indent=2, ensure_ascii=False, default=_default)
        except Exception:
            pass

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------

def _safe(s: str) -> str:
    return "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in str(s))[:120]

def _site_counts(df: pd.DataFrame) -> Dict[str, int]:
    """JSON-safe per-site counts."""
    if df is None or df.empty:
        return {}
    from urllib.parse import urlparse

    def infer(url):
        if not isinstance(url, str):
            return "unknown"
        h = urlparse(url).netloc.lower()
        if "indeed" in h: return "indeed"
        if "glassdoor" in h: return "glassdoor"
        if "ziprecruiter" in h: return "zip_recruiter"
        if "linkedin" in h: return "linkedin"
        if "google" in h: return "google"
        return "unknown"

    if "site_name" in df.columns:
        sites = df["site_name"].fillna("unknown")
    else:
        sites = df.get("job_url", pd.Series(["unknown"] * len(df))).apply(infer)
    vc = sites.value_counts(dropna=False)
    return {str(k): int(v) for k, v in vc.items()}

# --- Country normalization per JobSpy README ---
_ALLOWED_COUNTRIES = {
    "Argentina","Australia","Austria","Bahrain","Belgium","Brazil","Canada","Chile","China","Colombia","Costa Rica","Czech Republic",
    "Denmark","Ecuador","Egypt","Finland","France","Germany","Greece","Hong Kong","Hungary","India","Indonesia","Ireland","Israel","Italy",
    "Japan","Kuwait","Luxembourg","Malaysia","Mexico","Morocco","Netherlands","New Zealand","Nigeria","Norway","Oman","Pakistan","Panama",
    "Peru","Philippines","Poland","Portugal","Qatar","Romania","Saudi Arabia","Singapore","South Africa","South Korea","Spain","Sweden",
    "Switzerland","Taiwan","Thailand","Turkey","Ukraine","United Arab Emirates","UK","USA","Uruguay","Venezuela","Vietnam"
}

def normalize_country(c: str | None) -> str:
    """
    Map common aliases to JobSpy’s accepted names and enforce allowlist.
    Default to 'USA' if not provided or not recognized.
    """
    if not c:
        return "USA"
    c = c.strip()
    # common aliases
    alias = {
        "us": "USA", "u.s.": "USA", "u.s.a.": "USA", "united states": "USA", "usa": "USA",
        "uk": "UK", "united kingdom": "UK",
    }
    c_norm = alias.get(c.lower(), c)
    if c_norm not in _ALLOWED_COUNTRIES:
        return "USA"
    return c_norm

# --- US state helpers & Glassdoor fallbacks ---
_STATE_NAME_TO_ABBR = {
    "alabama":"AL","alaska":"AK","arizona":"AZ","arkansas":"AR","california":"CA","colorado":"CO",
    "connecticut":"CT","delaware":"DE","district of columbia":"DC","florida":"FL","georgia":"GA",
    "hawaii":"HI","idaho":"ID","illinois":"IL","indiana":"IN","iowa":"IA","kansas":"KS","kentucky":"KY",
    "louisiana":"LA","maine":"ME","maryland":"MD","massachusetts":"MA","michigan":"MI","minnesota":"MN",
    "mississippi":"MS","missouri":"MO","montana":"MT","nebraska":"NE","nevada":"NV","new hampshire":"NH",
    "new jersey":"NJ","new mexico":"NM","new york":"NY","north carolina":"NC","north dakota":"ND","ohio":"OH",
    "oklahoma":"OK","oregon":"OR","pennsylvania":"PA","rhode island":"RI","south carolina":"SC",
    "south dakota":"SD","tennessee":"TN","texas":"TX","utah":"UT","vermont":"VT","virginia":"VA",
    "washington":"WA","west virginia":"WV","wisconsin":"WI","wyoming":"WY",
}
_ABBR_SET = set(_STATE_NAME_TO_ABBR.values())

_GD_STATE_FALLBACKS = {
    "NJ": ["Newark, NJ", "Jersey City, NJ", "Edison, NJ"],
    "CT": ["Hartford, CT", "Stamford, CT", "New Haven, CT"],
    "NY": ["New York, NY", "Buffalo, NY", "Albany, NY"],
    "CA": ["Los Angeles, CA", "San Francisco, CA", "San Jose, CA", "San Diego, CA", "Sacramento, CA"],
    "TX": ["Dallas, TX", "Houston, TX", "Austin, TX", "San Antonio, TX"],
}

def _no_comma_variant(city_st: str) -> str | None:
    if not city_st or "," not in city_st:
        return None
    city, st = [p.strip() for p in city_st.split(",", 1)]
    if not city or not st:
        return None
    return f"{city} {st}"

def _clean_country_suffix(x: str) -> str:
    x = x.strip()
    lowers = x.lower()
    for suf in (", us", ", usa", ", united states", ", united states of america", ", u.s.", ", u.s.a."):
        if lowers.endswith(suf):
            return x[: -len(suf)].strip()
    return x

def _to_city_state(candidate: str) -> str | None:
    if "," not in candidate:
        return None
    city, right = [p.strip() for p in candidate.split(",", 1)]
    if not city or not right:
        return None
    rlow = right.lower()
    if rlow in _STATE_NAME_TO_ABBR:
        return f"{city}, {_STATE_NAME_TO_ABBR[rlow]}"
    if right.upper() in _ABBR_SET and len(right) == 2:
        return f"{city}, {right.upper()}"
    return None

def _split_city_state(loc: str) -> tuple[str, str]:
    if not loc:
        return "", ""
    parts = [p.strip() for p in str(loc).split(",", 1)]
    if len(parts) == 2:
        return parts[0], parts[1]
    return str(loc), ""

def _gd_locations_for(loc: str) -> list[str]:
    if not loc:
        return []
    x = _clean_country_suffix(str(loc))
    xl = x.lower()
    if "remote" in xl:
        return []
    if xl in _STATE_NAME_TO_ABBR:
        st = _STATE_NAME_TO_ABBR[xl]
        return _GD_STATE_FALLBACKS.get(st, [])
    if len(x.strip()) in (2, 3) and x.strip().upper()[:2] in _ABBR_SET:
        st = x.strip().upper()[:2]
        return _GD_STATE_FALLBACKS.get(st, [])
    as_city_st = _to_city_state(x)
    if as_city_st:
        return [as_city_st]
    return [x]

def _site_results_cap(site: str, desired: int) -> int:
    site = (site or "").lower()
    if site == "zip_recruiter":
        return min(desired, 25)
    if site == "google":
        return min(desired, 50)
    return desired

def _proxy_rotator(proxies: Optional[List[str]]) -> Optional[Iterator[Optional[str]]]:
    if not proxies:
        return None
    def _iter():
        i = 0
        while True:
            yield proxies[i % len(proxies)]
            i += 1
    return _iter()

def _scrape_with_retry(site: str, call_kwargs: Dict[str, Any], max_retries: int = 3,
                       proxies_iter: Optional[Iterator[str]] = None) -> pd.DataFrame:
    attempt = 0
    last_exc = None
    while attempt < max_retries:
        try:
            return scrape_with_jobspy(**call_kwargs)
        except Exception as e:
            last_exc = e
            if proxies_iter is not None:
                try:
                    call_kwargs["proxies"] = [next(proxies_iter)]
                except Exception:
                    pass
            time.sleep(1.0 + attempt * 0.7)
            attempt += 1
    raise last_exc

# Build per-site passes enforcing JobSpy "only one of" constraints
def _build_site_passes(site: str, hours_old: Optional[int],
                       work_mode: str, employment_type: str,
                       linkedin_easy_apply: bool) -> List[Dict[str, Any]]:
    s = (site or "").lower()
    wm = (work_mode or "any").strip().lower()

    is_remote_flag = None
    if wm.startswith("remote"):
        is_remote_flag = True
    elif wm.startswith("on-site") or wm.startswith("onsite"):
        is_remote_flag = False

    job_type = None
    if employment_type and employment_type.lower() in {"fulltime", "parttime", "internship", "contract", "full-time", "part-time"}:
        job_type = employment_type.lower().replace("-", "")

    if s == "indeed":
        pA = {"hours_old": hours_old, "easy_apply": False, "is_remote": None, "job_type": None}
        pB = {"hours_old": None, "easy_apply": False, "is_remote": is_remote_flag, "job_type": job_type}
        passes = []
        if pA["hours_old"] is not None:
            passes.append(pA)
        passes.append(pB)
        return passes

    if s == "linkedin":
        return [{"hours_old": hours_old, "easy_apply": False, "is_remote": None, "job_type": None}]

    return [{
        "hours_old": hours_old,
        "easy_apply": False,
        "is_remote": is_remote_flag,
        "job_type": job_type,
    }]

def _normalize_location_for_site(site: str, loc: str) -> str:
    return loc

# -----------------------------------------------------------------------------
# Main entry
# -----------------------------------------------------------------------------

def search_jobs(
    titles: List[str],
    locations: List[str],
    boards: List[str],
    hours_old: Optional[int] = None,
    work_mode: str = "any",                    # "any" | "Remote only" | "On-site only" | "Hybrid only"
    min_experience: Optional[int] = None,
    max_experience: Optional[int] = None,
    strict_titles: bool = True,
    employment_type: str = "any",              # "any" | "fulltime" | "parttime" | "contract" | "internship"
    results_wanted: int = 100,
    country_indeed: str = "USA",
    is_remote: Optional[bool] = None,          # deprecated in favor of work_mode; kept for signature compat
    linkedin_fetch_description: bool = False,
    jobspy_proxies: Optional[List[str]] = None,
    jobspy_user_agent: Optional[str] = None,
    jobspy_verbose: int = 2,
    linkedin_easy_apply: bool = False,
    sequential_mode: bool = True,
    per_site_delay: float = 2.0,
    debug_enabled: bool = False,
    quiet_logs: bool = True,
) -> pd.DataFrame:

    if quiet_logs:
        for name in ("JobSpy", "jobspy", "selenium", "urllib3"):
            logging.getLogger(name).setLevel(logging.WARNING)

    sink = DebugSink(enabled=debug_enabled)

    # Normalize inputs
    titles = [t.strip() for t in (titles or []) if str(t).strip()]
    locations = [l.strip() for l in (locations or []) if str(l).strip()]
    boards = [b.strip().lower() for b in (boards or []) if str(b).strip()]
    country_indeed = normalize_country(country_indeed)

    if not jobspy_user_agent:
        jobspy_user_agent = (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/119.0.0.0 Safari/537.36"
        )

    def _strip_country(x: str) -> str:
        x = x.strip()
        xl = x.lower()
        for suf in (", us", ", usa", ", united states", ", u.s.", ", u.s.a."):
            if xl.endswith(suf):
                return x[: -len(suf)].strip()
        return x

    locations = [("Remote" if l.strip().lower().startswith("remote") else _strip_country(l)) for l in locations]

    if not titles or not locations or not boards:
        return pd.DataFrame()

    proxy_iter = _proxy_rotator(jobspy_proxies)
    frames: List[pd.DataFrame] = []

    for title_term in titles:
        for loc in locations:
            loc_for_all = loc

            if sequential_mode:
                for site in boards:
                    passes = _build_site_passes(site, hours_old, work_mode, employment_type, linkedin_easy_apply)
                    per_site_requested = _site_results_cap(site, results_wanted)
                    loc_for_site = _normalize_location_for_site(site, loc_for_all)
                    proxies_to_use = [next(proxy_iter)] if proxy_iter else jobspy_proxies

                    # ===================== Glassdoor: search_term-only strategy =====================
                    if site == "glassdoor":
                        gd_locs = _gd_locations_for(loc_for_site)
                        if not gd_locs:
                            # skip invalid GD targets like "Remote"
                            continue

                        for gd_loc in gd_locs:
                            for pidx, pconf in enumerate(passes, start=1):
                                # Build two terms: quoted and unquoted
                                if gd_loc:
                                    term1 = f'{title_term} "{gd_loc}"'
                                    term2 = f"{title_term} {gd_loc}"
                                else:
                                    term1 = title_term
                                    term2 = title_term

                                # Keep for debug json
                                req = {
                                    "site": site,
                                    "title": title_term,
                                    "gd_loc": gd_loc,
                                    "pass": pidx,
                                    "hours_old": pconf.get("hours_old"),
                                    "results_wanted": per_site_requested,
                                    "country_indeed": country_indeed,
                                    "proxies": proxies_to_use or [],
                                    "user_agent": jobspy_user_agent or "",
                                    "verbose": jobspy_verbose,
                                    "mode": "search_term_only_no_location",
                                }
                                sink.write_json(f"query_{_safe(site)}_{pidx}_{_safe(title_term)}_{_safe(gd_loc)}.json", req)

                                def _gd_call(term: str):
                                    call_kwargs = dict(
                                        site_name=["glassdoor"],
                                        search_term=term,
                                        # IMPORTANT: do not pass location/is_remote to avoid GD resolver
                                        results_wanted=per_site_requested,
                                        country_indeed=country_indeed,  # harmless, per JobSpy notes
                                        hours_old=pconf.get("hours_old"),
                                        proxies=proxies_to_use,
                                        user_agent=jobspy_user_agent,
                                        verbose=jobspy_verbose,
                                        easy_apply=False,
                                    )
                                    return _scrape_with_retry("glassdoor", call_kwargs, max_retries=3, proxies_iter=proxy_iter)

                                # Attempt 1: quoted
                                df = _gd_call(term1)
                                # Attempt 2: unquoted
                                if (df is None or df.empty):
                                    df = _gd_call(term2)

                                # Optional: tighten by city/state if we have them
                                if df is not None and not df.empty and gd_loc and ("location" in df.columns):
                                    city, state = _split_city_state(gd_loc)
                                    m = False
                                    if state:
                                        m = df["location"].astype(str).str.contains(state, case=False, na=False)
                                    if city:
                                        m = m | df["location"].astype(str).str.contains(city, case=False, na=False)
                                    if isinstance(m, pd.Series):
                                        df = df[m]

                                if df is not None and not df.empty:
                                    df = df.copy()
                                    df["SEARCH_TITLE"] = title_term
                                    df["SEARCH_LOCATION"] = loc_for_all
                                    df["NORMALIZED_LOCATION"] = gd_loc
                                    sink.write_df(f"raw_glassdoor_{pidx}_{_safe(title_term)}_{_safe(gd_loc)}.csv", df)
                                    frames.append(df)

                                time.sleep(max(0.0, float(per_site_delay) + random.uniform(0, 0.6)))

                        # Done with GD for this location
                        continue
                    # =================== end Glassdoor: search_term-only strategy ===================

                    # ------------------------ other boards (Indeed/Google/LinkedIn/etc.) ------------------------
                    for pidx, pconf in enumerate(passes, start=1):
                        wm = (work_mode or "any").strip().lower()
                        is_remote_flag = None
                        if site != "linkedin":
                            if wm.startswith("remote"):
                                is_remote_flag = True
                            elif wm.startswith("on-site") or wm.startswith("onsite"):
                                is_remote_flag = False

                        req = {
                            "site": site,
                            "title": title_term,
                            "location_original": loc_for_all,
                            "location_normalized": loc_for_site,
                            "pass": pidx,
                            "hours_old": pconf.get("hours_old"),
                            "easy_apply": pconf.get("easy_apply"),
                            "is_remote": pconf.get("is_remote", is_remote_flag),
                            "job_type": pconf.get("job_type"),
                            "results_wanted": per_site_requested,
                            **({"country_indeed": country_indeed} if site in ("indeed", "glassdoor") else {}),
                            "linkedin_fetch_description": (linkedin_fetch_description if site == "linkedin" else False),
                            "proxies": proxies_to_use or [],
                            "user_agent": jobspy_user_agent or "",
                            "verbose": jobspy_verbose,
                        }
                        sink.write_json(f"query_{_safe(site)}_{pidx}_{_safe(title_term)}_{_safe(loc_for_site)}.json", req)

                        call_kwargs = dict(
                            site_name=[site],
                            location=loc_for_site,
                            results_wanted=per_site_requested,
                            linkedin_fetch_description=(linkedin_fetch_description if site == "linkedin" else False),
                            proxies=proxies_to_use,
                            user_agent=jobspy_user_agent,
                            verbose=jobspy_verbose,
                            easy_apply=req["easy_apply"] or False,
                        )
                        if site in ("indeed", "glassdoor"):
                            call_kwargs["country_indeed"] = country_indeed
                            call_kwargs.setdefault("distance", 50)

                        _isr2 = req.get("is_remote")
                        if isinstance(_isr2, bool):
                            call_kwargs["is_remote"] = _isr2

                        if site == "google":
                            call_kwargs["google_search_term"] = f"{title_term} {loc_for_site}"
                        else:
                            call_kwargs["search_term"] = title_term

                        call_kwargs["hours_old"] = req["hours_old"]

                        if site == "indeed" and req.get("job_type"):
                            call_kwargs["job_type"] = req["job_type"]

                        df = _scrape_with_retry(site, call_kwargs, max_retries=4, proxies_iter=proxy_iter)

                        if site == "google" and (df is None or df.empty):
                            alt_kwargs = call_kwargs.copy()
                            alt_kwargs["google_search_term"] = f"{title_term} jobs in {loc_for_site}"
                            df = _scrape_with_retry(site, alt_kwargs, max_retries=2, proxies_iter=proxy_iter)

                        if df is not None and not df.empty:
                            df = df.copy()
                            df["SEARCH_TITLE"] = title_term
                            df["SEARCH_LOCATION"] = loc_for_all
                            df["NORMALIZED_LOCATION"] = loc_for_site
                            sink.write_df(f"raw_{_safe(site)}_{pidx}_{_safe(title_term)}_{_safe(loc_for_site)}.csv", df)
                            frames.append(df)

                        time.sleep(max(0.0, float(per_site_delay) + random.uniform(0, 0.6)))

            else:
                # Non-sequential mode
                for site in boards:
                    passes = _build_site_passes(site, hours_old, work_mode, employment_type, linkedin_easy_apply)
                    per_site_requested = _site_results_cap(site, results_wanted)
                    proxies_to_use = [next(proxy_iter)] if proxy_iter else jobspy_proxies

                    for pidx, pconf in enumerate(passes, start=1):
                        call_kwargs = dict(
                            site_name=[site],
                            location=loc_for_all,
                            results_wanted=per_site_requested,
                            proxies=proxies_to_use,
                            user_agent=jobspy_user_agent,
                            verbose=jobspy_verbose,
                            easy_apply=pconf.get("easy_apply") or False,
                            hours_old=pconf.get("hours_old"),
                        )
                        if site in ("indeed", "glassdoor"):
                            call_kwargs["country_indeed"] = country_indeed
                            call_kwargs.setdefault("distance", 50)

                        wm = (work_mode or "any").strip().lower()
                        is_remote_flag = None
                        if site != "linkedin":
                            if wm.startswith("remote"):
                                is_remote_flag = True
                            elif wm.startswith("on-site") or wm.startswith("onsite"):
                                is_remote_flag = False
                        _isr3 = pconf.get("is_remote", is_remote_flag)
                        if isinstance(_isr3, bool):
                            call_kwargs["is_remote"] = _isr3

                        if pconf.get("job_type"):
                            call_kwargs["job_type"] = pconf["job_type"]

                        if site == "google":
                            call_kwargs["google_search_term"] = f"{title_term} {loc_for_all}"
                        else:
                            call_kwargs["search_term"] = title_term

                        df = _scrape_with_retry(site, call_kwargs, max_retries=3, proxies_iter=proxy_iter)

                        if site == "google" and (df is None or df.empty):
                            alt_kwargs = call_kwargs.copy()
                            alt_kwargs["google_search_term"] = f"{title_term} jobs in {loc_for_all}"
                            df = _scrape_with_retry(site, alt_kwargs, max_retries=2, proxies_iter=proxy_iter)

                        if df is not None and not df.empty:
                            df = df.copy()
                            df["SEARCH_TITLE"] = title_term
                            df["SEARCH_LOCATION"] = loc_for_all
                            df["NORMALIZED_LOCATION"] = loc_for_all
                            sink.write_df(f"raw_{_safe(site)}_{pidx}_{_safe(title_term)}_{_safe(loc_for_all)}.csv", df)
                            frames.append(df)

    # Avoid concat warning: remove empty frames
    frames = [f for f in frames if f is not None and not f.empty]
    if not frames:
        return pd.DataFrame()

    # Merge + normalize BEFORE dedupe
    merged = pd.concat(frames, ignore_index=True, sort=False)
    sink.write_df("merged_all.csv", merged)
    try:
        merged = normalize_jobpost_df(merged)
        sink.write_df("merged_all_normalized.csv", merged)
    except Exception as e:
        logging.warning("normalize_jobpost_df failed: %s", e)
    sink.write_json("counts_1_merged.json", _site_counts(merged))

    cleaned = dedupe_jobs(merged)
    sink.write_json("counts_2_deduped.json", _site_counts(cleaned))

    try:
        if hours_old is not None:
            cleaned = filter_by_hours(cleaned, hours_old, keep_unknown=True)
    except Exception as e:
        logging.warning("filter_by_hours failed: %s", e)
    sink.write_json("counts_3_hours.json", _site_counts(cleaned))

    try:
        cleaned = filter_by_work_mode(cleaned, work_mode or "any")
    except Exception as e:
        logging.warning("filter_by_work_mode failed: %s", e)
    try:
        cleaned = filter_by_experience(cleaned, min_years=min_experience, max_years=max_experience, keep_unknown=True)
    except Exception as e:
        logging.warning("filter_by_experience failed: %s", e)
    try:
        cleaned = filter_by_employment_type(cleaned, employment_type or "any")
    except Exception as e:
        logging.warning("filter_by_employment_type failed: %s", e)

    try:
        final_df = filter_title_contains_any(cleaned, titles, include_abbrevs=True) if strict_titles else cleaned
    except Exception as e:
        logging.warning("filter_title_contains_any failed (falling back, strict_titles=%s): %s", strict_titles, e)
        final_df = cleaned

    sink.write_df("filtered_all.csv", final_df)
    sink.write_json("counts_4_final.json", _site_counts(final_df))

    return final_df
