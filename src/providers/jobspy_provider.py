# src/providers/jobspy_provider.py
from __future__ import annotations

import os
import logging
from typing import Any, Dict, List, Optional

import pandas as pd
from jobspy import scrape_jobs

log = logging.getLogger("JobScraper.JobSpyProvider")


def _ensure_list(x) -> List[str]:
    if x is None:
        return []
    if isinstance(x, (list, tuple)):
        return list(x)
    return [x]


def scrape_with_jobspy(**kwargs) -> pd.DataFrame:
    """
    Thin, safe wrapper around jobspy.scrape_jobs.

    - Normalizes `site_name` to a list and validates it.
    - For Google Jobs, maps `search_term` -> `google_search_term` and
      ensures we don't pass both.
    - Prunes fields that JobSpy's Pydantic model rejects when they are None
      (e.g., is_remote=None).
    - Cleans proxies and user-agent inputs.
    - Optionally uses a custom CA bundle if JOBSPY_CA_CERT (or explicit ca_cert) is present.
    - Casts/clamps results_wanted to a reasonable integer range.
    - Returns an empty DataFrame on None result, otherwise the DataFrame from JobSpy.

    Parameters are passed through as-is to JobSpy after cleanup.
    """
    # --- site_name normalization ------------------------------------------------
    sites = _ensure_list(kwargs.get("site_name"))
    if not sites:
        raise ValueError("scrape_with_jobspy: `site_name` is required (e.g., ['indeed']).")
    sites_lower = [str(s).lower() for s in sites]
    kwargs["site_name"] = sites  # keep original case if any

    # --- defaults ---------------------------------------------------------------
    kwargs.setdefault("verbose", 2)
    kwargs.setdefault("description_format", "markdown")
    # results_wanted clamped below
    kwargs.setdefault("country_indeed", "USA")

    # --- results_wanted: cast + clamp ------------------------------------------
    rw = kwargs.get("results_wanted", 50)
    try:
        rw = int(rw)
    except Exception:
        rw = 50
    # hard cap to prevent accidental huge pulls; service may have finer caps
    kwargs["results_wanted"] = max(1, min(rw, 2000))

    # --- Google: enforce google_search_term ------------------------------------
    # If any site is 'google', prefer `google_search_term` and drop `search_term`
    if "google" in sites_lower:
        if "google_search_term" not in kwargs and "search_term" in kwargs:
            kwargs["google_search_term"] = kwargs["search_term"]
        # Avoid passing both; JobSpy expects google_search_term specifically
        kwargs.pop("search_term", None)

    # --- Optional CA bundle (corporate proxies) --------------------------------
    ca_cert = kwargs.get("ca_cert") or os.getenv("JOBSPY_CA_CERT")
    if ca_cert:
        if os.path.exists(ca_cert):
            kwargs["ca_cert"] = ca_cert
        else:
            log.warning("JOBSPY_CA_CERT set but file not found: %s", ca_cert)
            kwargs.pop("ca_cert", None)

    # --- Clean user-agent -------------------------------------------------------
    ua = kwargs.get("user_agent")
    if not ua or not str(ua).strip():
        kwargs.pop("user_agent", None)

    # --- Clean proxies (accept str or list; drop blanks) ------------------------
    proxies = kwargs.get("proxies")
    if proxies:
        if not isinstance(proxies, (list, tuple)):
            proxies = [proxies]
        proxies = [p for p in proxies if isinstance(p, str) and p.strip()]
        if proxies:
            kwargs["proxies"] = proxies
        else:
            kwargs.pop("proxies", None)
    else:
        kwargs.pop("proxies", None)

    # --- Prune None for Pydantic-strict fields ---------------------------------
    # (Recent JobSpy versions reject is_remote=None, etc.)
    for key in (
        "is_remote",
        "hours_old",
        "job_type",
        "linkedin_fetch_description",
        "easy_apply",
        "offset",
    ):
        if key in kwargs and kwargs[key] is None:
            kwargs.pop(key)

    # --- Call JobSpy ------------------------------------------------------------
    try:
        df = scrape_jobs(**kwargs)
        if df is None:
            return pd.DataFrame()
        # Ensure DataFrame return type (JobSpy should already return one)
        if not isinstance(df, pd.DataFrame):
            log.warning("JobSpy returned non-DataFrame type: %s", type(df))
            return pd.DataFrame(df)
        return df
    except Exception as e:
        log.error(
            "JobSpy scrape failed (sites=%s, location=%s): %s",
            sites, kwargs.get("location"), e, exc_info=False
        )
        # Re-raise so caller can retry / write debug artifacts
        raise
