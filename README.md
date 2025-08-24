# Job Scraper — JobSpy Only

A Streamlit app and CLI automation that scrape jobs from **Indeed, Glassdoor, Google (Jobs)** (and optionally LinkedIn) using **python-jobspy**, apply filters (hours, work mode, experience, employment type), de-duplicate, export CSV, and **email results** via SMTP (Brevo/Gmail/etc.). Includes a nightly automation runner and ready-to-use GitHub Actions workflow to send results **daily at 5 PM Pacific**.

> **Heads-up:** This project uses **`python-jobspy`**, not `jobspy`. Installing the wrong package will cause imports to fail.

---

## Features

- Streamlit UI for interactive searches  
- Multiple locations, titles, and job boards  
- Hours window (e.g., last 24/36/48 hours)  
- Work mode: Remote / On-site / Hybrid  
- Experience range filter (keeps “unknown” years by default)  
- Employment type: Full-time / Contract / Part-time / Internship  
- CSV export + one-click **Email results** (SMTP)  
- CLI **automation_runner.py** with named profiles (Yoshitha, Shazia, Ruthvej)  
- Debug artifacts for each scrape (queries, raw CSV per board, merged, filtered)  
- GitHub Actions workflow to run and email **every day at 5 PM PT**  

---

## Project Structure (key files)

app/
app.py # Streamlit UI
src/
services/
search_service.py # Orchestrates multi-board searches + debug outputs
providers/
jobspy_provider.py # Thin wrapper over python-jobspy's scrape_jobs
utils/
filtering.py # Filters (hours, titles, experience, employment type, etc.)
dedupe.py # De-duplication heuristics
normalize_jobpost.py # Normalizes JobSpy output columns
automation_runner.py # CLI profiles + email send
requirements.txt
README.md


---

## Install

```bash
# macOS / Linux
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements.txt

Example: ./.venv/bin/python automation_runner.py --profile shazia --dry-run --debug