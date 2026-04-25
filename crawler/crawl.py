import argparse
import json
import logging
import logging.handlers
import os
import random
import time

import requests

BASE_URL = "https://israeldrugs.health.gov.il/GovServiceList/IDRServer"
DELAY = float(os.getenv("CRAWL_DELAY", "2"))
JITTER = 0.4
MAX_RETRIES = 5
TIMEOUT = 60
THROTTLE_BACKOFF = 60  # seconds to wait after a timeout before retrying

_ROOT = os.path.join(os.path.dirname(__file__), "..")
STATE_FILE = os.path.join(_ROOT, "state", "progress.json")
OUTPUT_FILE = os.path.join(_ROOT, "catalog.json")
LOG_FILE = os.path.join(_ROOT, "logs", "crawl.log")

HEADERS = {
    "Content-Type": "application/json",
    "Accept": "application/json",
    "User-Agent": "mabasal-crawler/1.0",
}

SESSION = requests.Session()
SESSION.headers.update(HEADERS)

log = logging.getLogger("crawler")


def setup_logging(verbose: bool = False) -> None:
    os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
    log.setLevel(logging.DEBUG)

    fmt = logging.Formatter("%(asctime)s  %(levelname)-8s  %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

    fh = logging.handlers.RotatingFileHandler(LOG_FILE, maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)

    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG if verbose else logging.INFO)
    ch.setFormatter(fmt)

    log.addHandler(fh)
    log.addHandler(ch)


def rate_limited_post(path: str, payload: dict) -> dict:
    url = BASE_URL + path
    sleep_for = DELAY + random.uniform(-JITTER, JITTER)
    time.sleep(max(sleep_for, 0.5))

    body_hint = ", ".join(f"{k}={v!r}" for k, v in payload.items() if v not in (None, False, 0, ""))

    for attempt in range(MAX_RETRIES):
        log.debug("POST %s  %s", path, body_hint)
        try:
            resp = SESSION.post(url, json=payload, timeout=TIMEOUT)
            log.debug("  -> status=%s  bytes=%d", resp.status_code, len(resp.content))

            if resp.status_code != 200:
                log.warning("HTTP %s on attempt %d for %s", resp.status_code, attempt + 1, path)
                time.sleep(THROTTLE_BACKOFF * (attempt + 1))
                continue

            try:
                return resp.json()
            except json.JSONDecodeError as exc:
                preview = resp.text[:500].replace("\n", " ")
                log.error(
                    "JSON decode failed on attempt %d for %s: %s | body preview: %s",
                    attempt + 1, path, exc, preview,
                )
                time.sleep(THROTTLE_BACKOFF * (attempt + 1))

        except requests.Timeout:
            wait = THROTTLE_BACKOFF * (attempt + 1)
            log.warning("Timeout on attempt %d for %s — waiting %ds before retry", attempt + 1, path, wait)
            time.sleep(wait)

        except requests.RequestException as exc:
            log.error("Request error on attempt %d for %s: %s", attempt + 1, path, exc)
            time.sleep(THROTTLE_BACKOFF * (attempt + 1))

    raise RuntimeError(f"Failed after {MAX_RETRIES} attempts on {path} — check {LOG_FILE} for details")


def load_state() -> dict:
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"search_done": False, "drugs": {}, "page": 1}


def save_state(state: dict) -> None:
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    tmp = STATE_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False)
    os.replace(tmp, STATE_FILE)


SEARCH_ADV_BASE = {
    "val": "", "veterinary": False, "cytotoxic": False, "prescription": False,
    "isGSL": False, "healthServices": False, "isPeopleMedication": False,
    "fromCanceledDrags": None, "toCanceledDrags": None,
    "fromUpdateInstructions": None, "toUpdateInstructions": None,
    "fromNewDrags": None, "toNewDrags": None,
    "newDragsDrop": 0, "orderBy": 0, "types": "0",
}


def run_phase1(state: dict) -> None:
    if state["search_done"]:
        log.info("Phase 1: already complete, skipping.")
        return

    log.info("Phase 1: enumerating all drugs via SearchByAdv…")
    total_pages = None

    while True:
        page = state["page"]
        if total_pages and page > total_pages:
            break

        data = rate_limited_post("/SearchByAdv", {**SEARCH_ADV_BASE, "pageIndex": page})

        # SearchByAdv returns a plain list (not a wrapped object)
        results = data if isinstance(data, list) else data.get("results", [])
        if total_pages is None:
            total_pages = results[0].get("pages", 1) if results else 1

        new_count = 0
        for drug in results:
            reg_num = drug.get("dragRegNum")
            if reg_num and reg_num not in state["drugs"]:
                state["drugs"][reg_num] = drug
                new_count += 1

        log.info("page %d/%d  +%d new drugs  (%d total)", page, total_pages, new_count, len(state["drugs"]))

        state["page"] = page + 1
        save_state(state)

        if page >= total_pages:
            break

    state["search_done"] = True
    save_state(state)
    log.info("Phase 1 complete: %d drugs found.", len(state["drugs"]))


def run_phase2(state: dict) -> None:
    pending = [reg for reg, drug in state["drugs"].items() if "detail" not in drug]
    total = len(state["drugs"])
    done = total - len(pending)

    if not pending:
        log.info("Phase 2: already complete, skipping.")
        return

    log.info("Phase 2: fetching detail for %d drugs (%d already done)…", len(pending), done)

    for i, reg_num in enumerate(pending, start=1):
        data = rate_limited_post("/GetSpecificDrug", {"dragRegNum": reg_num})
        state["drugs"][reg_num]["detail"] = data.get("data") or data
        save_state(state)

        if i % 50 == 0 or i == len(pending):
            log.info("%d/%d  (%s)", done + i, total, reg_num)

    log.info("Phase 2 complete.")


def write_output(state: dict) -> None:
    drugs = list(state["drugs"].values())
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(drugs, f, ensure_ascii=False, indent=2)
    log.info("Wrote %d drugs to %s", len(drugs), OUTPUT_FILE)


def main() -> None:
    parser = argparse.ArgumentParser(description="Israeli drug registry crawler")
    parser.add_argument("-v", "--verbose", action="store_true", help="Log every request to console")
    args = parser.parse_args()

    setup_logging(verbose=args.verbose)
    log.info("=== crawl started ===")
    state = load_state()
    run_phase1(state)
    run_phase2(state)
    write_output(state)
    log.info("=== done: %d drugs ===", len(state["drugs"]))


if __name__ == "__main__":
    main()
