import csv
import os
import re
import tarfile
import tempfile
import time
import xml.etree.ElementTree as ET

import requests
from tqdm import tqdm

# --------- Config ---------
CSV_FILE = "SB_publication_PMC.csv"   # <-- adjust if needed
OUTPUT_DIR = "PMC_PDFs"
NCBI_OA_API_BASE = "https://www.ncbi.nlm.nih.gov/pmc/utils/oa/oa.fcgi?id="
# Add tool/email for good API citizenship
OA_API_SUFFIX = "&tool=pmc-oa-downloader&email=you@example.com"
HEADERS = {"User-Agent": "PMC-OA-Downloader/1.1 (+you@example.com)"}
MAX_RETRIES = 3
RETRY_SLEEP = 2  # seconds

# New: max concurrent requests
MAX_CONCURRENT = 5
# --------------------------

os.makedirs(OUTPUT_DIR, exist_ok=True)

# New: shared session for connection pooling across threads
SESSION = requests.Session()
SESSION.headers.update(HEADERS)

def _http_get(url, stream=False, timeout=30):
    """GET with simple retries using shared session."""
    last_err = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = SESSION.get(url, stream=stream, timeout=timeout)
            r.raise_for_status()
            return r
        except Exception as e:
            last_err = e
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_SLEEP)
    raise last_err

def _ftp_to_https(url: str) -> str:
    """Convert NCBI ftp link to https mirror for easy downloading."""
    if url.startswith("ftp://ftp.ncbi.nlm.nih.gov"):
        return url.replace("ftp://ftp.ncbi.nlm.nih.gov", "https://ftp.ncbi.nlm.nih.gov")
    return url

def _parse_oa_record(xml_text: str):
    """Return (pdf_url, tgz_url) from oa.fcgi XML for a single record, or (None, None)."""
    root = ET.fromstring(xml_text)
    record = root.find(".//record")
    if record is None:
        return None, None
    pdf_el = record.find(".//link[@format='pdf']")
    tgz_el = record.find(".//link[@format='tgz']")
    pdf_url = _ftp_to_https(pdf_el.get("href")) if pdf_el is not None else None
    tgz_url = _ftp_to_https(tgz_el.get("href")) if tgz_el is not None else None
    return pdf_url, tgz_url

def _save_stream_to_file(resp, out_path, desc):
    """Stream response to file with tqdm progress."""
    total = int(resp.headers.get("content-length", 0))
    chunk = 1024 * 1024  # 1MB
    with open(out_path, "wb") as f, tqdm(
        total=total if total > 0 else None,
        unit="B",
        unit_scale=True,
        desc=desc,
        leave=False
    ) as pbar:
        for part in resp.iter_content(chunk_size=chunk):
            if part:
                f.write(part)
                if total > 0:
                    pbar.update(len(part))

def _choose_best_pdf(members):
    """Pick the largest .pdf (usually the main article)."""
    pdf_members = [m for m in members if m.isfile() and m.name.lower().endswith(".pdf")]
    if not pdf_members:
        return None
    pdf_members.sort(key=lambda m: (m.size or 0), reverse=True)
    return pdf_members[0]

def _extract_pdf_from_tgz(tgz_path, out_pdf_path):
    """Extract the best PDF from the TGZ archive and save to out_pdf_path."""
    try:
        with tarfile.open(tgz_path, "r:gz") as tf:
            best = _choose_best_pdf(tf.getmembers())
            if best is None:
                return False
            fobj = tf.extractfile(best)
            if fobj is None:
                return False
            with open(out_pdf_path, "wb") as out:
                out.write(fobj.read())
            return True
    except Exception:
        return False

def download_pmc_pdf(pmcid: str) -> bool:
    """Try PDF link first; else TGZ -> extract PDF. Skip if already downloaded."""
    pmcid = pmcid.upper().strip()
    if not pmcid.startswith("PMC"):
        pmcid = f"PMC{pmcid}"

    out_pdf = os.path.join(OUTPUT_DIR, f"{pmcid}.pdf")
    if os.path.exists(out_pdf) and os.path.getsize(out_pdf) > 0:
        print(f"[{pmcid}] Skipped (already downloaded).")
        return True

    api_url = f"{NCBI_OA_API_BASE}{pmcid.replace('PMC','')}{OA_API_SUFFIX}"
    try:
        api_resp = _http_get(api_url, stream=False, timeout=20)
    except Exception as e:
        print(f"[{pmcid}] OA API error: {e}")
        return False

    pdf_url, tgz_url = _parse_oa_record(api_resp.text)

    # Case 1: direct PDF
    if pdf_url:
        try:
            r = _http_get(pdf_url, stream=True, timeout=120)
            _save_stream_to_file(r, out_pdf, desc=f"{pmcid} (PDF)")
            print(f"[{pmcid}] Downloaded PDF via direct link.")
            return True
        except Exception as e:
            print(f"[{pmcid}] Failed direct PDF download: {e}")
            # fall back to TGZ if available

    # Case 2: TGZ
    if tgz_url:
        try:
            with tempfile.TemporaryDirectory() as td:
                tgz_path = os.path.join(td, f"{pmcid}.tar.gz")
                r = _http_get(tgz_url, stream=True, timeout=300)
                _save_stream_to_file(r, tgz_path, desc=f"{pmcid} (TGZ)")
                ok = _extract_pdf_from_tgz(tgz_path, out_pdf)
                if ok:
                    print(f"[{pmcid}] Extracted PDF from TGZ.")
                    return True
                else:
                    print(f"[{pmcid}] No PDF found inside TGZ.")
                    # If no PDF inside, remove empty file if created
                    if os.path.exists(out_pdf) and os.path.getsize(out_pdf) == 0:
                        try: os.remove(out_pdf)
                        except Exception: pass
                    return False
        except Exception as e:
            print(f"[{pmcid}] TGZ download/extract error: {e}")
            return False

    print(f"[{pmcid}] No OA PDF or TGZ available (not in OA subset?).")
    return False

def _extract_pmcids_from_values(values):
    """Yield PMCIDs from a list of strings (URLs or IDs)."""
    seen = set()
    for val in values:
        m = re.search(r"(PMC\d+)", val, flags=re.IGNORECASE)
        if m:
            pmcid = m.group(1).upper()
            if pmcid not in seen:
                seen.add(pmcid)
                yield pmcid

def load_pmcids(csv_path):
    """Load all PMCIDs from CSV into a list (handles header/no header)."""
    pmcids = []
    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        sample = f.read(2048)
        f.seek(0)
        has_header = csv.Sniffer().has_header(sample)
        if has_header:
            reader = csv.DictReader(f)
            for row in reader:
                if not row:
                    continue
                vals = [(row.get(k) or "").strip() for k in row.keys()]
                pmcids.extend(list(_extract_pmcids_from_values(vals)))
        else:
            reader = csv.reader(f)
            for row in reader:
                vals = [v.strip() for v in row if v and v.strip()]
                pmcids.extend(list(_extract_pmcids_from_values(vals)))
    # Deduplicate but keep order
    seen = set()
    unique = []
    for p in pmcids:
        if p not in seen:
            seen.add(p)
            unique.append(p)
    return unique

# Add concurrency imports and config
from concurrent.futures import ThreadPoolExecutor, as_completed

if __name__ == "__main__":
    try:
        pmcids = load_pmcids(CSV_FILE)
    except FileNotFoundError:
        print(f"CSV file not found: {CSV_FILE}")
        pmcids = []

    ok_count = 0
    fail_count = 0
    # New: collect missing PMCIDs
    missing_pmcids = []

    # Prepare list skipping already-downloaded files to reduce submitted tasks
    to_process = []
    for pmcid in pmcids:
        out_pdf = os.path.join(OUTPUT_DIR, f"{pmcid.upper()}.pdf")
        if os.path.exists(out_pdf) and os.path.getsize(out_pdf) > 0:
            print(f"[{pmcid}] Skipped (already downloaded).")
            ok_count += 1
        else:
            to_process.append(pmcid)

    with tqdm(total=len(pmcids), desc="Overall", unit="article") as master:
        # advance the bar for already-skipped items
        master.update(len(pmcids) - len(to_process))

        if to_process:
            with ThreadPoolExecutor(max_workers=MAX_CONCURRENT) as ex:
                future_to_pmc = {ex.submit(download_pmc_pdf, pmcid): pmcid for pmcid in to_process}
                for fut in as_completed(future_to_pmc):
                    pmcid = future_to_pmc[fut]
                    try:
                        ok = fut.result()
                    except Exception as e:
                        print(f"[{pmcid}] Exception in worker: {e}")
                        ok = False
                    if not ok:
                        missing_pmcids.append(pmcid)
                    ok_count += int(bool(ok))
                    fail_count += int(not ok)
                    master.update(1)

    # Write missing PMCIDs to file (always produce the log, may be empty)
    missing_file = os.path.join(OUTPUT_DIR, "missing_pmcids.txt")
    try:
        with open(missing_file, "w", encoding="utf-8") as mf:
            mf.write(f"# Missing PMCIDs - generated: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
            mf.write(f"# Requested: {len(pmcids)}  Succeeded: {ok_count}  Failed: {fail_count}\n")
            for p in missing_pmcids:
                mf.write(p + "\n")
        print(f"Missing PMCIDs logged to: {os.path.abspath(missing_file)}")
    except Exception as e:
        print(f"Failed to write missing PMCIDs file: {e}")

    print(f"\nDone. Success: {ok_count} | Failed: {fail_count} | Output: {os.path.abspath(OUTPUT_DIR)}")
