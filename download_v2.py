import csv
import os
import re
import tarfile
import tempfile
import time
import xml.etree.ElementTree as ET

import requests

# --------- Config ---------
CSV_FILE = "SB_publications/SB_publication_PMC.csv"   # <-- adjust if needed
OUTPUT_DIR = "PMC_PDFs"
NCBI_OA_API = "https://www.ncbi.nlm.nih.gov/pmc/utils/oa/oa.fcgi?id="
HEADERS = {
    "User-Agent": "PMC-OA-Downloader/1.0 (+you@example.com)"
}
MAX_RETRIES = 3
RETRY_SLEEP = 2  # seconds between retries
# --------------------------

os.makedirs(OUTPUT_DIR, exist_ok=True)

def _http_get(url, stream=False, timeout=30):
    """
    GET with simple retries.
    """
    last_err = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(url, headers=HEADERS, stream=stream, timeout=timeout)
            r.raise_for_status()
            return r
        except Exception as e:
            last_err = e
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_SLEEP)
    raise last_err

def _ftp_to_https(url: str) -> str:
    """
    Convert NCBI ftp link to https mirror for easy downloading.
    """
    if url.startswith("ftp://ftp.ncbi.nlm.nih.gov"):
        return url.replace("ftp://ftp.ncbi.nlm.nih.gov", "https://ftp.ncbi.nlm.nih.gov")
    return url

def _parse_oa_record(xml_text: str):
    """
    Return (pdf_url, tgz_url) from oa.fcgi XML for a single record, or (None, None).
    """
    root = ET.fromstring(xml_text)
    record = root.find(".//record")
    if record is None:
        return None, None
    pdf_el = record.find(".//link[@format='pdf']")
    tgz_el = record.find(".//link[@format='tgz']")
    pdf_url = _ftp_to_https(pdf_el.get("href")) if pdf_el is not None else None
    tgz_url = _ftp_to_https(tgz_el.get("href")) if tgz_el is not None else None
    return pdf_url, tgz_url

def _save_stream_to_file(resp, out_path, chunk=1024 * 1024):
    with open(out_path, "wb") as f:
        for part in resp.iter_content(chunk_size=chunk):
            if part:
                f.write(part)

def _choose_best_pdf(members):
    """
    Given tarfile members, choose the 'best' PDF:
    - Prefer the largest .pdf (usually the main article)
    """
    pdf_members = [m for m in members if m.isfile() and m.name.lower().endswith(".pdf")]
    if not pdf_members:
        return None
    # Pick largest
    pdf_members.sort(key=lambda m: (m.size or 0), reverse=True)
    return pdf_members[0]

def _extract_pdf_from_tgz(tgz_path, out_pdf_path):
    """
    Extract the best PDF from the TGZ archive and save to out_pdf_path.
    Returns True if saved, False otherwise.
    """
    try:
        with tarfile.open(tgz_path, "r:gz") as tf:
            best = _choose_best_pdf(tf.getmembers())
            if best is None:
                return False
            # Extract file-like and write out
            fobj = tf.extractfile(best)
            if fobj is None:
                return False
            with open(out_pdf_path, "wb") as out:
                out.write(fobj.read())
            return True
    except Exception:
        return False

def download_pmc_pdf(pmcid: str) -> bool:
    """
    Try to download PDF via OA API; if only TGZ exists, download and extract PDF.
    """
    pmcid = pmcid.upper().strip()
    if not pmcid.startswith("PMC"):
        pmcid = f"PMC{pmcid}"

    api_url = f"{NCBI_OA_API}{pmcid.replace('PMC','')}"
    try:
        api_resp = _http_get(api_url, stream=False, timeout=20)
    except Exception as e:
        print(f"[{pmcid}] OA API error: {e}")
        return False

    pdf_url, tgz_url = _parse_oa_record(api_resp.text)

    out_pdf = os.path.join(OUTPUT_DIR, f"{pmcid}.pdf")

    # Case 1: direct PDF available
    if pdf_url:
        try:
            r = _http_get(pdf_url, stream=True, timeout=60)
            _save_stream_to_file(r, out_pdf)
            print(f"[{pmcid}] Downloaded PDF via direct link.")
            return True
        except Exception as e:
            print(f"[{pmcid}] Failed direct PDF download: {e}")
            # fall through to TGZ if available

    # Case 2: only TGZ available
    if tgz_url:
        try:
            with tempfile.TemporaryDirectory() as td:
                tgz_path = os.path.join(td, f"{pmcid}.tar.gz")
                r = _http_get(tgz_url, stream=True, timeout=120)
                _save_stream_to_file(r, tgz_path)

                ok = _extract_pdf_from_tgz(tgz_path, out_pdf)
                if ok:
                    print(f"[{pmcid}] Extracted PDF from TGZ.")
                    return True
                else:
                    print(f"[{pmcid}] No PDF found inside TGZ.")
                    return False
        except Exception as e:
            print(f"[{pmcid}] TGZ download/extract error: {e}")
            return False

    print(f"[{pmcid}] No OA PDF or TGZ available (not in OA subset?).")
    return False

def iter_csv_rows(csv_path):
    """
    Iterate rows, yielding a PMCID string found in common columns or any value.
    """
    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        # Try DictReader; if the CSV has no header, fall back to simple reader
        sample = f.read(2048)
        f.seek(0)
        has_header = csv.Sniffer().has_header(sample)
        if has_header:
            reader = csv.DictReader(f)
            for row in reader:
                if not row:
                    continue
                # Pick likely column names or first non-empty value
                candidates = []
                for k in row.keys():
                    v = (row.get(k) or "").strip()
                    if v:
                        candidates.append(v)
                yield from _extract_pmcids_from_values(candidates)
        else:
            reader = csv.reader(f)
            for row in reader:
                vals = [v.strip() for v in row if v and v.strip()]
                yield from _extract_pmcids_from_values(vals)

def _extract_pmcids_from_values(values):
    """
    From a list of strings (URLs or IDs), yield PMCID strings we can act on.
    """
    seen = set()
    for val in values:
        m = re.search(r"(PMC\d+)", val, flags=re.IGNORECASE)
        if m:
            pmcid = m.group(1).upper()
            if pmcid not in seen:
                seen.add(pmcid)
                yield pmcid

if __name__ == "__main__":
    ok_count = 0
    fail_count = 0
    try:
        for pmcid in iter_csv_rows(CSV_FILE):
            ok = download_pmc_pdf(pmcid)
            ok_count += int(ok)
            fail_count += int(not ok)
    except FileNotFoundError:
        print(f"CSV file not found: {CSV_FILE}")
    finally:
        print(f"\nDone. Success: {ok_count} | Failed: {fail_count}")
