import csv, requests, os
import re
import xml.etree.ElementTree as ET

# Path to CSV file containing PMCID list. Assuming the CSV has either a column with 
# PMCID values or URLs containing the PMCID.
csv_file = "SB_publications/SB_publication_PMC.csv"
output_dir = "PMC_PDFs"
os.makedirs(output_dir, exist_ok=True)

# Function to fetch PDF for a given PMCID
def download_pmc_pdf(pmcid):
    # Ensure the ID is in the correct format (strip "PMC" prefix for query if present)
    pmcid_str = str(pmcid)
    query_id = pmcid_str.replace("PMC", "")  # numeric part for API
    url = f"https://www.ncbi.nlm.nih.gov/pmc/utils/oa/oa.fcgi?id={query_id}"
    try:
        res = requests.get(url, timeout=10)
        res.raise_for_status()
    except Exception as e:
        print(f"Error querying API for {pmcid}: {e}")
        return False
    # Parse XML response
    root = ET.fromstring(res.text)
    record = root.find(".//record")
    if record is None:
        print(f"No open-access PDF available for {pmcid}.")
        return False
    # Find PDF link in the response
    link = record.find(".//link[@format='pdf']")
    if link is None:
        print(f"No PDF link found for {pmcid} (it might not be OA).")
        return False
    pdf_url = link.get("href")
    # The href is an FTP link. Convert to HTTPS for downloading
    if pdf_url.startswith("ftp://"):
        pdf_url = pdf_url.replace("ftp://ftp.ncbi.nlm.nih.gov", "https://ftp.ncbi.nlm.nih.gov")
    # Download the PDF content
    try:
        pdf_res = requests.get(pdf_url, timeout=20)
        pdf_res.raise_for_status()
    except Exception as e:
        print(f"Failed to download PDF for {pmcid}: {e}")
        return False
    # Save PDF to file
    filename = pmcid_str + ".pdf"
    filepath = os.path.join(output_dir, filename)
    with open(filepath, "wb") as f:
        f.write(pdf_res.content)
    print(f"Downloaded {pmcid} -> {filename}")
    return True

# Read the CSV and process PMCIDs
try:
    with open(csv_file, newline='', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Prefer a 'Link' column (case-insensitive)
            link = None
            if row is None:
                continue
            # Get link value (handle different casings)
            for key in ('Link', 'link'):
                if key in row and row[key]:
                    link = row[key].strip()
                    break
            # Fallback: maybe PMCID is in the first column
            if not link and len(row) > 0:
                # take the first non-empty value
                for v in row.values():
                    if v and v.strip():
                        link = v.strip()
                        break
            if not link:
                print("Skipping row without link/pmcid.")
                continue
            # Extract PMCID using regex (handles URLs and plain 'PMC12345' tokens)
            m = re.search(r'(PMC\d+)', link, re.IGNORECASE)
            if m:
                pmcid = m.group(1).upper()
                download_pmc_pdf(pmcid)
            else:
                print(f"Could not find PMCID in link/value: {link}")
except FileNotFoundError:
    print(f"CSV file not found: {csv_file}")
except Exception as e:
    print(f"Error reading CSV: {e}")
