import pandas
import re
import os
import shutil
import argparse

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

def _sanitize_filename(name: str, max_len: int = 200) -> str:
	"""Make a filesystem-safe filename from title."""
	# remove or replace characters invalid on Windows and generally problematic
	invalid = r'[<>:"/\\|?*\n\r\t]'
	out = re.sub(invalid, "_", name).strip()
	# collapse multiple spaces/underscores
	out = re.sub(r'[_\s]+', " ", out).strip()
	# shorten
	if len(out) > max_len:
		out = out[:max_len].rstrip()
	# avoid empty name
	return out or "untitled"

def build_pmcid_title_map(df: pandas.DataFrame):
	"""Return dict pmcid -> title using DataFrame (searches all cells for PMCIDs)."""
	mapping = {}
	for _, row in df.iterrows():
		# gather candidate strings from the row (Title, Link, any other)
		values = [str(v) for v in row.tolist() if pandas.notna(v)]
		# find pmcids in the combined row
		for pmcid in _extract_pmcids_from_values(values):
			# prefer Title column if present
			title = None
			if "Title" in df.columns and pandas.notna(row.get("Title")):
				title = str(row["Title"])
			else:
				# fallback: use first non-url value or the link text
				for v in values:
					if not v.lower().startswith("http"):
						title = v
						break
				if not title and values:
					title = values[0]
			mapping[pmcid] = title or pmcid
	return mapping

def copy_pdfs_for_map(pmc_map, pdf_dir, out_dir, dry_run=False):
	os.makedirs(out_dir, exist_ok=True)
	missing = []
	copied = 0
	for pmcid, title in pmc_map.items():
		src_name = f"{pmcid.upper()}.pdf"
		src_path = os.path.join(pdf_dir, src_name)
		if not os.path.exists(src_path):
			missing.append(pmcid)
			continue
		safe_title = _sanitize_filename(title)
		dest_name = f"{safe_title}.pdf"
		dest_path = os.path.join(out_dir, dest_name)
		# if exists, append suffix
		if os.path.exists(dest_path):
			base, ext = os.path.splitext(dest_name)
			i = 1
			while True:
				new_name = f"{base} ({i}){ext}"
				new_path = os.path.join(out_dir, new_name)
				if not os.path.exists(new_path):
					dest_path = new_path
					break
				i += 1
		if dry_run:
			print(f"Would copy: {src_path} -> {dest_path}")
		else:
			shutil.copy2(src_path, dest_path)
			copied += 1
			print(f"Copied: {src_name} -> {os.path.basename(dest_path)}")
	return copied, missing

def main():
	parser = argparse.ArgumentParser(description="Rename/copy PMC PDFs from PMCID-based names to titles from CSV")
	parser.add_argument("--csv", "-c", default="SB_publication_PMC.csv", help="CSV file with Title and Link columns")
	parser.add_argument("--pdf-dir", "-p", default="PMC_PDFs", help="Directory containing {PMCID}.pdf files")
	parser.add_argument("--out-dir", "-o", default="named_pdfs", help="Output directory for renamed PDFs")
	parser.add_argument("--dry-run", action="store_true", help="Don't actually copy, just show actions")
	args = parser.parse_args()

	try:
		df = pandas.read_csv(args.csv, encoding="utf-8-sig")
	except Exception as e:
		print(f"Failed to read CSV '{args.csv}': {e}")
		return

	pmc_map = build_pmcid_title_map(df)
	print(f"Found {len(pmc_map)} PMCIDs in CSV.")

	copied, missing = copy_pdfs_for_map(pmc_map, args.pdf_dir, args.out_dir, dry_run=args.dry_run)

	# # write missing list
	# missing_file = os.path.join(args.out_dir, "missing_pmcids.txt")
	# try:
	# 	with open(missing_file, "w", encoding="utf-8") as mf:
	# 		mf.write(f"# Missing PMCIDs - generated\n")
	# 		for p in missing:
	# 			mf.write(p + "\n")
	# 	print(f"Copied: {copied}. Missing: {len(missing)}. Missing list: {missing_file}")
	# except Exception as e:
	# 	print(f"Failed to write missing file: {e}")

if __name__ == "__main__":
	main()