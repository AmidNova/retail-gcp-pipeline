"""Download the full Online Retail II dataset (UCI) and write it as one CSV.

The UCI archive ships an .xlsx with two sheets (2009-2010 and 2010-2011).
Opening it in Excel and saving as CSV silently truncates it to 1,048,575 rows,
so this script reads both sheets with pandas and writes the CSV directly.

Usage:
    python scripts/download_dataset.py [--output include/dataset/online_retail.csv]
"""

import argparse
import io
import tempfile
import urllib.request
import zipfile
from pathlib import Path

import pandas as pd

UCI_URL = "https://archive.ics.uci.edu/static/public/502/online+retail+ii.zip"
XLSX_NAME = "online_retail_II.xlsx"
OUTPUT_COLUMNS = [
    "Invoice",
    "StockCode",
    "Description",
    "Quantity",
    "InvoiceDate",
    "Price",
    "Customer_id",
    "Country",
]


def download_xlsx(target_dir: Path) -> Path:
    print(f"Downloading {UCI_URL} ...")
    with urllib.request.urlopen(UCI_URL, timeout=300) as response:
        archive = zipfile.ZipFile(io.BytesIO(response.read()))
    archive.extract(XLSX_NAME, target_dir)
    return target_dir / XLSX_NAME


def to_dataframe(xlsx_path: Path) -> pd.DataFrame:
    print(f"Reading both sheets of {xlsx_path.name} (takes a few minutes) ...")
    sheets = pd.read_excel(
        xlsx_path,
        sheet_name=None,
        dtype={"Invoice": str, "StockCode": str, "Description": str, "Country": str},
    )
    frame = pd.concat(sheets.values(), ignore_index=True)
    frame = frame.rename(columns={"Customer ID": "Customer_id"})
    frame["InvoiceDate"] = pd.to_datetime(frame["InvoiceDate"]).dt.strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    frame["Customer_id"] = frame["Customer_id"].astype("Int64")
    return frame[OUTPUT_COLUMNS]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--output", default="include/dataset/online_retail.csv", type=Path
    )
    args = parser.parse_args()

    with tempfile.TemporaryDirectory() as tmp:
        frame = to_dataframe(download_xlsx(Path(tmp)))

    args.output.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(args.output, index=False)
    print(
        f"Wrote {len(frame):,} rows to {args.output} "
        f"({frame['InvoiceDate'].min()} -> {frame['InvoiceDate'].max()})"
    )


if __name__ == "__main__":
    main()
