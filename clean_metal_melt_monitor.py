import sys
import pandas as pd
import numpy as np
from pathlib import Path

# dependency check 
try:
    import openpyxl  
except ImportError:
    sys.exit("\n[ERROR] openpyxl not installed.  Fix: pip install openpyxl\n")
BASE_DIR        = Path(__file__).parent.resolve()
INTENSITY_FILE  = BASE_DIR / "ECUK_2025_Intensity_tables.xlsx"
EUROSTAT_FILE   = BASE_DIR / "estat_nrg_ind_eff_en.csv"
OUTPUT_DIR      = BASE_DIR / "cleaned_data"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

YEAR_MIN = 2005
YEAR_MAX = 2024

EU_GEOS = ["EU27_2020", "DE", "FR", "IT", "ES", "PL", "NL"]

GEO_NAME_MAP = {
    "UK":        "United Kingdom",
    "EU27_2020": "EU27",
    "DE":        "Germany",
    "FR":        "France",
    "IT":        "Italy",
    "ES":        "Spain",
    "PL":        "Poland",
    "NL":        "Netherlands",
}

def to_numeric_safe(series: pd.Series) -> pd.Series:
    """Coerce a mixed-type column to float, turning sentinel strings into NaN."""
    SENTINELS = {"x", "..", ":", "nan", "n/a", "-", ""}
    return pd.to_numeric(
        series.astype(str)
              .str.replace(",", "", regex=False)
              .str.strip()
              .replace(SENTINELS, np.nan),
        errors="coerce",
    )


def _find_header_row(df: pd.DataFrame, needle: str, search_rows: int = 40) -> int | None:
    """Return the first row index (within search_rows) that contains needle (case-insensitive)."""
    needle_lower = needle.lower()
    for i in range(min(search_rows, len(df))):
        row_str = " ".join(df.iloc[i].fillna("").astype(str)).lower()
        if needle_lower in row_str:
            return i
    return None


def _find_sector_col(df: pd.DataFrame, row: int, sector: str) -> int | None:
    """Return the column index where sector label appears in the given row."""
    sector_lower = sector.lower()
    for j, val in enumerate(df.iloc[row]):
        if str(val).strip().lower() == sector_lower:
            return j
    return None

def parse_ecuk_table_i4(
    df_raw: pd.DataFrame,
    sector_name: str = "Iron steel, non-ferrous metals",
) -> pd.DataFrame:
    df_str = df_raw.astype(str)
    hdr_row = _find_header_row(df_str, sector_name.split(",")[0])   # "Iron steel"
    if hdr_row is None:
        hdr_row = _find_header_row(df_str, "iron")
    if hdr_row is None:
        raise ValueError(
            f"Could not find sector header row for '{sector_name}' in Table I4.\n"
            "Check that the sheet name and sector label match the workbook exactly."
        )
    start_col = _find_sector_col(df_str, hdr_row, sector_name)
    if start_col is None:
        for j, val in enumerate(df_str.iloc[hdr_row]):
            if "iron" in str(val).lower():
                start_col = j
                break
    if start_col is None:
        raise ValueError(
            f"Could not locate column for sector '{sector_name}' "
            f"in header row {hdr_row}.\n"
            f"Row contents: {df_str.iloc[hdr_row].tolist()}"
        )
    end_col = min(start_col + 7, df_raw.shape[1])
    block   = df_raw.iloc[:, start_col:end_col].copy()
    n_cols  = block.shape[1]

    base_col_names = [
        "consumption_ktoe",
        "output",
        "consumption_per_unit_output",
        "consumption_index_2000_100",
        "output_index_2000_100",
        "intensity_index_2000_100",
    ]
    block.columns = base_col_names[:n_cols] if n_cols <= 6 else base_col_names + ["extra"]
    block.insert(0, "year", df_raw.iloc[:, 0])
    block["year"] = to_numeric_safe(block["year"]).astype("Int64")
    for col in block.columns[1:]:
        if col != "extra":
            block[col] = to_numeric_safe(block[col])
    block = block[block["year"].notna()].copy()
    block["year"] = block["year"].astype(int)
    block = block[block["year"] >= 1990].copy()
    block = block[[c for c in block.columns if c != "extra"]]
    block["sector"] = "Iron, steel, non-ferrous metals"
    desired_order = [
        "year", "sector",
        "consumption_ktoe", "output", "consumption_per_unit_output",
        "consumption_index_2000_100", "output_index_2000_100",
        "intensity_index_2000_100",
    ]
    final_cols = [c for c in desired_order if c in block.columns]
    return block[final_cols].reset_index(drop=True)

def derive_uk_eedi05(ecuk_df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert the ECUK intensity index (base 2000=100) into an EEDI05-comparable
    index rebased to 2005=100, covering YEAR_MIN–YEAR_MAX.

    Method:
        uk_rebased(y) = intensity_index_2000_100(y)
                        / intensity_index_2000_100(2005)  * 100
    """
    col = "intensity_index_2000_100"
    if col not in ecuk_df.columns:
        raise KeyError(
            f"'{col}' not found in ECUK data. "
            f"Available columns: {ecuk_df.columns.tolist()}"
        )

    base_row = ecuk_df[ecuk_df["year"] == 2005]
    if base_row.empty or pd.isna(base_row[col].iloc[0]):
        raise ValueError("No valid 2005 value in ECUK intensity data — cannot rebase.")

    base_val = float(base_row[col].iloc[0])

    uk = ecuk_df[
        (ecuk_df["year"] >= YEAR_MIN) & (ecuk_df["year"] <= YEAR_MAX)
    ][["year", col]].copy()

    uk["eedi05_index"] = (uk[col] / base_val) * 100.0
    uk = uk.dropna(subset=["eedi05_index"])
    uk["geo"]     = "UK"
    uk["country"] = "United Kingdom"
    uk["source"]  = "ECUK (rebased 2005=100)"

    return uk[["geo", "country", "year", "eedi05_index", "source"]].reset_index(drop=True)

def parse_eurostat_eedi05(file_path: Path, geos: list[str]) -> pd.DataFrame:
    """
    Parse estat_nrg_ind_eff_en.csv for EEDI05 rows (FEC_EED / I05) for the
    specified geo codes.
    """
    records: list[dict] = []

    try:
        df_raw    = pd.read_csv(file_path, dtype=str, on_bad_lines="skip")
        col_lower = {c: c.lower() for c in df_raw.columns}

        geo_col  = next((c for c in df_raw.columns if "geo"         in col_lower[c]), None)
        bal_col  = next((c for c in df_raw.columns if "nrg_bal"     in col_lower[c]), None)
        unit_col = next((c for c in df_raw.columns if col_lower[c] == "unit"),        None)
        time_col = next((c for c in df_raw.columns if "time_period" in col_lower[c]), None)
        val_col  = next(
            (c for c in df_raw.columns if "obs_value" in col_lower[c]),
            next((c for c in df_raw.columns if "value" in col_lower[c]), df_raw.columns[-1]),
        )

        if all([geo_col, bal_col, unit_col, time_col]):
            mask = (
                (df_raw[bal_col].str.strip()  == "FEC_EED") &
                (df_raw[unit_col].str.strip() == "I05")     &
                (df_raw[geo_col].isin(geos))
            )
            sub = df_raw[mask][[geo_col, time_col, val_col]].copy()
            sub.columns = ["geo", "year", "eedi05_index"]

            sub["year"]         = pd.to_numeric(sub["year"], errors="coerce")
            sub["eedi05_index"] = to_numeric_safe(sub["eedi05_index"])
            sub = sub.dropna(subset=["year", "eedi05_index"])
            sub["year"]         = sub["year"].astype(int)
            sub["source"]       = "Eurostat FEC_EED I05"

            records = sub.to_dict("records")
        else:
            print(
                "  [WARNING] Eurostat CSV is missing expected columns.\n"
                f"  Found: {df_raw.columns.tolist()}"
            )

    except Exception as e:
        print(f"  [CSV parse failed: {e}]")

    if not records:
        print(
            "\n[WARNING] No EEDI05 records found for requested EU geos.\n"
            "The eu_eedi05_benchmark_clean.csv EU section will be empty.\n"
        )

    df = pd.DataFrame(records).drop_duplicates()
    if not df.empty:
        df["country"] = df["geo"].map(GEO_NAME_MAP).fillna(df["geo"])
        df = df[
            (df["year"] >= YEAR_MIN) & (df["year"] <= YEAR_MAX)
        ].sort_values(["country", "year"]).reset_index(drop=True)

    return df[["geo", "country", "year", "eedi05_index", "source"]] if not df.empty else df

def main() -> None:

    missing = [f for f in [INTENSITY_FILE, EUROSTAT_FILE] if not f.exists()]
    if missing:
        sys.exit(
            "\n[ERROR] Input file(s) not found:\n"
            + "\n".join(f"  {p}" for p in missing)
            + f"\n\nExpected directory: {BASE_DIR}\n"
        )
    print("=" * 60)
    print("STEP 1: Reading ECUK intensity workbook...")
    xls = pd.ExcelFile(INTENSITY_FILE, engine="openpyxl")
    print(f"  Sheets found: {xls.sheet_names}")

    i4_sheet = next((s for s in xls.sheet_names if "i4" in s.lower()), None)
    if i4_sheet is None:
        sys.exit(
            "\n[ERROR] No 'Table I4' sheet found.\n"
            f"Available sheets: {xls.sheet_names}\n"
        )

    print(f"  Parsing sheet: '{i4_sheet}'")
    df_i4_raw = pd.read_excel(
        INTENSITY_FILE, sheet_name=i4_sheet, header=None, engine="openpyxl"
    )

    metals_df = parse_ecuk_table_i4(df_i4_raw)
    for col_short, col_full in [
        ("consumption", "consumption_ktoe"),
        ("output",      "output"),
        ("intensity",   "consumption_per_unit_output"),
    ]:
        if col_full in metals_df.columns:
            metals_df[f"{col_short}_missing"] = metals_df[col_full].isna()

    print(f"  ECUK metals block: {len(metals_df)} rows, {metals_df.shape[1]} columns")
    print(f"  Year range: {metals_df['year'].min()} – {metals_df['year'].max()}")
    print(metals_df.head(3).to_string(index=False))

    # Derive UK EEDI05 proxy (rebased 2005=100) — v4 contribution
    uk_eedi05_df = derive_uk_eedi05(metals_df)
    print(
        f"\n  UK EEDI05 rows derived: {len(uk_eedi05_df)}  "
        f"({uk_eedi05_df['year'].min()}–{uk_eedi05_df['year'].max()})"
    )
    print(uk_eedi05_df.head(3).to_string(index=False))
    print("\n" + "=" * 60)
    print("STEP 2: Reading Eurostat EEDI05 CSV...")
    eu_df = parse_eurostat_eedi05(EUROSTAT_FILE, EU_GEOS)

    if not eu_df.empty:
        print(f"  EU rows: {len(eu_df)}  ({eu_df['year'].min()}–{eu_df['year'].max()})")
        print(f"  Countries: {sorted(eu_df['country'].unique())}")
        print(eu_df.head(6).to_string(index=False))
    else:
        print("  [WARNING] Eurostat DataFrame is empty.")
    print("\n" + "=" * 60)
    print("STEP 3: Building combined EEDI05 benchmark...")

    combined_eedi05 = pd.concat(
        [eu_df.sort_values(["country", "year"]), uk_eedi05_df.sort_values("year")],
        ignore_index=True,
    )
    print(
        f"  Combined benchmark: {len(combined_eedi05)} rows  |  "
        f"Countries: {sorted(combined_eedi05['country'].unique())}"
    )
    print("\n" + "=" * 60)
    print("STEP 4: Building dashboard master table...")

    uk_eedi_merge = (
        uk_eedi05_df[["year", "eedi05_index"]]
        .rename(columns={"eedi05_index": "uk_eedi05_index"})
        .copy()
    )
    metals_dashboard = metals_df.merge(uk_eedi_merge, on="year", how="left")

    if "consumption_per_unit_output" in metals_dashboard.columns:
        metals_dashboard["efficiency_proxy"] = np.where(
            metals_dashboard["consumption_per_unit_output"].notna()
            & (metals_dashboard["consumption_per_unit_output"] != 0),
            1 / metals_dashboard["consumption_per_unit_output"],
            np.nan,
        )

    print(
        f"  Dashboard master: {len(metals_dashboard)} rows, "
        f"{metals_dashboard.shape[1]} columns"
    )
    print("\n" + "=" * 60)
    print("STEP 5: Writing output files...")

    metals_df.to_csv(        OUTPUT_DIR / "uk_metals_intensity_clean.csv",   index=False)
    combined_eedi05.to_csv(  OUTPUT_DIR / "eu_eedi05_benchmark_clean.csv",   index=False)
    metals_dashboard.to_csv( OUTPUT_DIR / "metal_melt_dashboard_master.csv", index=False)

    country_stats = (
        combined_eedi05.groupby("country")
        .agg(
            rows         = ("year", "count"),
            year_min     = ("year", "min"),
            year_max     = ("year", "max"),
            index_min    = ("eedi05_index", "min"),
            index_max    = ("eedi05_index", "max"),
            missing_vals = ("eedi05_index", lambda x: x.isna().sum()),
            source       = ("source", "first"),
        )
        .reset_index()
    )

    file_summary = pd.DataFrame({
        "dataset":    [
            "uk_metals_intensity_clean",
            "eu_eedi05_benchmark_clean",
            "metal_melt_dashboard_master",
        ],
        "rows":    [len(metals_df), len(combined_eedi05), len(metals_dashboard)],
        "columns": [metals_df.shape[1], combined_eedi05.shape[1], metals_dashboard.shape[1]],
        "year_min": [
            metals_df["year"].min()        if not metals_df.empty        else None,
            combined_eedi05["year"].min()  if not combined_eedi05.empty  else None,
            metals_dashboard["year"].min() if not metals_dashboard.empty else None,
        ],
        "year_max": [
            metals_df["year"].max()        if not metals_df.empty        else None,
            combined_eedi05["year"].max()  if not combined_eedi05.empty  else None,
            metals_dashboard["year"].max() if not metals_dashboard.empty else None,
        ],
        "missing_index_values": [
            int(metals_df["intensity_index_2000_100"].isna().sum())
            if "intensity_index_2000_100" in metals_df.columns else None,
            int(combined_eedi05["eedi05_index"].isna().sum()),
            int(metals_dashboard["uk_eedi05_index"].isna().sum())
            if "uk_eedi05_index" in metals_dashboard.columns else None,
        ],
    })

    log_path = OUTPUT_DIR / "data_quality_log.csv"
    with open(log_path, "w") as f:
        f.write("=== FILE SUMMARY ===\n")
        file_summary.to_csv(f, index=False)
        f.write("\n=== PER-COUNTRY EEDI05 DETAIL ===\n")
        country_stats.to_csv(f, index=False)

    print("\n" + "─" * 60)
    print("Done.  Output directory:", OUTPUT_DIR)
    print("\nFile summary:")
    print(file_summary.to_string(index=False))
    print("\nPer-country EEDI05 detail:")
    print(country_stats.to_string(index=False))
    print("\nFiles written:")
    for f in sorted(OUTPUT_DIR.glob("*.csv")):
        print(f"  {f.name:<45}  {f.stat().st_size / 1024:.1f} KB")


if __name__ == "__main__":
    main()
