"""
===============================================================================
EA WATER QUALITY DATA PROCESSOR
===============================================================================
Title   : Environment Agency (England) Open Water Quality Archive — Processor
Version : 2.1.0
Authors : Domanique Bridglalsingh, Ahmed Abdalla, Jia Hu, Geyong Min, Xiaohong Li, and Siwei Zheng
Licence : CC-BY-4.0  (same licence as the underlying EA data)
Python  : >= 3.9

PURPOSE
-------
The Environment Agency (EA) published annual CSV files of water-quality
measurements from 2000 to 2025.  Those files are no longer publicly hosted.
This script reads the raw yearly CSVs, applies transparent and reproducible
cleaning steps, and produces a single, analysis-ready dataset.

TWO OUTPUT MODES
----------------
1. "full"             – Every water-related test, type, and sampling point.
2. "electrochemistry" – A focused subset of dissolved metals, ions, pH,
                        conductivity, temperature, and turbidity — the
                        parameters most relevant to electrochemical sensing.

HOW TO USE (see bottom of file for a ready-made example)
--------------------------------------------------------
1. Place all 26 raw CSV files (2000.csv … 2025.csv) in ONE folder.
2. Open this script in a Jupyter notebook or run it as a Python file.
3. Set  RAW_DATA_FOLDER  to the path that contains your CSV files.
4. Set  MODE  to "full" or "electrochemistry".
5. Run.  A new subfolder is created automatically with all outputs.

WHAT THE SCRIPT DOES (step by step)
------------------------------------
For every yearly CSV the script will:
  • Drop columns that are not needed (internal EA IDs, compliance flags …).
  • Rename columns to short, human-readable names.
  • Use the *definition* column (determinand.definition) for test names,
    because it is more descriptive than the abbreviated label.
  • Remove non-quantitative rows (unit = "coded", "text", "yes/no" …).
  • Remove tests that appear fewer than MIN_TEST_COUNT times across
    all 26 years (default 50 — less than 2 per year on average).
  • Remove sample types that are not water-related (biota, soil, gas …).
  • Standardise units:
        µg/l  → mg/l   (÷ 1 000)
        ng/l  → mg/l   (÷ 1 000 000)
        pg/l  → mg/l   (÷ 1 000 000 000)
        g/l   → mg/l   (× 1 000)
        ppm   → mg/l   (1 : 1 in dilute water)
        FTU   → NTU    (1 : 1 — both nephelometric turbidity scales)
        ms/cm → µS/cm  (× 1 000)
        Various µS/cm spellings → uS/cm
        no/ml  → no/100ml  (× 100)
        no/ul  → no/100ml  (× 100 000)
        no/10ul → no/100ml (× 10 000)
        g/kg   → ppt       (1 : 1 for salinity)
  • Convert British National Grid (Easting / Northing) to WGS-84
    Latitude / Longitude using the pyproj library.
  • Remove known dummy / placeholder coordinates that the EA used for
    mis-registered samples (Easting = 500 000, Northing = 1–8).
  • Flag (but NOT delete) potential outliers for key parameters.
  • Print a detailed log of every action so you can see exactly what
    happened at every step.

OUTPUTS (saved in <RAW_DATA_FOLDER>/EA_processed_output/)
---------
  • EA_clean_2000_2025.csv       – The main clean dataset.
  • EA_clean_2000_2025.parquet   – Same data in fast columnar format.
  • EA_statistics_2000_2025.xlsx – Descriptive statistics.
  • EA_qa_report.html            – Visual quality-assurance summary.
  • processing_log.txt           – Full text log of every cleaning step.

DEPENDENCIES (auto-installed if missing)
-----------
  pandas, numpy, pyproj, pyarrow, openpyxl, chardet
===============================================================================
"""

# ============================================================================
# STEP 0 — AUTOMATICALLY INSTALL MISSING LIBRARIES
# ============================================================================
# This block runs BEFORE any imports so that the user never sees an
# ImportError.  It checks each required package and installs it silently
# using pip if it is not already present.

def _ensure_dependencies():
    """Install any missing Python packages required by this script."""
    import subprocess, sys, importlib

    REQUIRED = {
        "pandas":   "pandas",
        "numpy":    "numpy",
        "pyproj":   "pyproj",
        "pyarrow":  "pyarrow",
        "openpyxl": "openpyxl",
        "chardet":  "chardet",
    }

    missing = []
    for import_name, pip_name in REQUIRED.items():
        try:
            importlib.import_module(import_name)
        except ImportError:
            missing.append(pip_name)

    if missing:
        print(f"Installing missing packages: {', '.join(missing)} ...")
        subprocess.check_call(
            [sys.executable, "-m", "pip", "install", "--quiet",
             "--break-system-packages", *missing],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        print("  Done.\n")

_ensure_dependencies()

# ============================================================================
# IMPORTS
# ============================================================================

from pathlib import Path
import pandas as pd
import numpy as np
from pyproj import Transformer
from typing import Dict, Any, Optional, List
from datetime import datetime
import warnings, sys, io, re

# Suppress ALL non-critical warnings so the user sees only our clean log.
# This prevents pandas regex-group warnings and deprecation notices from
# cluttering the output in Jupyter notebooks.
warnings.filterwarnings("ignore")


# ============================================================================
# MAIN FUNCTION
# ============================================================================

def build_ea_clean_2000_2025(
    input_dir: str | Path,
    mode: str = "full",
    years: range = range(2000, 2026),
    chunksize: int = 250_000,
    min_test_count: int = 50,
    flag_outliers: bool = True,
    generate_stats: bool = True,
    generate_qa_report: bool = True,
    save_log: bool = True,
) -> Dict[str, Any]:
    """
    Clean and combine the EA yearly CSV files into one analysis-ready dataset.

    Parameters
    ----------
    input_dir : str or Path
        Folder that contains the raw yearly CSV files (2000.csv … 2025.csv).
        A subfolder called  EA_processed_output/  will be created here
        automatically to hold every output file.

    mode : str, default "full"
        "full"             → keep ALL water-related tests and types.
        "electrochemistry" → keep only dissolved metals, ions, pH,
                             conductivity, temperature, turbidity.

    years : range, default range(2000, 2026)
        Which years to process.  Change this if you only have a subset.

    chunksize : int, default 250_000
        Number of CSV rows read at a time.  Lower this if you have
        limited RAM (e.g. 100_000).  Raise it if you have plenty of RAM.

    min_test_count : int, default 50
        Tests with fewer total records than this across ALL years are
        dropped.  Set to 0 to keep everything.  Only used in "full" mode.

    flag_outliers : bool, default True
        If True, an  outlier_flag  column is added.  Values outside
        physically plausible ranges are flagged but NOT removed — the
        user decides what to do with them.

    generate_stats : bool, default True
        If True, an Excel workbook of descriptive statistics is saved.

    generate_qa_report : bool, default True
        If True, an HTML quality-assurance report is saved.

    save_log : bool, default True
        If True, the full processing log is also saved as a text file.

    Returns
    -------
    dict  with keys:
        "final_rows"    – number of rows in the final dataset
        "per_year_rows" – dict  {year: row_count}
        "output_dir"    – path to the output folder
        "csv"           – path to the CSV output
        "parquet"       – path to the Parquet output (or None)
        "statistics"    – path to the statistics Excel file (or None)
        "qa_report"     – path to the HTML QA report (or None)
        "log"           – path to the text log (or None)
        "data_quality"  – dict of summary quality metrics
    """

    # Suppress warnings inside the function as well, so that even if the
    # user resets the global filter, our function stays clean.
    warnings.filterwarnings("ignore")

    # ------------------------------------------------------------------
    # Capture all print output so we can save it as a log file later
    # ------------------------------------------------------------------
    log_buffer = io.StringIO()

    def log(msg: str = ""):
        """Print to screen AND capture in the log buffer."""
        print(msg)
        log_buffer.write(msg + "\n")

    # ------------------------------------------------------------------
    # Resolve directories
    # ------------------------------------------------------------------
    input_dir = Path(input_dir).resolve()
    out_dir   = input_dir / "EA_processed_output"
    out_dir.mkdir(parents=True, exist_ok=True)

    mode = mode.strip().lower()
    if mode not in ("full", "electrochemistry"):
        raise ValueError(f"mode must be 'full' or 'electrochemistry', got '{mode}'")

    log("=" * 70)
    log("  EA WATER QUALITY DATA PROCESSOR  v2.1")
    log("=" * 70)
    log(f"  Mode            : {mode.upper()}")
    log(f"  Years           : {min(years)} – {max(years)}")
    log(f"  Input folder    : {input_dir}")
    log(f"  Output folder   : {out_dir}")
    log(f"  Chunk size      : {chunksize:,}")
    log(f"  Min test count  : {min_test_count}")
    log(f"  Flag outliers   : {flag_outliers}")
    log(f"  Started at      : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log("=" * 70)
    log()

    # ==================================================================
    #  CONFIGURATION — TYPES TO KEEP
    # ==================================================================
    # We keep every sample type that represents a WATER body.
    # Biological tissue, soil, gas, sediment, waste, and calibration
    # samples are excluded because they are not water-quality readings.
    # ------------------------------------------------------------------

    WATER_TYPES = {
        # Surface water
        "RIVER / RUNNING SURFACE WATER",
        "POND / LAKE / RESERVOIR WATER",
        "CANAL WATER",
        "CANAL WATER - SALINE",
        # Wastewater & sewage
        "FINAL SEWAGE EFFLUENT",
        "CRUDE SEWAGE",
        "ANY SEWAGE",
        "STORM SEWER OVERFLOW DISCHARGE",
        "STORM TANK EFFLUENT",
        "STORM TANK INFLUENT",
        "SURFACE DRAINAGE",
        # Trade / industrial effluent
        "ANY TRADE EFFLUENT",
        "TRADE EFFLUENT - FRESHWATER RETURNED ABSTRACTED",
        "TRADE EFFLUENT - SALINE WATER RETURNED ABSTRACTED",
        "TRADE EFFLUENT - GROUNDWATER RETURNED ABSTRACTED",
        # Groundwater
        "GROUNDWATER",
        "GROUNDWATER - PURGED/PUMPED/REFILLED",
        "GROUNDWATER - STATIC/UNPURGED",
        # Leachate & minewater
        "ANY LEACHATE",
        "MINEWATER",
        "MINEWATER (FLOWING/PUMPED)",
        # Marine & estuarine
        "SEA WATER",
        "SEA WATER - INTERTIDAL",
        "SEA WATER AT HIGH TIDE",
        "SEA WATER AT LOW TIDE",
        "ESTUARINE WATER",
        "ESTUARINE WATER - INTERTIDAL",
        "ESTUARINE WATER AT HIGH TIDE",
        "ESTUARINE WATER AT LOW TIDE",
    }

    # Regex pattern for types we always exclude regardless of mode.
    # NOTE: We use (?:...) NON-CAPTURING groups to prevent pandas
    #        from emitting UserWarning about regex capture groups.
    DROP_TYPE_PATTERN = (
        r"(?:SEDIMENT|WHOLE ANIMAL|MUSCLE|LIVER|DIGESTIVE GLAND|BIOTA|"
        r"SOIL|ASH|WASTE\b|GAS|PRECIPITATION|CALIBRATION WATER|"
        r"POTABLE WATER|BOREHOLE GAS|ANY WATER\b|ANY NON-AQUEOUS LIQUID|"
        r"UNCODED|ANY AGRICULTURAL|ANY SEWAGE SLUDGE|ANY TIPPED|"
        r"ALGAE|SEAWEED|INVERTEBRATE|FISH|FLATFISH|BRYOPHYTE|"
        r"HIGHER PLANT|RANUNCULUS|FONTINALIS|ANY OIL|ANY BIOTA|"
        r"SOLID/SEDIMENT|MOSS|WRACK|COCKLE|MUSSEL|OYSTER|"
        r"SHRIMP|WORM|TELLIN|SCALLOP|TROUT|EEL|ROACH|FLOUNDER|"
        r"DAB|PLAICE|SOLE\b|WHITEBAIT|AIR\b|CONSTRUCTION)"
    )

    # ==================================================================
    #  CONFIGURATION — ELECTROCHEMISTRY TEST SET
    # ==================================================================

    ELECTROCHEMISTRY_TESTS = {
        # Dissolved metals
        "Magnesium, Dissolved", "Copper, Dissolved", "Nickel, Dissolved",
        "Iron, Dissolved", "Manganese, Dissolved", "Uranium, Dissolved",
        "Lithium, Dissolved", "Potassium, Dissolved", "Sodium, Dissolved",
        "Lead, Dissolved", "Cadmium, Dissolved", "Mercury, Dissolved",
        "Silver, Dissolved", "Barium, Dissolved", "Zinc, Dissolved",
        "Chromium, Dissolved", "Arsenic, Dissolved", "Calcium, Dissolved",
        "Boron, Dissolved", "Aluminium, Dissolved", "Strontium, Filtered",
        # Total metals (useful for comparison)
        "Magnesium", "Copper", "Nickel", "Iron", "Manganese",
        "Potassium", "Sodium", "Lead", "Cadmium", "Mercury",
        "Silver", "Barium", "Zinc", "Chromium", "Arsenic",
        "Calcium", "Boron", "Aluminium",
        # Physical chemistry
        "pH",
        "Conductivity at 25 C", "Conductivity at 20 C",
        "Temperature of Water",
        "Turbidity",
        # Key anions / nutrients
        "Chloride",
        "Ammoniacal Nitrogen as N",
        "Nitrogen, Total Oxidised as N",
        "Orthophosphate, reactive as P",
        "Nitrate as N", "Nitrite as N",
        "Sulphate as SO4",
        "Fluoride",
        # Oxygen
        "Oxygen, Dissolved as O2",
        "Oxygen, Dissolved, % Saturation",
        # Other useful
        "Alkalinity to pH 4.5 as CaCO3",
        "Hardness, Total as CaCO3",
        "Solids, Suspended at 105 C",
        "BOD : 5 Day ATU",
        "Salinity : In Situ",
    }

    # ==================================================================
    #  CONFIGURATION — NON-QUANTITATIVE UNITS TO DROP
    # ==================================================================

    NON_QUANTITATIVE_UNITS = {
        "coded",      # categorical indicators (e.g. "No flow / No sample")
        "text",       # free-text descriptions
        "yes/no",     # binary flags (e.g. "Photo Taken: Yes/No")
        "pres/nf",    # present / not found (biological presence)
        "pres/nft",   # variant spelling
        "garber c",   # Garber colour class (ordinal, not continuous)
        "hh.mm",      # clock-time notation
        "ngr",        # National Grid Reference (spatial code, not a number)
        "deccafix",   # Decca navigation fix (obsolete positioning)
        "ug",         # bare micrograms — mass with no volume denominator.
                      #   685 records, mostly PAH recovery tests.
                      #   Meaningless for water-quality concentration analysis.
    }

    # ==================================================================
    #  CONFIGURATION — NON-QUANTITATIVE TEST FRAGMENTS TO DROP
    # ==================================================================
    # We use re.escape() on each fragment so that special characters
    # like  (  )  /  are treated literally, not as regex operators.
    # This also prevents pandas UserWarning about regex capture groups.
    # ------------------------------------------------------------------

    BAD_TEST_FRAGMENTS = [
        "No flow", "No sample", "Site Inspection",
        "Present/Not found", "Pass/Fail",
        "Population Equivalent", "Sampling Frequency",
        "Photo Taken",
        "Weather :",
        "Bathing Water Profile",
        "National Grid Reference",
        "Sewage debris", "Foam Visible",
        "Colour : Abnormal", "Tarry residues",
        "MST Filtration", "Time of high tide",
        "Number of beach users", "Bathers per 100",
        "Type of flow", "Laboratory Sample Number",
        "State tide", "Colour (1/0)", "Tars/Floatg",
        "OilTypeQual", "WEATHER FLAG",
        "Borehole RefPt", "Sample Depth",
    ]

    _BAD_TEST_PATTERN = "|".join(re.escape(f) for f in BAD_TEST_FRAGMENTS)

    # ==================================================================
    #  CONFIGURATION — DUMMY / WRONG COORDINATES
    # ==================================================================

    DUMMY_EASTING  = 500_000
    DUMMY_NORTHINGS = {1, 2, 3, 4, 5, 6, 7, 8}

    # ==================================================================
    #  CONFIGURATION — OUTLIER THRESHOLDS
    # ==================================================================
    # Values outside these ranges are FLAGGED but NOT removed.
    #
    # RATIONALE FOR NOT DELETING OUTLIERS:
    #   Silently deleting records is irreversible and hides potentially
    #   real extreme events (e.g. pollution spills, instrument faults).
    #   Flagging is transparent: every record stays in the dataset and
    #   the flag column lets the user decide what to include.
    #
    # NOTE ON NEGATIVE TEMPERATURES:
    #   Water in UK rivers can reach temperatures very close to 0 °C in
    #   winter.  Readings slightly below 0 °C can occur from super-
    #   cooling near ice formation or minor instrument calibration drift.
    #   We flag these but do NOT delete them.
    # ------------------------------------------------------------------

    OUTLIER_THRESHOLDS = {
        "Temperature of Water":        (-5,    45),
        "pH":                          ( 1,    14),
        "Conductivity at 25 C":        ( 0, 80_000),
        "Conductivity at 20 C":        ( 0, 80_000),
        "Salinity : In Situ":          ( 0,    50),
        "Solids, Suspended at 105 C":  ( 0, 50_000),
        "Oxygen, Dissolved, % Saturation": (0, 250),
        "Oxygen, Dissolved as O2":     ( 0,    25),
        "Ammoniacal Nitrogen as N":    ( 0, 1_000),
        "Turbidity":                   ( 0, 10_000),
    }

    # ==================================================================
    #  SEASON LOOKUP
    # ==================================================================

    SEASON_CATS = ["Winter", "Spring", "Summer", "Autumn"]

    def _month_to_season(m) -> str:
        if pd.isna(m):
            return pd.NA
        m = int(m)
        if m in (12, 1, 2): return "Winter"
        if m in (3, 4, 5):  return "Spring"
        if m in (6, 7, 8):  return "Summer"
        return "Autumn"

    # ==================================================================
    #  UNIT STANDARDISATION
    # ==================================================================

    def _standardise_units(df: pd.DataFrame) -> pd.DataFrame:
        """
        Convert all measurement units to a consistent set.

        Conversions applied:
          µg/l   → mg/l     (÷ 1,000)
          ng/l   → mg/l     (÷ 1,000,000)
          pg/l   → mg/l     (÷ 1,000,000,000)
          g/l    → mg/l     (× 1,000)
          ppm    → mg/l     (1 : 1 for dilute aqueous solutions)
          FTU    → NTU      (1 : 1 — both are nephelometric turbidity)
          ms/cm  → uS/cm   (× 1,000)
          no/ml  → no/100ml (× 100)
          no/ul  → no/100ml (× 100,000)
          no/10ul→ no/100ml (× 10,000)
          g/kg   → ppt      (1 : 1 for salinity)
          psu, ‰ → ppt      (1 : 1 by definition)
        """
        if "Test" not in df.columns or "Unit" not in df.columns:
            return df

        u = df["Unit"].astype(str).str.strip()

        # --- Conductivity unit spelling variants → uS/cm ---------------
        u = (u.str.replace("µ", "u", regex=False)
              .str.replace("μ", "u", regex=False)
              .str.replace("US/CM", "uS/cm", regex=False)
              .str.replace("Us/cm", "uS/cm", regex=False)
              .str.replace("us/cm", "uS/cm", regex=False)
              .str.replace("µS/cm", "uS/cm", regex=False)
              .str.replace("μS/cm", "uS/cm", regex=False))
        df["Unit"] = u

        # --- Normalise conductivity test name variants ------------------
        df["Test"] = df["Test"].str.replace(
            "Conductivity at 20C", "Conductivity at 20 C", regex=False
        )

        # --- µg/l  →  mg/l  (÷ 1,000) ---------------------------------
        mask = df["Unit"].str.lower() == "ug/l"
        if mask.any():
            df.loc[mask, "result"] = (
                pd.to_numeric(df.loc[mask, "result"], errors="coerce") / 1_000
            )
            df.loc[mask, "Unit"] = "mg/l"

        # --- ng/l  →  mg/l  (÷ 1,000,000) ------------------------------
        mask = df["Unit"].str.lower() == "ng/l"
        if mask.any():
            df.loc[mask, "result"] = (
                pd.to_numeric(df.loc[mask, "result"], errors="coerce") / 1_000_000
            )
            df.loc[mask, "Unit"] = "mg/l"

        # --- pg/l  →  mg/l  (÷ 1,000,000,000) --------------------------
        mask = df["Unit"].str.lower() == "pg/l"
        if mask.any():
            df.loc[mask, "result"] = (
                pd.to_numeric(df.loc[mask, "result"], errors="coerce") / 1_000_000_000
            )
            df.loc[mask, "Unit"] = "mg/l"

        # --- g/l  →  mg/l  (× 1,000) ----------------------------------
        mask = df["Unit"].str.lower() == "g/l"
        if mask.any():
            df.loc[mask, "result"] = (
                pd.to_numeric(df.loc[mask, "result"], errors="coerce") * 1_000
            )
            df.loc[mask, "Unit"] = "mg/l"

        # --- ppm  →  mg/l  (1 : 1 in dilute water) --------------------
        mask = df["Unit"].str.lower() == "ppm"
        if mask.any():
            df.loc[mask, "Unit"] = "mg/l"

        # --- FTU  →  NTU  (1 : 1, applied to ALL tests) ---------------
        #     Previously only caught Test == "Turbidity" exactly.
        #     "Turbidity : In Situ" (48,912 records) also uses FTU.
        #     FTU and NTU are both nephelometric scales and are
        #     numerically identical, so we convert regardless of test.
        mask = df["Unit"].str.lower() == "ftu"
        if mask.any():
            df.loc[mask, "Unit"] = "NTU"

        # --- ntu (lowercase) → NTU (capitalisation only) ---------------
        mask = df["Unit"] == "ntu"
        if mask.any():
            df.loc[mask, "Unit"] = "NTU"

        # --- ms/cm  →  uS/cm  (× 1,000) -------------------------------
        mask = df["Unit"].str.lower() == "ms/cm"
        if mask.any():
            df.loc[mask, "result"] = (
                pd.to_numeric(df.loc[mask, "result"], errors="coerce") * 1_000
            )
            df.loc[mask, "Unit"] = "uS/cm"

        # --- MICROBIOLOGY: no/ml → no/100ml (× 100) -------------------
        #     The EA used both no/ml and no/100ml for coliform and
        #     streptococcal tests.  no/100ml is the regulatory standard.
        mask = df["Unit"] == "no/ml"
        if mask.any():
            df.loc[mask, "result"] = (
                pd.to_numeric(df.loc[mask, "result"], errors="coerce") * 100
            )
            df.loc[mask, "Unit"] = "no/100ml"

        # --- no/ul → no/100ml (× 100,000) -----------------------------
        mask = df["Unit"] == "no/ul"
        if mask.any():
            df.loc[mask, "result"] = (
                pd.to_numeric(df.loc[mask, "result"], errors="coerce") * 100_000
            )
            df.loc[mask, "Unit"] = "no/100ml"

        # --- no/10ul → no/100ml (× 10,000) ----------------------------
        mask = df["Unit"] == "no/10ul"
        if mask.any():
            df.loc[mask, "result"] = (
                pd.to_numeric(df.loc[mask, "result"], errors="coerce") * 10_000
            )
            df.loc[mask, "Unit"] = "no/100ml"

        # --- SALINITY: g/kg, psu, ‰ → ppt (all 1 : 1) ----------------
        mask = df["Unit"].str.lower().isin({"g/kg", "psu", "\u2030"})
        if mask.any():
            df.loc[mask, "Unit"] = "ppt"

        return df

    # ==================================================================
    #  OUTLIER FLAGGER
    # ==================================================================

    def _flag_outliers(df: pd.DataFrame) -> pd.DataFrame:
        """Add outlier_flag column. Nothing is deleted."""
        if "outlier_flag" not in df.columns:
            df["outlier_flag"] = False

        for test_name, (lo, hi) in OUTLIER_THRESHOLDS.items():
            mask = df["Test"] == test_name
            if mask.any():
                bad = mask & ((df["result"] < lo) | (df["result"] > hi))
                df.loc[bad, "outlier_flag"] = True

        return df

    # ==================================================================
    #  COORDINATE CONVERTER  (British National Grid → WGS-84)
    # ==================================================================

    def _convert_coordinates(df: pd.DataFrame) -> pd.DataFrame:
        """Replace Easting/Northing (EPSG:27700) with Lat/Lon (EPSG:4326)."""
        if "Easting" not in df.columns or "Northing" not in df.columns:
            return df

        log("  Converting Easting/Northing → Latitude/Longitude …")

        unique = (
            df[["Easting", "Northing"]]
            .drop_duplicates()
            .dropna(subset=["Easting", "Northing"])
        )

        transformer = Transformer.from_crs(
            "EPSG:27700", "EPSG:4326", always_xy=True
        )

        lons, lats = transformer.transform(
            unique["Easting"].values, unique["Northing"].values
        )
        unique = unique.copy()
        unique["Latitude"]  = lats
        unique["Longitude"] = lons

        n_before = len(df)
        df = df.merge(unique, on=["Easting", "Northing"], how="left")
        df = df.drop(columns=["Easting", "Northing"])

        log(f"    {len(unique):,} unique coordinate pairs converted.")
        log(f"    Rows with valid lat/lon: "
            f"{df['Latitude'].notna().sum():,} / {n_before:,}")

        return df

    # ==================================================================
    #  CHUNK CLEANER
    # ==================================================================

    def _clean_chunk(
        raw: pd.DataFrame,
        year_hint: int,
        test_filter: Optional[set],
    ) -> pd.DataFrame:
        df = raw.copy()

        # --- Drop unneeded columns ------------------------------------
        drop_cols = [
            "@id", "sample.samplingPoint",
            "sample.samplingPoint.notation",
            "resultQualifier.notation",
            "codedResultInterpretation.interpretation",
            "determinand.label",
            "sample.isComplianceSample",
            "sample.purpose.label",
            "determinand.notation",
        ]
        existing = [c for c in drop_cols if c in df.columns]
        if existing:
            df = df.drop(columns=existing)

        # --- Rename columns -------------------------------------------
        rename_map = {
            "sample.samplingPoint.label":       "Sampling Point",
            "sample.sampleDateTime":            "Date",
            "sample.sampledMaterialType.label": "Type",
            "determinand.definition":           "Test",
            "determinand.unit.label":           "Unit",
            "result":                           "result",
            "Result":                           "result",
            "sample.samplingPoint.easting":     "Easting",
            "sample.samplingPoint.northing":    "Northing",
        }
        rename_map = {k: v for k, v in rename_map.items() if k in df.columns}
        df = df.rename(columns=rename_map)

        # --- Parse coordinates to numeric -----------------------------
        for col in ("Easting", "Northing"):
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # --- Remove dummy / wrong coordinates -------------------------
        if "Easting" in df.columns and "Northing" in df.columns:
            dummy_mask = (
                df["Easting"].eq(DUMMY_EASTING)
                & df["Northing"].isin(DUMMY_NORTHINGS)
            )
            df = df[~dummy_mask]

        # --- Parse Date → Season, SourceYear --------------------------
        if "Date" in df.columns:
            df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
            df["Season"] = df["Date"].dt.month.map(_month_to_season)
            df["Season"] = pd.Categorical(
                df["Season"], categories=SEASON_CATS, ordered=True
            )
            df["SourceYear"] = (
                df["Date"].dt.year.fillna(year_hint).astype("Int64")
            )
        else:
            df["Season"]    = pd.Categorical([], categories=SEASON_CATS)
            df["SourceYear"] = year_hint

        # --- Filter by sample type ------------------------------------
        if "Type" in df.columns:
            df = df[
                ~df["Type"].astype(str)
                .str.contains(DROP_TYPE_PATTERN, case=False, na=False)
            ]
            df = df[df["Type"].isin(WATER_TYPES)]

        # --- Remove non-quantitative units ----------------------------
        if "Unit" in df.columns:
            df = df[~df["Unit"].str.strip().str.lower().isin(NON_QUANTITATIVE_UNITS)]

        # --- Remove non-quantitative test fragments -------------------
        if "Test" in df.columns:
            bad_test_mask = (
                df["Test"].astype(str)
                .str.contains(_BAD_TEST_PATTERN, case=False, na=False)
            )
            df = df[~bad_test_mask]

        # --- Filter tests (mode-dependent) ----------------------------
        if "Test" in df.columns and test_filter is not None:
            df = df[df["Test"].isin(test_filter)]

        # --- Convert result to numeric --------------------------------
        if "result" in df.columns:
            df["result"] = pd.to_numeric(df["result"], errors="coerce")

        # --- Standardise units ----------------------------------------
        if {"Test", "Unit", "result"}.issubset(df.columns):
            df = _standardise_units(df)

        # --- Drop rows where result is NaN ----------------------------
        if "result" in df.columns:
            df = df[df["result"].notna()]

        # --- Flag outliers -------------------------------------------
        if flag_outliers:
            df = _flag_outliers(df)

        # --- Arrange columns -----------------------------------------
        col_order = [
            "Sampling Point", "Type", "Date", "Test", "result", "Unit",
            "Season", "SourceYear", "Easting", "Northing",
        ]
        if flag_outliers:
            col_order.append("outlier_flag")
        col_order = [c for c in col_order if c in df.columns]
        extra     = [c for c in df.columns if c not in col_order]
        df = df[col_order + extra]

        return df

    # ==================================================================
    #  FIND INPUT FILES
    # ==================================================================

    year_files: List[tuple] = []
    for y in years:
        p = input_dir / f"{y}.csv"
        if p.exists():
            year_files.append((y, p))
        else:
            matches = sorted(input_dir.glob(f"{y}*.csv"))
            if matches:
                year_files.append((y, matches[0]))

    if not year_files:
        raise FileNotFoundError(
            f"No CSV files found for years {list(years)} in {input_dir}\n"
            f"Expected files named  2000.csv, 2001.csv, …, 2025.csv"
        )

    log(f"Found {len(year_files)} raw CSV files:\n")
    for y, p in year_files:
        log(f"  {y}  →  {p.name}")
    log()

    # ==================================================================
    #  DETERMINE WHICH TESTS TO KEEP
    # ==================================================================

    if mode == "electrochemistry":
        test_filter = ELECTROCHEMISTRY_TESTS
        log(f"Mode = ELECTROCHEMISTRY  →  keeping {len(test_filter)} "
            f"pre-defined tests.\n")
    else:
        if min_test_count > 0:
            log(f"Mode = FULL  →  first pass: counting tests to drop "
                f"those with < {min_test_count} total records …")
            test_counts: Dict[str, int] = {}
            for y, csv_path in year_files:
                try:
                    enc = "utf-8"
                    pd.read_csv(csv_path, nrows=2, encoding="utf-8")
                except UnicodeDecodeError:
                    enc = "latin-1"
                for chunk in pd.read_csv(
                    csv_path, chunksize=chunksize, low_memory=False,
                    encoding=enc,
                    usecols=lambda c: c in (
                        "determinand.definition", "determinand.unit.label"
                    ),
                ):
                    col = "determinand.definition"
                    if col in chunk.columns:
                        for t, cnt in chunk[col].value_counts().items():
                            test_counts[t] = test_counts.get(t, 0) + cnt

            total_tests = len(test_counts)
            test_filter = {
                t for t, c in test_counts.items() if c >= min_test_count
            }
            dropped_tests = total_tests - len(test_filter)
            log(f"  Total unique tests found : {total_tests:,}")
            log(f"  Tests with >= {min_test_count} records : {len(test_filter):,}")
            log(f"  Rare tests dropped       : {dropped_tests:,}")
            log()
        else:
            test_filter = None
            log("Mode = FULL, min_test_count = 0  →  keeping ALL tests.\n")

    # ==================================================================
    #  MAIN PROCESSING LOOP
    # ==================================================================

    tag = "electrochemistry" if mode == "electrochemistry" else "full"
    out_csv  = out_dir / f"EA_clean_2000_2025_{tag}.csv"
    out_pq   = out_dir / f"EA_clean_2000_2025_{tag}.parquet"
    out_stats = out_dir / f"EA_statistics_2000_2025_{tag}.xlsx"
    out_qa   = out_dir / f"EA_qa_report_{tag}.html"
    out_log  = out_dir / f"EA_processing_log_{tag}.txt"
    tmp_csv  = out_dir / "_tmp_stream.csv"

    for p in (out_csv, out_pq, tmp_csv):
        if p.exists():
            p.unlink()

    summary: Dict[int, int] = {}
    total_streamed = 0
    header_written = False
    total_raw      = 0

    for y, csv_path in year_files:
        log(f"── Processing {y}  ({csv_path.name}) " + "─" * 30)

        n_year_clean = 0
        n_year_raw   = 0

        enc = None
        try:
            pd.read_csv(csv_path, nrows=5, encoding="utf-8")
            enc = "utf-8"
        except UnicodeDecodeError:
            try:
                import chardet
                with open(csv_path, "rb") as f:
                    detected = chardet.detect(f.read(100_000))
                enc = detected.get("encoding", "latin-1")
            except Exception:
                enc = "latin-1"

        for chunk in pd.read_csv(
            csv_path, chunksize=chunksize, low_memory=False, encoding=enc
        ):
            n_year_raw += len(chunk)
            total_raw  += len(chunk)

            cleaned = _clean_chunk(chunk, year_hint=y, test_filter=test_filter)

            if not cleaned.empty:
                cleaned.to_csv(
                    tmp_csv, mode="a", index=False,
                    header=(not header_written)
                )
                header_written = True
                n_year_clean  += len(cleaned)
                total_streamed += len(cleaned)

        dropped_year = n_year_raw - n_year_clean
        pct = (n_year_clean / n_year_raw * 100) if n_year_raw else 0
        summary[y] = n_year_clean

        log(f"  Raw rows       : {n_year_raw:>12,}")
        log(f"  Clean rows     : {n_year_clean:>12,}   ({pct:.1f}% kept)")
        log(f"  Rows removed   : {dropped_year:>12,}")
        log()

    log("=" * 70)
    log(f"  Streaming complete.  Total clean rows (pre-dedup): "
        f"{total_streamed:,}")
    log(f"  Total raw rows read: {total_raw:,}")
    log("=" * 70)
    log()

    if not tmp_csv.exists() or total_streamed == 0:
        raise ValueError(
            "No data survived the filtering criteria.  Check your "
            "input files and mode setting."
        )

    # ==================================================================
    #  FINAL PROCESSING
    # ==================================================================

    log("Loading streamed data for final processing …")
    df_all = pd.read_csv(tmp_csv, low_memory=False, parse_dates=["Date"])
    log(f"  Rows loaded: {len(df_all):,}")

    if "Type" in df_all.columns:
        mask = (
            df_all["Type"].isin(WATER_TYPES)
            & ~df_all["Type"].astype(str).str.contains(
                DROP_TYPE_PATTERN, case=False, na=False
            )
        )
        n_extra = (~mask).sum()
        df_all = df_all[mask]
        if n_extra:
            log(f"  Extra type filter removed {n_extra:,} rows.")

    key_cols = ["Date", "Sampling Point", "Type", "Test"]
    d0 = len(df_all)
    df_all = df_all.drop_duplicates(subset=key_cols, keep="first")
    n_dupes = d0 - len(df_all)
    log(f"  Duplicates removed: {n_dupes:,}")

    df_all["result"] = pd.to_numeric(df_all["result"], errors="coerce")
    n_nan = df_all["result"].isna().sum()
    df_all = df_all[df_all["result"].notna()]
    if n_nan:
        log(f"  NaN results dropped: {n_nan:,}")

    df_all["Season"] = pd.Categorical(
        df_all["Season"], categories=SEASON_CATS, ordered=True
    )

    df_final = _convert_coordinates(df_all)

    log(f"\n  Final dataset: {len(df_final):,} rows × "
        f"{len(df_final.columns)} columns")
    log(f"  Unique sampling points : {df_final['Sampling Point'].nunique():,}")
    log(f"  Unique tests           : {df_final['Test'].nunique()}")
    log(f"  Unique water types     : {df_final['Type'].nunique()}")
    log(f"  Unique units           : {df_final['Unit'].nunique()}")
    log(f"  Date range             : {df_final['Date'].min()} → "
        f"{df_final['Date'].max()}")
    log()

    # ==================================================================
    #  SAVE MAIN OUTPUTS
    # ==================================================================

    log("Saving main outputs …")
    df_final.to_csv(out_csv, index=False)
    log(f"  ✓  CSV saved     : {out_csv.name}")

    wrote_parquet = False
    try:
        df_final.to_parquet(out_pq, engine="pyarrow", compression="zstd")
        wrote_parquet = True
        log(f"  ✓  Parquet saved : {out_pq.name}")
    except Exception as e:
        log(f"  ⚠  Parquet skipped: {e}")

    # ==================================================================
    #  STATISTICAL SUMMARY
    # ==================================================================

    stats_output = None
    if generate_stats:
        log("\nGenerating statistics …")
        try:
            with pd.ExcelWriter(out_stats, engine="openpyxl") as writer:

                test_stats = (
                    df_final.groupby(["Test", "Unit"])["result"]
                    .agg([
                        "count", "min", "max", "mean", "median", "std",
                        ("p10", lambda x: x.quantile(0.10)),
                        ("p25", lambda x: x.quantile(0.25)),
                        ("p75", lambda x: x.quantile(0.75)),
                        ("p90", lambda x: x.quantile(0.90)),
                    ])
                    .round(4)
                    .reset_index()
                )
                test_stats.to_excel(writer, sheet_name="Test_Statistics", index=False)

                type_stats = (
                    df_final.groupby(["Type", "Test"])["result"]
                    .agg(["count", "mean", "median", "std"])
                    .round(4)
                    .reset_index()
                )
                type_stats.to_excel(writer, sheet_name="Type_Test_Stats", index=False)

                season_stats = (
                    df_final.groupby(["Season", "Test"])["result"]
                    .agg(["count", "mean", "median"])
                    .round(4)
                    .reset_index()
                )
                season_stats.to_excel(writer, sheet_name="Seasonal_Stats", index=False)

                coverage = pd.DataFrame({
                    "Metric": [
                        "Total Rows", "Unique Sampling Points",
                        "Unique Tests", "Unique Types", "Unique Units",
                        "Date Range Start", "Date Range End",
                        "Years Covered", "Mode",
                    ],
                    "Value": [
                        len(df_final),
                        df_final["Sampling Point"].nunique(),
                        df_final["Test"].nunique(),
                        df_final["Type"].nunique(),
                        df_final["Unit"].nunique(),
                        str(df_final["Date"].min().date()),
                        str(df_final["Date"].max().date()),
                        df_final["SourceYear"].nunique(),
                        mode.upper(),
                    ],
                })
                coverage.to_excel(writer, sheet_name="Coverage", index=False)

                if flag_outliers and "outlier_flag" in df_final.columns:
                    outlier_df = df_final[df_final["outlier_flag"]]
                    if not outlier_df.empty:
                        outlier_summary = (
                            outlier_df.groupby(["Test", "Type"])
                            .agg(count=("result", "size"),
                                 min_val=("result", "min"),
                                 max_val=("result", "max"))
                            .reset_index()
                        )
                        outlier_summary.to_excel(writer, sheet_name="Outliers", index=False)

                year_counts = (
                    df_final.groupby("SourceYear").size()
                    .reset_index(name="rows")
                )
                year_counts.to_excel(writer, sheet_name="Rows_Per_Year", index=False)

                unit_check = (
                    df_final.groupby("Test")["Unit"]
                    .apply(lambda x: ", ".join(sorted(x.unique())))
                    .reset_index().rename(columns={"Unit": "Units"})
                )
                unit_check["n_units"] = unit_check["Units"].str.count(",") + 1
                multi_unit = unit_check[unit_check["n_units"] > 1].sort_values(
                    "n_units", ascending=False
                )
                multi_unit.to_excel(writer, sheet_name="Multi_Unit_Tests", index=False)

            stats_output = out_stats
            log(f"  ✓  Statistics saved : {out_stats.name}")
        except Exception as e:
            log(f"  ⚠  Statistics failed: {e}")

    # ==================================================================
    #  QA REPORT
    # ==================================================================

    qa_output = None
    if generate_qa_report:
        log("\nGenerating QA report …")
        try:
            n_outliers = 0
            pct_outliers = 0
            if flag_outliers and "outlier_flag" in df_final.columns:
                n_outliers = int(df_final["outlier_flag"].sum())
                pct_outliers = (n_outliers / len(df_final)) * 100

            type_rows = ""
            for typ, cnt in df_final["Type"].value_counts().head(15).items():
                pct = cnt / len(df_final) * 100
                type_rows += f"<tr><td>{typ}</td><td>{cnt:,}</td><td>{pct:.1f}%</td></tr>\n"

            test_rows = ""
            for tst, cnt in df_final["Test"].value_counts().head(20).items():
                pct = cnt / len(df_final) * 100
                test_rows += f"<tr><td>{tst}</td><td>{cnt:,}</td><td>{pct:.1f}%</td></tr>\n"

            unit_dist_rows = ""
            for u_name, cnt in df_final["Unit"].value_counts().head(25).items():
                pct = cnt / len(df_final) * 100
                unit_dist_rows += f"<tr><td>{u_name}</td><td>{cnt:,}</td><td>{pct:.1f}%</td></tr>\n"

            unit_rows = ""
            unit_per_test = df_final.groupby("Test")["Unit"].nunique()
            multi = unit_per_test[unit_per_test > 1]
            if len(multi) == 0:
                unit_rows = "<tr><td colspan='3'>✓ All tests have a single consistent unit</td></tr>"
            else:
                for tst, n_u in multi.items():
                    units = ", ".join(df_final[df_final["Test"] == tst]["Unit"].unique())
                    unit_rows += f"<tr class='warn'><td>{tst}</td><td>{units}</td><td>⚠ {n_u} units</td></tr>\n"

            qa_html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>EA Water Quality — QA Report ({mode.upper()})</title>
<style>
  body {{ font-family: 'Segoe UI', Arial, sans-serif; margin: 2em; color: #222; }}
  h1 {{ color: #1a5276; }}
  h2 {{ color: #2c3e50; border-bottom: 2px solid #2980b9; padding-bottom: 4px; }}
  table {{ border-collapse: collapse; width: 100%; margin: 1em 0 2em; }}
  th, td {{ border: 1px solid #ccc; padding: 6px 10px; text-align: left; }}
  th {{ background: #2980b9; color: #fff; }}
  tr:nth-child(even) {{ background: #f4f6f7; }}
  .warn {{ background: #fef9e7; }}
  .good {{ color: #27ae60; font-weight: bold; }}
  .bad  {{ color: #c0392b; font-weight: bold; }}
  footer {{ margin-top: 3em; color: #888; font-size: 0.85em; }}
</style>
</head>
<body>
<h1>EA Water Quality Data — QA Report</h1>
<p><b>Mode:</b> {mode.upper()} &nbsp;|&nbsp;
   <b>Generated:</b> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>

<h2>Dataset Overview</h2>
<table>
<tr><th>Metric</th><th>Value</th></tr>
<tr><td>Total rows</td><td>{len(df_final):,}</td></tr>
<tr><td>Unique sampling points</td><td>{df_final['Sampling Point'].nunique():,}</td></tr>
<tr><td>Unique tests</td><td>{df_final['Test'].nunique()}</td></tr>
<tr><td>Unique water types</td><td>{df_final['Type'].nunique()}</td></tr>
<tr><td>Unique units</td><td>{df_final['Unit'].nunique()}</td></tr>
<tr><td>Date range</td><td>{df_final['Date'].min().date()} → {df_final['Date'].max().date()}</td></tr>
<tr><td>Years covered</td><td>{df_final['SourceYear'].min()} – {df_final['SourceYear'].max()}</td></tr>
<tr><td>Records with coordinates</td><td>{df_final['Latitude'].notna().sum():,} ({df_final['Latitude'].notna().mean()*100:.1f}%)</td></tr>
</table>

<h2>Data Quality Checks</h2>
<table>
<tr><th>Check</th><th>Result</th><th>Status</th></tr>
<tr><td>Duplicate key rows</td><td>0 (removed during processing)</td><td class="good">✓ PASS</td></tr>
<tr><td>NaN results</td><td>0 (removed during processing)</td><td class="good">✓ PASS</td></tr>
<tr><td>NaN dates</td><td>{df_final['Date'].isna().sum():,}</td>
    <td class="{'good' if df_final['Date'].isna().sum()==0 else 'bad'}">{'✓ PASS' if df_final['Date'].isna().sum()==0 else '⚠ WARNING'}</td></tr>
<tr><td>Flagged outliers</td><td>{n_outliers:,} ({pct_outliers:.2f}%)</td>
    <td class="{'good' if pct_outliers < 5 else 'bad'}">{'✓ OK' if pct_outliers < 5 else '⚠ CHECK'}</td></tr>
</table>

<h2>Unit Conversions Applied</h2>
<table>
<tr><th>From</th><th>To</th><th>Factor</th><th>Rationale</th></tr>
<tr><td>µg/l</td><td>mg/l</td><td>÷ 1,000</td><td>Standard mass-concentration scale</td></tr>
<tr><td>ng/l</td><td>mg/l</td><td>÷ 1,000,000</td><td>Standard mass-concentration scale</td></tr>
<tr><td>pg/l</td><td>mg/l</td><td>÷ 1,000,000,000</td><td>Standard mass-concentration scale</td></tr>
<tr><td>g/l</td><td>mg/l</td><td>× 1,000</td><td>Standard mass-concentration scale</td></tr>
<tr><td>ppm</td><td>mg/l</td><td>1 : 1</td><td>Equivalent for dilute aqueous solutions</td></tr>
<tr><td>FTU</td><td>NTU</td><td>1 : 1</td><td>Both nephelometric turbidity scales</td></tr>
<tr><td>ms/cm</td><td>uS/cm</td><td>× 1,000</td><td>Standard conductivity scale</td></tr>
<tr><td>no/ml</td><td>no/100ml</td><td>× 100</td><td>Regulatory standard for microbiology</td></tr>
<tr><td>no/ul</td><td>no/100ml</td><td>× 100,000</td><td>Regulatory standard for microbiology</td></tr>
<tr><td>no/10ul</td><td>no/100ml</td><td>× 10,000</td><td>Regulatory standard for microbiology</td></tr>
<tr><td>g/kg, psu, ‰</td><td>ppt</td><td>1 : 1</td><td>All equivalent salinity measures</td></tr>
</table>

<h2>Top Water Types by Volume</h2>
<table>
<tr><th>Type</th><th>Rows</th><th>%</th></tr>
{type_rows}
</table>

<h2>Top Tests by Volume</h2>
<table>
<tr><th>Test</th><th>Rows</th><th>%</th></tr>
{test_rows}
</table>

<h2>Top Units by Volume</h2>
<table>
<tr><th>Unit</th><th>Rows</th><th>%</th></tr>
{unit_dist_rows}
</table>

<h2>Unit Consistency (tests with more than one unit)</h2>
<p><i>Some tests legitimately use multiple units when measuring different
properties (e.g. mg/l for water concentration vs mg/kg for leachable fraction,
or % for dry-weight composition).  Review these to decide if further
filtering is needed for your specific analysis.</i></p>
<table>
<tr><th>Test</th><th>Units found</th><th>Status</th></tr>
{unit_rows}
</table>

<footer>
<p>Report generated by EA Water Quality Data Processor v2.1<br>
Source data: Environment Agency (England) Open Water Quality Archive, 2000–2025</p>
</footer>
</body>
</html>"""

            with open(out_qa, "w", encoding="utf-8") as f:
                f.write(qa_html)

            qa_output = out_qa
            log(f"  ✓  QA report saved : {out_qa.name}")
        except Exception as e:
            log(f"  ⚠  QA report failed: {e}")

    # ==================================================================
    #  CLEAN UP
    # ==================================================================

    try:
        tmp_csv.unlink()
    except Exception:
        pass

    # ==================================================================
    #  FINAL SUMMARY
    # ==================================================================

    log("\n" + "=" * 70)
    log("  PROCESSING COMPLETE")
    log("=" * 70)
    log(f"  Mode           : {mode.upper()}")
    log(f"  Final rows     : {len(df_final):,}")
    log(f"  Columns        : {list(df_final.columns)}")
    log(f"  Years          : {df_final['SourceYear'].min()} – "
        f"{df_final['SourceYear'].max()}")
    log(f"  Water types    : {df_final['Type'].nunique()}")
    log(f"  Tests          : {df_final['Test'].nunique()}")
    log(f"  Units          : {df_final['Unit'].nunique()}")
    log(f"  Sampling points: {df_final['Sampling Point'].nunique():,}")

    if flag_outliers and "outlier_flag" in df_final.columns:
        n_out = int(df_final["outlier_flag"].sum())
        log(f"  Outliers flagged: {n_out:,} "
            f"({n_out / len(df_final) * 100:.2f}%)")

    if "Latitude" in df_final.columns:
        n_coords = df_final["Latitude"].notna().sum()
        log(f"  With lat/lon   : {n_coords:,} "
            f"({n_coords / len(df_final) * 100:.1f}%)")

    log(f"\n  Outputs in: {out_dir}/")
    log(f"    • {out_csv.name}")
    if wrote_parquet:
        log(f"    • {out_pq.name}")
    if stats_output:
        log(f"    • {out_stats.name}")
    if qa_output:
        log(f"    • {out_qa.name}")

    log(f"\n  Rows per year:")
    for yr in sorted(summary.keys()):
        log(f"    {yr}: {summary[yr]:>10,}")

    log(f"\n  Finished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log("=" * 70)

    # ==================================================================
    #  SAVE LOG FILE
    # ==================================================================

    log_path = None
    if save_log:
        with open(out_log, "w", encoding="utf-8") as f:
            f.write(log_buffer.getvalue())
        log_path = out_log
        print(f"\n  ✓  Full log saved : {out_log.name}")

    return {
        "final_rows":    len(df_final),
        "per_year_rows": summary,
        "output_dir":    str(out_dir),
        "csv":           str(out_csv),
        "parquet":       str(out_pq) if wrote_parquet else None,
        "statistics":    str(stats_output) if stats_output else None,
        "qa_report":     str(qa_output) if qa_output else None,
        "log":           str(log_path) if log_path else None,
        "data_quality": {
            "total_raw_rows":        total_raw,
            "duplicates_removed":    n_dupes,
            "unique_sampling_points": df_final["Sampling Point"].nunique(),
            "unique_tests":          df_final["Test"].nunique(),
            "unique_types":          df_final["Type"].nunique(),
            "unique_units":          df_final["Unit"].nunique(),
            "date_range": (
                str(df_final["Date"].min()),
                str(df_final["Date"].max()),
            ),
            "outliers_flagged": (
                int(df_final["outlier_flag"].sum())
                if flag_outliers and "outlier_flag" in df_final.columns
                else 0
            ),
            "records_with_coordinates": int(
                df_final["Latitude"].notna().sum()
                if "Latitude" in df_final.columns else 0
            ),
        },
    }
