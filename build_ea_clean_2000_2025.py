"""
===============================================================================
EA WATER QUALITY DATA PROCESSOR
===============================================================================
Title   : Environment Agency (England) Open Water Quality Archive — Processor
Version : 2.0.0
Authors : Domanique Bridglalsingh, Ahmed Abdalla, Jia Hu, Geyong Minᵃ, Xiaohong Li, and Siwei Zheng
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
        g/l   → mg/l   (× 1 000)
        ppm   → mg/l   (1 : 1 in dilute water)
        FTU   → NTU    (1 : 1)
        ms/cm → µS/cm  (× 1 000)
        Various µS/cm spellings → uS/cm
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

    # Map:  import-name  →  pip-name  (they differ for a few packages)
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
import warnings, sys, io

warnings.filterwarnings("ignore", category=FutureWarning)


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
    log("  EA WATER QUALITY DATA PROCESSOR  v2.0")
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
    # These are biological tissue, soil, gas, waste, and other non-water
    # sample types that occasionally share names with water bodies.
    DROP_TYPE_PATTERN = (
        r"(SEDIMENT|WHOLE ANIMAL|MUSCLE|LIVER|DIGESTIVE GLAND|BIOTA|"
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
    # These are the tests relevant to electrochemical sensing research.
    # Used ONLY when  mode = "electrochemistry".
    # ------------------------------------------------------------------

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
    # These unit labels indicate non-numeric or coded observations.
    # Rows with these units are removed because they cannot be analysed
    # as continuous measurements.
    # ------------------------------------------------------------------

    NON_QUANTITATIVE_UNITS = {
        "coded",      # categorical indicators  (e.g. "No flow / No sample")
        "text",       # free-text descriptions
        "yes/no",     # binary flags            (e.g. "Photo Taken: Yes/No")
        "pres/nf",    # present / not found     (biological presence)
        "pres/nft",   # variant spelling
        "garber c",   # Garber colour class      (ordinal, not continuous)
        "hh.mm",      # clock-time notation
        "ngr",        # National Grid Reference  (a spatial code, not a number)
        "deccafix",   # Decca navigation fix     (obsolete positioning)
    }

    # ==================================================================
    #  CONFIGURATION — NON-QUANTITATIVE TEST FRAGMENTS TO DROP
    # ==================================================================
    # If a test name *contains* any of these fragments it is removed.
    # These are categorical observations, not measurements.
    # ------------------------------------------------------------------

    BAD_TEST_FRAGMENTS = [
        "No flow",
        "No sample",
        "Site Inspection",
        "Present/Not found",
        "Pass/Fail",
        "Population Equivalent",
        "Sampling Frequency",
        "Photo Taken",
        "Weather :",          # weather flags (categorical)
        "Bathing Water Profile",
        "National Grid Reference",
        "Sewage debris",
        "Foam Visible",
        "Colour : Abnormal",
        "Tarry residues",
        "MST Filtration",
        "Time of high tide",
        "Number of beach users",
        "Bathers per 100",
        "Type of flow",
        "Laboratory Sample Number",
        "State tide",
        "Colour (1/0)",
        "Tars/Floatg",
        "OilTypeQual",
        "WEATHER FLAG",
        "Borehole RefPt",
        "Sample Depth",
    ]

    # ==================================================================
    #  CONFIGURATION — DUMMY / WRONG COORDINATES
    # ==================================================================
    # The EA used placeholder coordinate pairs for mis-registered samples.
    # These fall in the North Sea and must be removed so that spatial
    # analyses are not biased.  The dummy sites are:
    #   Easting = 500 000,  Northing = 1, 2, 3, 4, 5, 6, 7, or 8
    # They were labelled things like "DUMMY SITE NO1 FOR GROUNDWATER
    # SAMPLES, SAMPLES REGISTERED INCORRECTLY".
    # ------------------------------------------------------------------

    DUMMY_EASTING  = 500_000
    DUMMY_NORTHINGS = {1, 2, 3, 4, 5, 6, 7, 8}

    # ==================================================================
    #  CONFIGURATION — OUTLIER THRESHOLDS
    # ==================================================================
    # Physically plausible ranges for key parameters.  Values outside
    # these ranges are FLAGGED (outlier_flag = True) but NOT removed.
    # The user can filter them out later if they wish.
    #
    # IMPORTANT NOTE ON NEGATIVE TEMPERATURES:
    # Water in UK rivers and lakes can reach temperatures very close to
    # 0 °C in winter and, under certain conditions (super-cooling near
    # ice formation, instrument calibration drift), readings just below
    # 0 °C have been recorded.  Rather than silently deleting these
    # values (which discards potentially valid extreme-cold events), we
    # FLAG them as outliers and let the user decide.  This is more
    # transparent and reproducible than blanket removal.
    # ------------------------------------------------------------------

    OUTLIER_THRESHOLDS = {
        "Temperature of Water":        (-5,    45),     # °C — allows slight sub-zero
        "pH":                          ( 1,    14),     # pH units
        "Conductivity at 25 C":        ( 0, 80_000),   # µS/cm
        "Conductivity at 20 C":        ( 0, 80_000),   # µS/cm
        "Salinity : In Situ":          ( 0,    50),     # ppt
        "Solids, Suspended at 105 C":  ( 0, 50_000),   # mg/l
        "Oxygen, Dissolved, % Saturation": (0, 250),    # %
        "Oxygen, Dissolved as O2":     ( 0,    25),     # mg/l
        "Ammoniacal Nitrogen as N":    ( 0, 1_000),     # mg/l
        "Turbidity":                   ( 0, 10_000),    # NTU
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
          µg/l  → mg/l   (÷ 1 000)
          g/l   → mg/l   (× 1 000)
          ppm   → mg/l   (equivalent for dilute aqueous solutions)
          FTU   → NTU    (1 : 1  — both are nephelometric turbidity)
          ms/cm → µS/cm  (× 1 000)
          Various µS/cm spellings  → uS/cm

        All comparisons are case-insensitive to handle inconsistent
        capitalisation in the raw data.
        """
        if "Test" not in df.columns or "Unit" not in df.columns:
            return df

        u = df["Unit"].astype(str).str.strip()

        # --- Conductivity unit spelling variants → uS/cm ---------------
        u = (u.str.replace("µ",  "u", regex=False)
              .str.replace("μ",  "u", regex=False)
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

        # --- µg/l  →  mg/l  (÷ 1 000) ---------------------------------
        mask_ug = df["Unit"].str.lower() == "ug/l"
        if mask_ug.any():
            df.loc[mask_ug, "result"] = (
                pd.to_numeric(df.loc[mask_ug, "result"], errors="coerce") / 1_000
            )
            df.loc[mask_ug, "Unit"] = "mg/l"

        # --- g/l  →  mg/l  (× 1 000) ----------------------------------
        mask_gl = df["Unit"].str.lower() == "g/l"
        if mask_gl.any():
            df.loc[mask_gl, "result"] = (
                pd.to_numeric(df.loc[mask_gl, "result"], errors="coerce") * 1_000
            )
            df.loc[mask_gl, "Unit"] = "mg/l"

        # --- ppm  →  mg/l  (1 : 1 in dilute water) --------------------
        mask_ppm = df["Unit"].str.lower() == "ppm"
        if mask_ppm.any():
            df.loc[mask_ppm, "Unit"] = "mg/l"

        # --- FTU  →  NTU  (1 : 1) -------------------------------------
        mask_ftu = (df["Test"] == "Turbidity") & (df["Unit"].str.lower() == "ftu")
        if mask_ftu.any():
            df.loc[mask_ftu, "Unit"] = "NTU"

        # --- ntu  →  NTU  (capitalisation) -----------------------------
        mask_ntu = df["Unit"].str.lower() == "ntu"
        if mask_ntu.any():
            df.loc[mask_ntu, "Unit"] = "NTU"

        # --- ms/cm  →  uS/cm  (× 1 000) -------------------------------
        mask_ms = df["Unit"].str.lower() == "ms/cm"
        if mask_ms.any():
            df.loc[mask_ms, "result"] = (
                pd.to_numeric(df.loc[mask_ms, "result"], errors="coerce") * 1_000
            )
            df.loc[mask_ms, "Unit"] = "uS/cm"

        # --- Salinity variants → ppt ----------------------------------
        mask_sal = (
            (df["Test"] == "Salinity : In Situ")
            & df["Unit"].str.lower().isin(["g/l", "psu", "‰"])
        )
        if mask_sal.any():
            df.loc[mask_sal, "Unit"] = "ppt"

        return df

    # ==================================================================
    #  OUTLIER FLAGGER
    # ==================================================================

    def _flag_outliers(df: pd.DataFrame) -> pd.DataFrame:
        """
        Add an  outlier_flag  column.  Values outside physically
        plausible thresholds are set to True.  Nothing is deleted.
        """
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
        """
        Replace  Easting / Northing  (British National Grid, EPSG:27700)
        with  Latitude / Longitude  (WGS-84, EPSG:4326).

        To avoid doing millions of slow per-row conversions, we first
        extract the unique coordinate pairs, convert them once, then
        merge the results back.  This is much faster.
        """
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

        # Drop the original Easting / Northing columns
        df = df.drop(columns=["Easting", "Northing"])

        log(f"    {len(unique):,} unique coordinate pairs converted.")
        log(f"    Rows with valid lat/lon: "
            f"{df['Latitude'].notna().sum():,} / {n_before:,}")

        return df

    # ==================================================================
    #  CHUNK CLEANER  (processes one chunk from one yearly CSV)
    # ==================================================================

    def _clean_chunk(
        raw: pd.DataFrame,
        year_hint: int,
        test_filter: Optional[set],
    ) -> pd.DataFrame:
        """
        Apply all cleaning rules to a single chunk of raw data.

        Parameters
        ----------
        raw         : one chunk read from pd.read_csv
        year_hint   : the year of the source file (used if Date is missing)
        test_filter : set of test names to keep, or None to keep all
        """
        df = raw.copy()

        # --- Drop unneeded columns ------------------------------------
        drop_cols = [
            "@id",
            "sample.samplingPoint",
            "sample.samplingPoint.notation",
            "resultQualifier.notation",
            "codedResultInterpretation.interpretation",
            "determinand.label",            # we use .definition instead
            "sample.isComplianceSample",
            "sample.purpose.label",
            "determinand.notation",
        ]
        existing = [c for c in drop_cols if c in df.columns]
        if existing:
            df = df.drop(columns=existing)

        # --- Rename columns to short names ----------------------------
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
            # First exclude anything matching the drop pattern
            df = df[
                ~df["Type"]
                .astype(str)
                .str.contains(DROP_TYPE_PATTERN, case=False, na=False)
            ]
            # Then keep only recognised water types
            df = df[df["Type"].isin(WATER_TYPES)]

        # --- Remove non-quantitative units ----------------------------
        if "Unit" in df.columns:
            df = df[~df["Unit"].str.strip().str.lower().isin(NON_QUANTITATIVE_UNITS)]

        # --- Remove non-quantitative test fragments -------------------
        if "Test" in df.columns:
            pattern = "|".join(BAD_TEST_FRAGMENTS)
            bad_test_mask = (
                df["Test"].astype(str).str.contains(pattern, case=False, na=False)
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

        # --- Flag outliers (if requested) -----------------------------
        if flag_outliers:
            df = _flag_outliers(df)

        # --- Arrange columns in a readable order ----------------------
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
            # Try patterns like  2000_data.csv  etc.
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
    #  DETERMINE WHICH TESTS TO KEEP  (mode-dependent)
    # ==================================================================

    if mode == "electrochemistry":
        test_filter = ELECTROCHEMISTRY_TESTS
        log(f"Mode = ELECTROCHEMISTRY  →  keeping {len(test_filter)} "
            f"pre-defined tests.\n")
    else:
        # In "full" mode we keep every test that has at least
        # min_test_count records.  We do a quick first pass to count.
        if min_test_count > 0:
            log(f"Mode = FULL  →  first pass: counting tests to drop "
                f"those with < {min_test_count} total records …")
            test_counts: Dict[str, int] = {}
            for y, csv_path in year_files:
                for chunk in pd.read_csv(
                    csv_path, chunksize=chunksize, low_memory=False,
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
            test_filter = None  # keep everything
            log("Mode = FULL, min_test_count = 0  →  keeping ALL tests.\n")

    # ==================================================================
    #  MAIN PROCESSING LOOP  (stream chunks, clean, write to temp CSV)
    # ==================================================================

    # Output paths
    tag = "electrochemistry" if mode == "electrochemistry" else "full"
    out_csv  = out_dir / f"EA_clean_2000_2025_{tag}.csv"
    out_pq   = out_dir / f"EA_clean_2000_2025_{tag}.parquet"
    out_stats = out_dir / f"EA_statistics_2000_2025_{tag}.xlsx"
    out_qa   = out_dir / f"EA_qa_report_{tag}.html"
    out_log  = out_dir / f"EA_processing_log_{tag}.txt"
    tmp_csv  = out_dir / "_tmp_stream.csv"

    # Clean slate
    for p in (out_csv, out_pq, tmp_csv):
        if p.exists():
            p.unlink()

    summary: Dict[int, int] = {}
    total_streamed    = 0
    header_written    = False
    total_raw         = 0
    total_type_dropped = 0
    total_unit_dropped = 0
    total_test_dropped = 0
    total_dummy_dropped = 0

    for y, csv_path in year_files:
        log(f"── Processing {y}  ({csv_path.name}) " + "─" * 30)

        n_year_clean = 0
        n_year_raw   = 0
        n_chunks     = 0

        enc = None
        # Auto-detect encoding if needed
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
            raw_rows = len(chunk)
            n_year_raw += raw_rows
            total_raw  += raw_rows

            # Count what we drop for reporting
            pre_type = len(chunk)
            # We cannot count exactly per-category in streaming mode
            # without duplicating logic, so we count the net result.

            cleaned = _clean_chunk(chunk, year_hint=y, test_filter=test_filter)
            clean_rows = len(cleaned)

            if not cleaned.empty:
                cleaned.to_csv(
                    tmp_csv, mode="a", index=False,
                    header=(not header_written)
                )
                header_written = True
                n_year_clean  += clean_rows
                total_streamed += clean_rows

            n_chunks += 1

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
    #  FINAL PROCESSING  (dedup, coordinate conversion, save)
    # ==================================================================

    log("Loading streamed data for final processing …")
    df_all = pd.read_csv(tmp_csv, low_memory=False, parse_dates=["Date"])
    log(f"  Rows loaded: {len(df_all):,}")

    # --- Final type safety net ----------------------------------------
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

    # --- Drop exact duplicates ----------------------------------------
    key_cols = ["Date", "Sampling Point", "Type", "Test"]
    d0 = len(df_all)
    df_all = df_all.drop_duplicates(subset=key_cols, keep="first")
    n_dupes = d0 - len(df_all)
    log(f"  Duplicates removed: {n_dupes:,}")

    # --- Ensure result is numeric and non-null ------------------------
    df_all["result"] = pd.to_numeric(df_all["result"], errors="coerce")
    n_nan = df_all["result"].isna().sum()
    df_all = df_all[df_all["result"].notna()]
    if n_nan:
        log(f"  NaN results dropped: {n_nan:,}")

    # --- Season dtype -------------------------------------------------
    df_all["Season"] = pd.Categorical(
        df_all["Season"], categories=SEASON_CATS, ordered=True
    )

    # --- Convert coordinates ------------------------------------------
    df_final = _convert_coordinates(df_all)

    log(f"\n  Final dataset: {len(df_final):,} rows × "
        f"{len(df_final.columns)} columns")
    log(f"  Unique sampling points : {df_final['Sampling Point'].nunique():,}")
    log(f"  Unique tests           : {df_final['Test'].nunique()}")
    log(f"  Unique water types     : {df_final['Type'].nunique()}")
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
    #  STATISTICAL SUMMARY  (optional)
    # ==================================================================

    stats_output = None
    if generate_stats:
        log("\nGenerating statistics …")
        try:
            with pd.ExcelWriter(out_stats, engine="openpyxl") as writer:

                # Sheet 1 — descriptive stats per test
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
                test_stats.to_excel(
                    writer, sheet_name="Test_Statistics", index=False
                )

                # Sheet 2 — stats per type × test
                type_stats = (
                    df_final.groupby(["Type", "Test"])["result"]
                    .agg(["count", "mean", "median", "std"])
                    .round(4)
                    .reset_index()
                )
                type_stats.to_excel(
                    writer, sheet_name="Type_Test_Stats", index=False
                )

                # Sheet 3 — seasonal stats
                season_stats = (
                    df_final.groupby(["Season", "Test"])["result"]
                    .agg(["count", "mean", "median"])
                    .round(4)
                    .reset_index()
                )
                season_stats.to_excel(
                    writer, sheet_name="Seasonal_Stats", index=False
                )

                # Sheet 4 — coverage summary
                coverage = pd.DataFrame({
                    "Metric": [
                        "Total Rows",
                        "Unique Sampling Points",
                        "Unique Tests",
                        "Unique Types",
                        "Date Range Start",
                        "Date Range End",
                        "Years Covered",
                        "Mode",
                    ],
                    "Value": [
                        len(df_final),
                        df_final["Sampling Point"].nunique(),
                        df_final["Test"].nunique(),
                        df_final["Type"].nunique(),
                        str(df_final["Date"].min().date()),
                        str(df_final["Date"].max().date()),
                        df_final["SourceYear"].nunique(),
                        mode.upper(),
                    ],
                })
                coverage.to_excel(writer, sheet_name="Coverage", index=False)

                # Sheet 5 — outlier summary (if flagging is on)
                if flag_outliers and "outlier_flag" in df_final.columns:
                    outlier_df = df_final[df_final["outlier_flag"]]
                    if not outlier_df.empty:
                        outlier_summary = (
                            outlier_df.groupby(["Test", "Type"])
                            .agg(
                                count=("result", "size"),
                                min_val=("result", "min"),
                                max_val=("result", "max"),
                            )
                            .reset_index()
                        )
                        outlier_summary.to_excel(
                            writer, sheet_name="Outliers", index=False
                        )

                # Sheet 6 — rows per year
                year_counts = (
                    df_final.groupby("SourceYear")
                    .size()
                    .reset_index(name="rows")
                )
                year_counts.to_excel(
                    writer, sheet_name="Rows_Per_Year", index=False
                )

            stats_output = out_stats
            log(f"  ✓  Statistics saved : {out_stats.name}")
        except Exception as e:
            log(f"  ⚠  Statistics failed: {e}")

    # ==================================================================
    #  QA REPORT  (optional)
    # ==================================================================

    qa_output = None
    if generate_qa_report:
        log("\nGenerating QA report …")
        try:
            # Build the HTML report
            n_outliers = 0
            pct_outliers = 0
            if flag_outliers and "outlier_flag" in df_final.columns:
                n_outliers = int(df_final["outlier_flag"].sum())
                pct_outliers = (n_outliers / len(df_final)) * 100

            # Top 15 types
            type_rows = ""
            for typ, cnt in df_final["Type"].value_counts().head(15).items():
                pct = cnt / len(df_final) * 100
                type_rows += (
                    f"<tr><td>{typ}</td><td>{cnt:,}</td>"
                    f"<td>{pct:.1f}%</td></tr>\n"
                )

            # Top 20 tests
            test_rows = ""
            for tst, cnt in df_final["Test"].value_counts().head(20).items():
                pct = cnt / len(df_final) * 100
                test_rows += (
                    f"<tr><td>{tst}</td><td>{cnt:,}</td>"
                    f"<td>{pct:.1f}%</td></tr>\n"
                )

            # Unit consistency
            unit_rows = ""
            unit_per_test = df_final.groupby("Test")["Unit"].nunique()
            multi = unit_per_test[unit_per_test > 1]
            if len(multi) == 0:
                unit_rows = (
                    "<tr><td colspan='3'>✓ All tests have a single "
                    "consistent unit</td></tr>"
                )
            else:
                for tst, n_u in multi.items():
                    units = ", ".join(
                        df_final[df_final["Test"] == tst]["Unit"].unique()
                    )
                    unit_rows += (
                        f"<tr class='warn'><td>{tst}</td>"
                        f"<td>{units}</td><td>⚠ {n_u} units</td></tr>\n"
                    )

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

<h2>Unit Consistency</h2>
<table>
<tr><th>Test</th><th>Units found</th><th>Status</th></tr>
{unit_rows}
</table>

<footer>
<p>Report generated by EA Water Quality Data Processor v2.0<br>
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
    #  CLEAN UP TEMP FILE
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

    # ==================================================================
    #  RETURN SUMMARY DICT
    # ==================================================================

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
