===============================================================================
EA WATER QUALITY DATA PROCESSOR
===============================================================================
**Title** : Environment Agency (England) Open Water Quality Archive — Processor
**Version** : 2.1.0
**Authors** : Domanique Bridglalsingh, Ahmed Abdalla, Jia Hu, Geyong Min, Xiaohong Li, and Siwei Zheng
**Licence** : CC-BY-4.0 (same licence as the underlying EA data)
**Python** : >= 3.9

PURPOSE
-------
The Environment Agency (EA) published annual CSV files of water-quality measurements from 2000 to 2025. Those files are no longer publicly hosted. This script reads the raw yearly CSVs, applies transparent and reproducible cleaning steps, and produces a single, analysis-ready dataset.

TWO OUTPUT MODES
----------------
1. **"full"** – Every water-related test, type, and sampling point.
2. **"electrochemistry"** – A focused subset of dissolved metals, ions, pH, conductivity, temperature, and turbidity — the parameters most relevant to electrochemical sensing.

HOW TO USE
--------------------------------------------------------
1. Create a single folder on your computer and place all 26 raw CSV files (2000.csv … 2025.csv) inside it.
2. Open `run_processor.py` (or scroll to the bottom of the main script if running it directly).
3. Set `RAW_DATA_FOLDER` to the exact path of the folder that contains your CSV files.
4. Set `MODE` to `"full"` or `"electrochemistry"`.
5. Run the script. A new subfolder is created automatically with all outputs.

WHAT THE SCRIPT DOES (step by step)
------------------------------------
For every yearly CSV the script will:
  • Drop columns that are not needed (internal EA IDs, compliance flags …).
  • Rename columns to short, human-readable names.
  • Use the *definition* column (`determinand.definition`) for test names, because it is more descriptive than the abbreviated label.
  • Remove non-quantitative rows (unit = "coded", "text", "yes/no" …).
  • Remove tests that appear fewer than `MIN_TEST_COUNT` times across all 26 years (default 50 — less than 2 per year on average).
  • Remove sample types that are not water-related (biota, soil, gas …).
  • Standardise units (e.g., µg/l → mg/l, ms/cm → uS/cm, FTU → NTU).
  • Convert British National Grid (Easting / Northing) to WGS-84 Latitude / Longitude using the pyproj library.
  • Remove known dummy / placeholder coordinates that the EA used for mis-registered samples.
  • Flag (but NOT delete) potential outliers for key parameters.
  • Print a detailed log of every action so you can see exactly what happened at every step.

OUTPUTS (saved in <RAW_DATA_FOLDER>/EA_processed_output/)
---------
  • **EA_clean_2000_2025.csv** – The main clean dataset.
  • **EA_clean_2000_2025.parquet** – Same data in fast columnar format.
  • **EA_statistics_2000_2025.xlsx** – Descriptive statistics.
  • **EA_qa_report.html** – Visual quality-assurance summary.
  • **processing_log.txt** – Full text log of every cleaning step.

DEPENDENCIES (auto-installed if missing)
-----------
  `pandas`, `numpy`, `pyproj`, `pyarrow`, `openpyxl`, `chardet`. The script will automatically check for and install these silently using pip if they are not already present.
===============================================================================
