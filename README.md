# EA Water Quality Data Processor

*Environment Agency (England) Open Water Quality Archive — Processor*

**Version:** 2.1.0  
**Authors:** Domanique Bridglalsingh, Ahmed Abdalla, Jia Hu, Geyong Min, Xiaohong Li, and Siwei Zheng  
**Licence:** CC-BY-4.0 (same licence as the underlying EA data)  
**Python:** >= 3.9  

## Purpose
The Environment Agency (EA) published annual CSV files of water-quality measurements from 2000 to 2025. Those files are no longer publicly hosted. This script reads the raw yearly CSVs, applies transparent and reproducible cleaning steps, and produces a single, analysis-ready dataset.

## Two Output Modes
1. **`full`** – Retains every water-related test, type, and sampling point.
2. **`electrochemistry`** – A focused subset of dissolved metals, ions, pH, conductivity, temperature, and turbidity — the parameters most relevant to electrochemical sensing.

## How to Use
1. Place all 26 raw CSV files (`2000.csv` … `2025.csv`) in ONE folder.
2. Open this script in a Jupyter notebook or run it as a standard Python file.
3. Set `RAW_DATA_FOLDER` to the path that contains your CSV files.
4. Set `MODE` to `"full"` or `"electrochemistry"`.
5. Run the script. A new subfolder is created automatically with all outputs.

## What the Script Does (Step-by-Step)
For every yearly CSV the script will:
* **Clean & Filter:** Drop internal columns (EA IDs, compliance flags) and remove non-quantitative rows (unit = "coded", "text", "yes/no"). It renames columns to short, human-readable names and uses the definition column (`determinand.definition`) for test names for better clarity.
* **Filter Types:** Removes sample types that are not water-related (biota, soil, gas).
* **Remove Rare Tests:** Removes tests that appear fewer than `MIN_TEST_COUNT` times across all 26 years (default 50).
* **Standardise Units:** Converts units to a consistent standard (e.g., µg/l → mg/l, FTU → NTU, ms/cm → µS/cm).
* **Geospatial Processing:** Converts British National Grid (Easting / Northing) to WGS-84 Latitude / Longitude using the `pyproj` library, and removes known dummy/placeholder coordinates.
* **Quality Assurance:** Flags (but does NOT delete) potential outliers for key parameters.
* **Logging:** Prints a detailed log of every action so you can see exactly what happened at every step.

## Outputs
All files are saved automatically in `<RAW_DATA_FOLDER>/EA_processed_output/`:
* `EA_clean_2000_2025.csv` – The main clean dataset.
* `EA_clean_2000_2025.parquet` – The same data in a fast columnar format.
* `EA_statistics_2000_2025.xlsx` – Descriptive statistics workbook.
* `EA_qa_report.html` – Visual quality-assurance summary.
* `processing_log.txt` – Full text log of every cleaning step.

## Dependencies
The script will automatically check for and install missing dependencies if they are not already present. These include:
`pandas`, `numpy`, `pyproj`, `pyarrow`, `openpyxl`, `chardet`.
