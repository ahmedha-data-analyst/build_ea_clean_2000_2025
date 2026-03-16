<div align="center">

# EA Water Quality Data Processor

[![Python Version](https://img.shields.io/badge/Python-3.9+-blue.svg?logo=python&logoColor=white)](#)
[![Version](https://img.shields.io/badge/Version-2.1.0-success.svg)](#)
[![License](https://img.shields.io/badge/License-CC--BY--4.0-lightgrey.svg)](#)

*Environment Agency (England) Open Water Quality Archive — Processor*

**Authors:** Domanique Bridglalsingh, Ahmed Abdalla, Jia Hu, Geyong Min, Xiaohong Li, and Siwei Zheng

</div>

---

## Purpose

The Environment Agency (EA) published annual CSV files of water-quality measurements from 2000 to 2025. Those files are no longer publicly hosted. This script reads the raw yearly CSVs, applies transparent and reproducible cleaning steps, and produces a single, analysis-ready dataset.

## Dependencies

The script will automatically install missing Python packages using pip if they are not already present. It relies on the following stack:

![Pandas](https://img.shields.io/badge/pandas-%23150458.svg?style=for-the-badge&logo=pandas&logoColor=white)
![NumPy](https://img.shields.io/badge/numpy-%23013243.svg?style=for-the-badge&logo=numpy&logoColor=white)
![Apache Arrow](https://img.shields.io/badge/pyarrow-%23D22128.svg?style=for-the-badge&logo=apache&logoColor=white)
![PyProj](https://img.shields.io/badge/pyproj-%233776AB.svg?style=for-the-badge&logo=python&logoColor=white)
![OpenPyXL](https://img.shields.io/badge/openpyxl-%233776AB.svg?style=for-the-badge&logo=python&logoColor=white)
![Chardet](https://img.shields.io/badge/chardet-%233776AB.svg?style=for-the-badge&logo=python&logoColor=white)

## Two Output Modes

| Mode | Description |
| :--- | :--- |
| **`full`** | Retains every water-related test, type, and sampling point. |
| **`electrochemistry`** | Retains a focused subset of dissolved metals, ions, pH, conductivity, temperature, and turbidity — the parameters most relevant to electrochemical sensing. |

## How to Use

1. Place all 26 raw CSV files (`2000.csv` … `2025.csv`) in **one** folder.
2. Open the main script in a Jupyter notebook or run it as a standard Python file.
3. Set `RAW_DATA_FOLDER` to the path that contains your CSV files.
4. Set `MODE` to `"full"` or `"electrochemistry"`.
5. Run the script. 

A new subfolder is created automatically with all outputs.

## Processing Pipeline

For every yearly CSV, the script will systematically apply the following cleaning steps:

* **Column Management:** Drops columns that are not needed, such as internal EA IDs and compliance flags. Renames remaining columns to short, human-readable names.
* **Test Definitions:** Uses the definition column (`determinand.definition`) for test names, because it is more descriptive than the abbreviated label.
* **Data Filtering:** Removes non-quantitative rows where the unit is "coded", "text", or "yes/no". 
* **Sample Types:** Removes sample types that are not water-related, including biota, soil, and gas.
* **Frequency Filtering:** Removes tests that appear fewer than a specified minimum count across all 26 years (default is 50).
* **Unit Standardisation:** Converts units to a consistent baseline (e.g., µg/l → mg/l, ms/cm → uS/cm, FTU → NTU).
* **Geospatial Processing:** Converts British National Grid (Easting / Northing) to WGS-84 Latitude / Longitude using the pyproj library. Removes known dummy placeholder coordinates that the EA used for mis-registered samples.
* **Quality Control:** Flags potential outliers for key parameters without deleting the records. 
* **Logging:** Prints a detailed log of every action so you can see exactly what happened at every step.

## Outputs

All processed files are saved automatically in `<RAW_DATA_FOLDER>/EA_processed_output/`.

| File Format | Output Name | Description |
| :--- | :--- | :--- |
| **CSV** | `EA_clean_2000_2025.csv` | The main clean dataset. |
| **Parquet** | `EA_clean_2000_2025.parquet` | The same data in fast columnar format. |
| **Excel** | `EA_statistics_2000_2025.xlsx` | Descriptive statistics workbook. |
| **HTML** | `EA_qa_report.html` | Visual quality-assurance summary. |
| **Text** | `processing_log.txt` | Full text log of every cleaning step. |
