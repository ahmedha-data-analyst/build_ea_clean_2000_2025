<img src="logo.png" alt="HydroStar Logo" width="150">

# EA water quality data processor

[![Python Version](https://img.shields.io/badge/Python-%3E%3D_3.9-30343c.svg?logo=python&logoColor=white)](#)
[![Version](https://img.shields.io/badge/Version-2.1.0-a7d730.svg)](#)
[![License](https://img.shields.io/badge/License-CC--BY--4.0-8c919a.svg)](#)

*Environment Agency (England) Open Water Quality Archive — Processor*

**Authors:** Domanique Bridglalsingh, Ahmed Abdalla, Jia Hu, Geyong Min, Xiaohong Li, and Siwei Zheng 
**Website:** [www.hydrostar-eu.com](http://www.hydrostar-eu.com)

---

<h2 style="color: #499823;">Purpose</h2>

The Environment Agency (EA) published annual CSV files of water-quality measurements from 2000 to 2025. Those files are no longer publicly hosted. This script reads the raw yearly CSVs, applies transparent and reproducible cleaning steps, and produces a single, analysis-ready dataset.

<h2 style="color: #499823;">Dependencies</h2>

The script will automatically install missing Python packages using pip if they are not already present. It relies on the following stack:

![Pandas](https://img.shields.io/badge/pandas-30343c.svg?style=for-the-badge&logo=pandas&logoColor=a7d730)
![NumPy](https://img.shields.io/badge/numpy-30343c.svg?style=for-the-badge&logo=numpy&logoColor=a7d730)
![Apache Arrow](https://img.shields.io/badge/pyarrow-30343c.svg?style=for-the-badge&logo=apache&logoColor=a7d730)
![PyProj](https://img.shields.io/badge/pyproj-30343c.svg?style=for-the-badge&logo=python&logoColor=a7d730)
![OpenPyXL](https://img.shields.io/badge/openpyxl-30343c.svg?style=for-the-badge&logo=python&logoColor=a7d730)
![Chardet](https://img.shields.io/badge/chardet-30343c.svg?style=for-the-badge&logo=python&logoColor=a7d730)

<h2 style="color: #499823;">Two output modes</h2>

| Mode | Description |
| :--- | :--- |
| **<span style="color: #499823;">full</span>** | Retains every water-related test, type, and sampling point. |
| **<span style="color: #499823;">electrochemistry</span>** | Retains a focused subset of dissolved metals, ions, pH, conductivity, temperature, and turbidity — the parameters most relevant to electrochemical sensing. |

<h2 style="color: #499823;">How to use</h2>

1. Place all 26 raw CSV files (`2000.csv` … `2025.csv`) in **one** folder.
2. Open the main script in a Jupyter notebook or run it as a standard Python file.
3. Set `RAW_DATA_FOLDER` to the path that contains your CSV files.
4. Set `MODE` to `"full"` or `"electrochemistry"`.
5. Run the script. A new subfolder is created automatically with all outputs.

<h2 style="color: #499823;">Processing pipeline</h2>

For every yearly CSV, the script will systematically apply the following cleaning steps:

* **Column management:** Drops columns that are not needed, such as internal EA IDs and compliance flags. Renames remaining columns to short, human-readable names.
* **Test definitions:** Uses the definition column (`determinand.definition`) for test names, because it is more descriptive than the abbreviated label.
* **Data filtering:** Removes non-quantitative rows where the unit is "coded", "text", or "yes/no". 
* **Sample types:** Removes sample types that are not water-related, including biota, soil, and gas.
* **Frequency filtering:** Removes tests that appear fewer than a specified minimum count across all 26 years (default is 50).
* **Unit standardisation:** Converts units to a consistent baseline (e.g., µg/l → mg/l, ms/cm → uS/cm, FTU → NTU).
* **Geospatial processing:** Converts British National Grid (Easting / Northing) to WGS-84 Latitude / Longitude using the pyproj library. Removes known dummy placeholder coordinates that the EA used for mis-registered samples.
* **Quality control:** Flags potential outliers for key parameters without deleting the records. 
* **Logging:** Prints a detailed log of every action so you can see exactly what happened at every step.

<h2 style="color: #499823;">Outputs</h2>

All processed files are saved automatically in `<RAW_DATA_FOLDER>/EA_processed_output/`.

| File Format | Output Name | Description |
| :--- | :--- | :--- |
| **CSV** | `EA_clean_2000_2025.csv` | The main clean dataset. |
| **Parquet** | `EA_clean_2000_2025.parquet` | The same data in fast columnar format. |
| **Excel** | `EA_statistics_2000_2025.xlsx` | Descriptive statistics workbook. |
| **HTML** | `EA_qa_report.html` | Visual quality-assurance summary. |
| **Text** | `processing_log.txt` | Full text log of every cleaning step. |
