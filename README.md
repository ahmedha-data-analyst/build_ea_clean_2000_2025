# EA WATER QUALITY DATA PROCESSOR

**Title** : Environment Agency (England) Open Water Quality Archive — Processor
**Version** : 2.1.0
**Authors** : Domanique Bridglalsingh, Ahmed Abdalla, Jia Hu, Geyong Min, Xiaohong Li, and Siwei Zheng
**Python** : >= 3.9

## WHAT THIS DOES
The Environment Agency (EA) published annual CSV files of water-quality measurements from 2000 to 2025. This script takes those 26 massive, messy raw CSV files and automatically cleans them, fixes the coordinates, standardizes the measurement units, and combines them into one ready-to-use dataset. 

It even installs the required Python libraries for you automatically!

## IDIOTPROOF QUICKSTART GUIDE
Follow these exact steps to run the cleaner:

**Step 1: Get the data ready**
1. Create a new folder on your computer. Let's call it `EA_Raw_Data`.
2. Put all 26 of your raw CSV files (`2000.csv` through `2025.csv`) into this `EA_Raw_Data` folder. Do not put them in sub-folders.

**Step 2: Tell the script where your data is**
1. Open the file named `run_processor.py` (or scroll to the very bottom of `ea_water_quality_processor.py`).
2. Look for the line that says `RAW_DATA_FOLDER = "."`.
3. Change `"."` to the exact path of the folder you created in Step 1. 
   * *Example:* `RAW_DATA_FOLDER = "C:/Users/YourName/Desktop/EA_Raw_Data"`

**Step 3: Choose your mode**
Find the line that says `MODE = "full"`. Leave it as "full" to keep everything, or change it to `"electrochemistry"` if you only want data for dissolved metals, ions, pH, conductivity, temperature, and turbidity.

**Step 4: Run it!**
Open your terminal (or command prompt) and run the script:
`python ea_water_quality_processor.py`

Go grab a coffee. The script will print a detailed log showing you exactly what it is doing at every step. 

## WHERE DOES MY CLEAN DATA GO?
When the script finishes, it will automatically create a new folder inside your raw data folder called `EA_processed_output`. Inside, you will find:
* **The clean data:** `EA_clean_2000_2025.csv` and a faster `.parquet` version.
* **The stats:** `EA_statistics_2000_2025.xlsx` (Excel file with descriptive statistics).
* **The health check:** `EA_qa_report.html` (Double-click this to open a visual report in your web browser).
* **The receipt:** `processing_log.txt` (A text file showing everything the script did).
