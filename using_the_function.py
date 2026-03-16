# ============================================================================
# USAGE  —  HOW TO RUN THIS SCRIPT
# ============================================================================
#
# Simply change the two settings below and run.
#
#   RAW_DATA_FOLDER  →  the folder that contains your yearly CSV files
#                        (2000.csv, 2001.csv, …, 2025.csv).
#
#   MODE             →  "full"             for the complete water-quality dataset
#                        "electrochemistry" for the focused electrochemistry subset
#
# A new subfolder called  EA_processed_output/  is created inside your
# RAW_DATA_FOLDER automatically.  All outputs go there.
# ============================================================================

if __name__ == "__main__":
 
    # ── SETTINGS (EDIT THESE) ─────────────────────────────────────────
    RAW_DATA_FOLDER = "."          # <-- put the path to your CSV folder here
    MODE            = "full"       # <-- "full" or "electrochemistry"
    # ──────────────────────────────────────────────────────────────────
 
    result = build_ea_clean_2000_2025(
        input_dir       = RAW_DATA_FOLDER,
        mode            = MODE,
        years           = range(2000, 2026),
        chunksize       = 250_000,
        min_test_count  = 50,
        flag_outliers   = True,
        generate_stats  = True,
        generate_qa_report = True,
        save_log        = True,
    )
 
    print("\n" + "─" * 60)
    print("QUICK SUMMARY")
    print("─" * 60)
    print(f"  Final rows : {result['final_rows']:,}")
    print(f"  Output dir : {result['output_dir']}")
    print(f"\n  Files created:")
    for key in ["csv", "parquet", "statistics", "qa_report", "log"]:
        if result.get(key):
            print(f"    • {Path(result[key]).name}")
    print("─" * 60)
