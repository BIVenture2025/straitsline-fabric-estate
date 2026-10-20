# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "d2b82ad9-6e34-4b75-b92f-00793a48287f",
# META       "default_lakehouse_name": "LH_StraitsLine",
# META       "default_lakehouse_workspace_id": "1fc42cf1-ef98-4dff-a5ab-22a0cecad731",
# META       "known_lakehouses": [
# META         {
# META           "id": "d2b82ad9-6e34-4b75-b92f-00793a48287f"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # NB_StraitsLine_Generate -- Phase 2 backfill
# 
# Project 30, Fabric E2E Ocean Freight Operations (**StraitsLine**). Generates the
# 60-day synthetic history and loads it into `LH_StraitsLine` schemas `ref`, `ops`
# and `stream`.
# 
# **All data here is SYNTHETIC and declared as such.** Vessels, ports, bookings and
# AIS positions are generated from a fixed seed; port coordinates are real approximate
# positions, every polygon and route line is invented.
# 
# **Before running:** upload `01_Source/generator/straitsline_gen.py` to this
# lakehouse under `Files/generator/`. The notebook is only a driver -- all of the
# logic lives in that module, which is imported, never restated. It is checked against
# SHA256 `343c9d229202fba5f75530134c2efb0d07da3c623cb8251e4fb4746c244dab08` before import, so a stale upload stops the run instead of quietly
# generating different data.
# 
# Parameters come from Variable Library **VL_StraitsLine** at run time; nothing is scaled
# here. `_defect` tags are dropped before ingest so detection logic cannot cheat, and
# the ground truth lands in `Files/ground_truth/`.

# CELL ********************

# Cell 1 -- import the generator from OneLake and prove it is the right build.
import hashlib, os, sys

GEN_DIR = "/lakehouse/default/Files/generator"
EXPECTED_SHA256 = "343c9d229202fba5f75530134c2efb0d07da3c623cb8251e4fb4746c244dab08"
_path = os.path.join(GEN_DIR, "straitsline_gen.py")

if not os.path.exists(_path):
    raise RuntimeError(
        f"generator module not found at {_path}. Upload "
        "01_Source/generator/straitsline_gen.py to Files/generator/ in "
        "LH_StraitsLine (Lakehouse explorer, drag and drop), then re-run this cell.")

_bytes = open(_path, "rb").read()
_got = hashlib.sha256(_bytes).hexdigest()
print("module path    :", _path)
print("module bytes   :", f"{len(_bytes):,}")
print("sha256 found   :", _got)
print("sha256 expected:", EXPECTED_SHA256)
if _got != EXPECTED_SHA256:
    raise RuntimeError(
        "the uploaded generator is not the build this notebook was made for - "
        "re-upload 01_Source/generator/straitsline_gen.py and re-run")

sys.path.insert(0, GEN_DIR)
import straitsline_gen as gen
print("imported", gen.__file__)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 2 -- run the backfill. Everything it does is in the module above:
# read VL_StraitsLine, generate, check the write contract, CREATE SCHEMA, write typed
# Delta tables, write the ground truth to Files/, then reconcile and fail loudly.
summary = gen.run_backfill(spark, notebookutils, varlib_name="VL_StraitsLine")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 3 -- restate the measured totals for the handover.
print("tables written :", len(summary["written"]))
print("rows in lakehouse:", f"{summary['total_rows']:,}")
for _k, _v in sorted(summary["written"].items()):
    print(f"  {_k:24s} {_v:>10,}")
print("time budget    :", summary["time_budget"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 4 -- report the verdict as the run's exit value. An API-triggered run
# cannot read printed output; jobs/execute/instances/{id}?beta=true exposes this
# as properties.exitValue. Reached only if run_backfill() reconciled (it raises
# otherwise), so "RECONCILED" here is the notebook's own verdict, not a guess.
import json
_manifest = json.load(open("/lakehouse/default/Files/ground_truth/injection_manifest.json"))
notebookutils.notebook.exit(json.dumps({
    "verdict": "RECONCILED",
    "module_sha256": EXPECTED_SHA256,
    "total_rows": summary["total_rows"],
    "written": summary["written"],
    "dwell_outlier_containers": _manifest["dwell_outlier_container_count"],
    "defect_counts": _manifest["defect_counts"],
}, sort_keys=True, separators=(",", ":")))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
