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

# # NB_StraitsLine_Replay -- Phase 3 hot path
# 
# Project 30, Fabric E2E Ocean Freight Operations (**StraitsLine**). Replays the generated
# AIS pings from `LH_StraitsLine.stream.ais_ping` into the **ES_StraitsLine** custom
# endpoint, so the Eventstream, the Eventhouse and the KQL geofence all run on live
# arriving events rather than a static table.
# 
# **All data here is SYNTHETIC and declared as such.**
# 
# ### Before running
# 1. Upload `01_Source/generator/straitsline_gen.py` to `Files/generator/` in
#    `LH_StraitsLine` if it is not already the current build. Checked against SHA256
#    `c4c596343b597f359feb9d93aee2fc9da9a231a7f676926641630c9e9de3a6f5`.
# 2. Run the KQL in `02_Platform/KQL_StraitsLine/geofence.kql` **sections 1 to 3 first**,
#    so `ais_ping_raw` exists before the first event arrives. An Eventstream destination
#    whose table is missing or whose shape disagrees sits in `Warning`, ingests nothing
#    and reports no error -- the events would be discarded silently.
# 3. Supply the custom endpoint connection string (the one that includes `EntityPath=`).
#    **By API (the route used in S4):** read it from the Eventstream topology API,
#    `eventstreams/<id>/sources/<sourceId>/connection` -> `accessKeys.primaryConnectionString`,
#    and pass it as the Text job parameter `CONNECTION_STRING`, together with the Boolean
#    `_inlineInstallationEnabled` = true, or Cell 1's install is skipped. **Interactively:**
#    ES_StraitsLine -> `ais_custom_endpoint` -> **Event Hub** tab, paste into Cell 2, and
#    **do not save the notebook with the value in it.**
# 
# ### Scope
# The full table, all 131,544 pings -- not the single day the S3 kickoff first scoped.
# The exit criterion is "Eventhouse ping counts match generator counts", and the geofence
# reconciles against 300 ground-truth port calls; one day of pings covers about five of
# them, so neither would be gradeable. Owner decision 2026-10-20.
# 
# ### After running
# A successful send is **not** proof of ingestion. Run section 6 of `geofence.kql` and
# read the row counts; that is the only evidence that the hot path works.

# CELL ********************

# Cell 1 -- azure-eventhub is not in the default Spark runtime. This cell must come
# FIRST, above even the parameters: %pip restarts the Python interpreter and every
# variable defined before it is lost (Learn, library-management), so a parameters cell
# above it -- and the value a Job Scheduler run injects beneath that cell -- would be
# gone by the replay cell. Found S4 on the tenant; the offline harness fakes the install
# and cannot see it. It must also be its own cell ABOVE the import: a %pip install in
# the same cell as the import does not take effect for that cell.
# An API- or pipeline-triggered run disables inline installs unless the run passes the
# Boolean job parameter `_inlineInstallationEnabled` = true (Learn, notebook-limitation).
# Built with NotebookBuilder.magic(): %pip is not valid Python, so code() rejects it.
%pip install azure-eventhub --quiet

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

# Cell 2 -- PARAMETERS (a tagged parameters cell). The connection string is passed
# per run as a Job Scheduler parameter, read from the Eventstream topology API
# (sources/<id>/connection) by the caller, so it is never pasted and never saved in the
# definition. Pasting it here interactively still works: clear it before saving.
# Nothing in the repo may ever carry this value (scrub_check.py gates the publish).
CONNECTION_STRING = ""

SOURCE_TABLE = "stream.ais_ping"
LIMIT = None                  # None = the whole table; set an int for a smoke test

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 3 -- import the module from OneLake and prove it is the right build.
import hashlib, os, sys

GEN_DIR = "/lakehouse/default/Files/generator"
EXPECTED_SHA256 = "c4c596343b597f359feb9d93aee2fc9da9a231a7f676926641630c9e9de3a6f5"
_path = os.path.join(GEN_DIR, "straitsline_gen.py")

if not os.path.exists(_path):
    raise RuntimeError(
        f"generator module not found at {_path}. Upload "
        "01_Source/generator/straitsline_gen.py to Files/generator/ in "
        "LH_StraitsLine, then re-run this cell.")

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

# Cell 4 -- replay. All of the work is in the SHA-verified module: it checks the
# connection string has an EntityPath, drops `_defect` so the dirty rows arrive looking
# exactly like clean ones, batches to just under 1 MB, retries on throttling, and raises
# if it sends fewer events than the table holds.
summary = gen.replay_to_eventstream(
    spark,
    connection_string=CONNECTION_STRING,
    source_table=SOURCE_TABLE,
    limit=LIMIT,
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 5 -- report the result as the run's exit value, so an API-triggered run can be
# read without the printed output (jobs/execute/instances/{id}?beta=true exposes this as
# properties.exitValue). Reached only if the module did not raise, so the counts here are
# its own verdict.
#
# "SENT" is deliberately not "INGESTED". This notebook can only prove what left the
# producer. The Eventhouse row count is the proof of delivery and it lives in
# 02_Platform/KQL_StraitsLine/geofence.kql section 6.
import json
notebookutils.notebook.exit(json.dumps({
    "verdict": "SENT",
    "module_sha256": EXPECTED_SHA256,
    "events_sent": summary["events_sent"],
    "rows_in_source": summary["rows_in_source"],
    "batches": summary["batches"],
    "throttle_retries": summary["throttle_retries"],
    "seconds": summary["seconds"],
    "next": "run geofence.kql section 6 and read the Eventhouse row counts",
}, sort_keys=True, separators=(",", ":")))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
