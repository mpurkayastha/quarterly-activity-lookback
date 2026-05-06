"""
run_all.py — run the full quarterly activity lookback pipeline end-to-end.

Usage:
    python run_all.py

Make sure you've updated config.py for the current quarter before running.
Requires: Salesforce CLI (sf) authenticated, SLACK_BOT_TOKEN env var set.
"""

import subprocess, sys

steps = [
    ("Fetching users",           "step1_fetch_users.py"),
    ("Exporting activities",     "step2_export_activities.py"),
    ("Analyzing opp-linked hrs", "step3_analyze.py"),
    ("SE_Activity breakdown",    "step4_se_activity.py"),
    ("Sending to Slack",         "step5_send_slack.py"),
]

for label, script in steps:
    print(f"\n{'='*60}")
    print(f"  {label}")
    print(f"{'='*60}")
    result = subprocess.run([sys.executable, script])
    if result.returncode != 0:
        print(f"\nERROR: {script} failed with exit code {result.returncode}")
        print("Fix the issue above and re-run from this step.")
        sys.exit(result.returncode)

print("\nDone. All steps completed successfully.")
