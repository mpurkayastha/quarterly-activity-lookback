# Quarterly Activity Lookback — AF vs D360

Analyzes Agentforce (AF) vs Data 360 (D360) deal coverage hours for John DeFoe's AFD360 org in Salesforce org62, with YoY comparison. Sends results to Slack.

## What it produces

- **Opp-linked hours per person** — activities logged against opportunities, classified as D360/AF/Both/Neither
- **Region × manager breakdown** — AMER/EMEA/APAC/LATAM/WW totals per manager (Aiysha Mubarik, Salman Mian, Teddy Griffin, Anil Dindigal)
- **SE_Activity total utilization** — all activities (not just opp-linked) from CRM Analytics, showing D360/AF/Both/Other per person
- **"Other" drill-down** — breaks out what's in Other: internal meetings, non-D360/AF opp coverage, account-only meetings, no WhatId, etc.

## Prerequisites

1. **Salesforce CLI** (`sf`) installed and authenticated:
   ```bash
   sf org login web --alias mpurkayastha@salesforce.com
   sf org display --target-org mpurkayastha@salesforce.com
   ```

2. **Python 3.9+** — uses only stdlib (csv, json, subprocess, urllib, re, collections)

3. **Slack Bot Token** with `chat:write` scope:
   ```bash
   export SLACK_BOT_TOKEN=xoxb-your-token-here
   ```

## Setup

1. Clone the repo and open `config.py`
2. Update these values for the current quarter:

   ```python
   TARGET_ORG       = "you@salesforce.com"   # your sf CLI alias
   DEFOE_MANAGER_ID = "00530000009MYBnAAO"    # manager's Salesforce User ID
   SLACK_CHANNEL    = "D0139UAK29J"           # your Slackbot DM channel ID

   CURRENT_PERIOD   = "FY28Q1"
   PRIOR_PERIOD     = "FY27Q1"
   CURRENT_START    = "2027-02-01"
   CURRENT_END      = "2027-04-30"
   PRIOR_START      = "2026-02-01"
   PRIOR_END        = "2026-04-30"

   SE_CURRENT_YEAR  = "2028"   # string
   SE_CURRENT_QTR   = "1"      # string
   SE_PRIOR_YEAR    = "2027"
   SE_PRIOR_QTR     = "1"
   ```

3. If the CRM Analytics dataset was republished, update `SE_DATASET` with the new version ID.

## Usage

### Run everything at once
```bash
python run_all.py
```

### Run steps individually
```bash
python step1_fetch_users.py       # pulls org users + geo regions
python step2_export_activities.py # exports Events, Tasks, OLI SKUs via Bulk API
python step3_analyze.py           # classifies opps, rolls up hours per person
python step4_se_activity.py       # queries CRM Analytics for total utilization
python step5_send_slack.py        # sends all results to Slack
```

### Claude Code skill
If you use Claude Code, run `/quarterly-activity-lookback` and tell Claude the quarters to analyze. It will walk through all steps interactively.

## How classification works

Each opportunity is classified based on its OpportunityLineItem product names/families/codes:

| Category | Matched products |
|---|---|
| **D360** | Data Cloud, D360, Data 360, DataCloud, CDP, Flex Credit, Einstein Analytics, Data Stream, Identity Resolution, Customer Data Platform |
| **AF** | Agentforce, SELA, Einstein 1, Agent, A4X, A1E, AELA, Autonomous |
| **Both** | Opp has SKUs matching both D360 and AF |
| **Neither / Other** | Everything else |

Multi-SKU opps use a merge rule: Both wins > D360/AF > Neither.

## Understanding "Other" in SE_Activity

"Other" = total hours minus D360+AF+Both. It breaks down as:

| Bucket | Typical % | What it is |
|---|---|---|
| Internal meetings | ~36% | 1:1s, team standups, internal planning, prep calls |
| Non-D360/AF opp meetings | ~13% | Customer meetings on Core Cloud, MuleSoft, Sales Cloud opps |
| Account-only meetings | ~14% | Customer meetings logged to Account, not an Opp |
| No WhatId | ~13% | Customer meetings with no related record logged |
| Case/DSR/StratInit/Campaign | ~2% | Activities linked to non-opp objects |
| Blank/unclassified | ~10% | No Customer_Related flag set |

> **Note**: Account/no-WhatId sub-buckets are only populated for recent periods. In prior-year data, all non-opp customer time rolls into "opp not tagged D360/AF."

## Key SE_Activity details

- Dataset ID: `0Fb30000000TNFvCAO` (version `0Fced000009QlCbCAK`)
- DeFoe is at manager level L05 in SE_Activity hierarchy (CEO=L01 → DeFoe=L05)
- Fiscal year and quarter are **strings** in SAQL filters: `"2027"`, `"1"`
- `Full.OppId` links activities to opportunities; `-BLANK-` = no opp
- `Customer_Related` values: `"Customer Related"`, `"Non Customer Related"`, `-BLANK-`

## Output files

All written to `/tmp/`:

| File | Contents |
|---|---|
| `defoe_users.csv` | Active users under manager (Id, Name, Title, ManagerId) |
| `defoe_users_geo.csv` | User geo regions from sfbase__Region__c |
| `events_current_bulk.csv` | Events for current quarter |
| `events_prior_bulk.csv` | Events for prior quarter |
| `tasks_current_bulk.csv` | Tasks for current quarter |
| `tasks_prior_bulk.csv` | Tasks for prior quarter |
| `oli_skus.csv` | OpportunityLineItems for all opp IDs |
| `q1_activity_lookback.csv` | Per-person opp-linked hours (D360/AF/Both/Neither) |
| `se_activity_breakdown.csv` | Per-person SE_Activity hours (D360/AF/Both/Other) |
| `se_activity_other_breakdown.csv` | Org-level Other sub-category breakdown |
