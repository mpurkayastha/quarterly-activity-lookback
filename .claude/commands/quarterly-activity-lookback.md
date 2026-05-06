# Quarterly Activity Lookback — AF vs D360

Runs the full FY Q1 activity analysis for John DeFoe's AFD360 org, comparing Agentforce (AF) vs Data 360 (D360) deal coverage hours, then sends results to Slackbot.

**Usage:** `/quarterly-activity-lookback` — then tell me the FY quarter to analyze (e.g. "FY28 Q1") and the comparison quarter (e.g. "FY27 Q1").

---

## What this skill does

1. Pulls all active users under manager `00530000009MYBnAAO` (John DeFoe) — up to 5 levels deep — from Salesforce org62.
2. Fetches `sfbase__Region__c` (AMER/EMEA/APAC/LACA/JP/WW) for each user to enable geographic breakdowns.
3. Exports Events and Tasks for both the current and prior-year Q1 periods using Bulk API 2.0.
4. Pulls OpportunityLineItem SKUs for all opportunity IDs found in those activities.
5. **Fetches Account Asset records** for all accounts linked to activities — classifies each account as D360/AF/Both/Neither based on their purchased/installed products. This ensures account-linked meetings (customer success, expansion work on existing customers) are attributed to D360/AF rather than landing in Neither.
6. Classifies each activity using: Opp → OLI SKUs; Account → Asset products; all other WhatId types → Neither.
7. Rolls up hours per person (Events = DurationInMinutes/60, Tasks = Duration__c already in hours).
8. Maps each person to their org region via DeFoe's direct reports (Aiysha Mubarik, Salman Mian, Teddy Griffin, Anil Dindigal).
9. Outputs a per-person CSV: `Region_Manager, Manager, Name, Title, Geo, [FY_current]_D360/AF/Both/Neither/Total, [FY_prior]_D360/AF/Both/Neither/Total, YoY_total`.
10. Queries SE_Activity CRM Analytics dataset for total utilization hours (all activities, not just opp-linked), broken down by D360/AF/Both/Other per person and region.
11. Produces an "Other" drill-down showing what the non-D360/AF hours consist of (internal meetings, non-D360/AF opps, account-only, no WhatId, etc.).
12. Sends to Slackbot DM:
    - **Summary**: org-wide + top-line per manager
    - **Region table**: AMER/EMEA/APAC/LATAM/WW totals across all managers
    - **Per-manager × region breakdown**: one table per manager (Aiysha, Salman, Teddy, Anil)
    - **SE_Activity per-person CSV**: chunked as thread replies (≤4300 chars each)
    - **Other breakdown**: summary table + CSV showing what "Other" is

---

## Key constants (update each quarter)

| Variable | Current value | Notes |
|---|---|---|
| `DEFOE_MANAGER_ID` | `00530000009MYBnAAO` | John DeFoe's Salesforce User ID |
| `TARGET_ORG` | `mpurkayastha@salesforce.com` | Salesforce CLI alias / username |
| `SLACK_CHANNEL` | `D0139UAK29J` | Your Slackbot DM channel ID |
| `SLACK_USER_ID` | `W0133V63QJX` | Your Slack user ID |
| FY27 Q1 dates | `2026-02-01` → `2026-04-30` | Update to current quarter |
| FY26 Q1 dates | `2025-02-01` → `2025-04-30` | Update to prior-year quarter |
| SE_Activity dataset ID | `0Fb30000000TNFvCAO` | CRM Analytics dataset ID |
| SE_Activity version ID | `0Fced000009QlCbCAK` | Update if dataset is republished |
| SE_Activity fiscal year | `"2027"` (string) for FY27Q1 | SE_Activity uses string year, integer quarter |
| SE_Activity fiscal quarter | `"1"` (string) | |
| DeFoe manager level | `Owner.ManagerL05.Name` | DeFoe is at L05 in SE_Activity hierarchy |

---

## Step 1 — Verify Salesforce CLI auth

```bash
sf org display --target-org mpurkayastha@salesforce.com
```

If not authenticated, run:
```bash
sf org login web --alias mpurkayastha@salesforce.com
```

---

## Step 2 — Pull DeFoe org users (up to 5 manager levels)

```bash
sf data query \
  --target-org mpurkayastha@salesforce.com \
  --query "SELECT Id, Name, Title, ManagerId FROM User WHERE IsActive = true AND (
    ManagerId = '00530000009MYBnAAO'
    OR Manager.ManagerId = '00530000009MYBnAAO'
    OR Manager.Manager.ManagerId = '00530000009MYBnAAO'
    OR Manager.Manager.Manager.ManagerId = '00530000009MYBnAAO'
    OR Manager.Manager.Manager.Manager.ManagerId = '00530000009MYBnAAO'
  )" \
  --result-format csv > /tmp/defoe_users.csv
```

---

## Step 2b — Fetch user geo regions

Pull `sfbase__Region__c` and `sfbase__Subregion__c` in two batches of 200 (SOQL IN limit):

```python
import csv, subprocess, json

ids = [r['Id'] for r in csv.DictReader(open('/tmp/defoe_users.csv'))]
all_records = []
for batch in [ids[:200], ids[200:]]:
    if not batch:
        continue
    id_list = "','".join(batch)
    query = f"SELECT Id, Name, sfbase__Region__c, sfbase__Subregion__c FROM User WHERE Id IN ('{id_list}')"
    result = subprocess.run(
        ['sf', 'data', 'query', '--target-org', 'mpurkayastha@salesforce.com',
         '--query', query, '--result-format', 'json'],
        capture_output=True, text=True
    )
    all_records.extend(json.loads(result.stdout)['result']['records'])

with open('/tmp/defoe_users_geo.csv', 'w', newline='') as f:
    w = csv.DictWriter(f, fieldnames=['Id','Name','sfbase__Region__c','sfbase__Subregion__c'])
    w.writeheader()
    for r in all_records:
        w.writerow({k: r.get(k,'') for k in ['Id','Name','sfbase__Region__c','sfbase__Subregion__c']})
print(f"Wrote {len(all_records)} rows")
```

Region normalization applied during analysis: `LACA → LATAM`, `JP → APAC`, blank → `Unknown`.

---

## Step 3 — Export Events via Bulk API 2.0

Build the user ID list first:
```python
import csv
ids = [r['Id'] for r in csv.DictReader(open('/tmp/defoe_users.csv'))]
id_list = "','".join(ids)
```

Then export (repeat for prior-year dates, saving to `events_fy26q1_bulk.csv`):
```bash
sf data export bulk \
  --target-org mpurkayastha@salesforce.com \
  --wait 10 \
  --output-file /tmp/events_fy27q1_bulk.csv \
  --query "SELECT Id, OwnerId, WhatId, DurationInMinutes, ActivityDateTime, Subject
           FROM Event
           WHERE OwnerId IN ('<USER_IDS>')
           AND ActivityDateTime >= 2026-02-01T00:00:00Z
           AND ActivityDateTime < 2026-05-01T00:00:00Z
           AND WhatId != null"
```

> If the bulk job times out, use the async + REST pagination approach (Step 3b).

### Step 3b — Async fallback for Events/Tasks

```python
import subprocess, json, urllib.request, csv, io, time

def get_access_token(org='mpurkayastha@salesforce.com'):
    result = subprocess.run(
        ['sf', 'org', 'display', '--target-org', org, '--json'],
        capture_output=True, text=True
    )
    return json.loads(result.stdout)['result']['accessToken']

def get_instance_url(org='mpurkayastha@salesforce.com'):
    result = subprocess.run(
        ['sf', 'org', 'display', '--target-org', org, '--json'],
        capture_output=True, text=True
    )
    return json.loads(result.stdout)['result']['instanceUrl']

def download_bulk_job(job_id, token, instance_url, output_file):
    """Paginate Bulk API 2.0 results using sforce-locator."""
    locator = None
    all_rows = []
    header = None
    while True:
        url = f"{instance_url}/services/data/v59.0/jobs/query/{job_id}/results"
        if locator:
            url += f"?locator={locator}"
        req = urllib.request.Request(url, headers={
            'Authorization': f'Bearer {token}',
            'Accept': 'text/csv'
        })
        with urllib.request.urlopen(req) as resp:
            body = resp.read().decode('utf-8')
            next_locator = resp.headers.get('sforce-locator', 'null')
        reader = csv.reader(io.StringIO(body))
        rows = list(reader)
        if not header:
            header = rows[0]
            all_rows.extend(rows[1:])
        else:
            all_rows.extend(rows[1:])
        if next_locator == 'null' or not next_locator:
            break
        locator = next_locator
    with open(output_file, 'w', newline='') as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(all_rows)
    print(f"Written {len(all_rows)} rows to {output_file}")
```

---

## Step 3c — Fetch Account Asset data (classify account-linked meetings)

After exporting Events/Tasks, collect all Account IDs (WhatId starting with `001`) and query their `Asset` records to classify each account as D360/AF/Both/Neither based on purchased/installed products. This means meetings logged against an account — customer success, expansion calls, QBRs on existing customers — are attributed to the right product line rather than falling into Neither.

```python
import csv, subprocess, json, re

D360_PAT = re.compile(r'data.?cloud|d360|data 360|data360|datacloud|flex credit|einstein analytics|data stream|identity resolution|cdp|customer data platform', re.I)
AF_PAT   = re.compile(r'agentforce|sela|einstein 1|agent|a4x|a1e|aela|autonomous', re.I)
ACTIVE_STATUSES = {'Installed', 'Active', 'Purchased', 'In Use'}

MERGE = {
    frozenset(['D360','AF']):'Both', frozenset(['Both','D360']):'Both',
    frozenset(['Both','AF']):'Both', frozenset(['Both','Neither']):'Both',
    frozenset(['D360','Neither']):'D360', frozenset(['AF','Neither']):'AF',
    frozenset(['Neither','Neither']):'Neither', frozenset(['D360','D360']):'D360',
    frozenset(['AF','AF']):'AF', frozenset(['Both','Both']):'Both',
}

# Collect account IDs from all four activity files
account_ids = set()
for fname in ['/tmp/events_fy27q1_bulk.csv', '/tmp/events_fy26q1_bulk.csv',
              '/tmp/tasks_fy27q1_bulk.csv', '/tmp/tasks_fy26q1_bulk.csv']:
    for row in csv.DictReader(open(fname)):
        wid = row.get('WhatId', '')
        if wid.startswith('001'):
            account_ids.add(wid)
print(f"Unique account IDs: {len(account_ids)}")

# Query Asset records in batches of 200
all_assets = []
for batch in [list(account_ids)[i:i+200] for i in range(0, len(account_ids), 200)]:
    id_str = "','".join(batch)
    result = subprocess.run(
        ['sf', 'data', 'query', '--target-org', 'mpurkayastha@salesforce.com',
         '--query', f"SELECT AccountId, Product2.Name, Product2.Family, Product2.ProductCode, Status FROM Asset WHERE AccountId IN ('{id_str}')",
         '--result-format', 'json'],
        capture_output=True, text=True
    )
    records = json.loads(result.stdout).get('result', {}).get('records', [])
    for r in records:
        all_assets.append({
            'AccountId':            r.get('AccountId', ''),
            'Product2.Name':        r.get('Product2', {}).get('Name', '') if r.get('Product2') else '',
            'Product2.Family':      r.get('Product2', {}).get('Family', '') if r.get('Product2') else '',
            'Product2.ProductCode': r.get('Product2', {}).get('ProductCode', '') if r.get('Product2') else '',
            'Status':               r.get('Status', ''),
        })

# Classify each account
account_category = {}
for row in all_assets:
    if row.get('Status') and row['Status'] not in ACTIVE_STATUSES:
        continue
    aid = row['AccountId']
    if not aid: continue
    text = f"{row['Product2.Name']} {row['Product2.Family']} {row['Product2.ProductCode']}"
    is_d360, is_af = bool(D360_PAT.search(text)), bool(AF_PAT.search(text))
    cat = 'Both' if (is_d360 and is_af) else ('D360' if is_d360 else ('AF' if is_af else 'Neither'))
    prev = account_category.get(aid, 'Neither')
    account_category[aid] = MERGE.get(frozenset([prev, cat]), 'Both')

with open('/tmp/account_category.csv', 'w', newline='') as f:
    w = csv.DictWriter(f, fieldnames=['AccountId','Category'])
    w.writeheader()
    for aid, cat in account_category.items():
        w.writerow({'AccountId': aid, 'Category': cat})
print(f"Wrote {len(account_category)} account categories → /tmp/account_category.csv")
```

---

## Step 4 — Export Tasks via Bulk API 2.0

The correct hours field on Task is **`Duration__c`** (labeled "Time(Hrs)", type double). Do NOT use `Hours__c` or `Hour__c`.

```bash
sf data export bulk \
  --target-org mpurkayastha@salesforce.com \
  --wait 10 \
  --output-file /tmp/tasks_fy27q1_bulk.csv \
  --query "SELECT Id, OwnerId, WhatId, ActivityDate, Subject, Duration__c
           FROM Task
           WHERE OwnerId IN ('<USER_IDS>')
           AND ActivityDate >= 2026-02-01
           AND ActivityDate <= 2026-04-30
           AND WhatId != null"
```

---

## Step 5 — Pull OpportunityLineItem SKUs

Extract all opportunity IDs (WhatId starting with `006`) from both event files, then query OLIs:

```python
import csv, re

opp_ids = set()
for fname in ['/tmp/events_fy27q1_bulk.csv', '/tmp/events_fy26q1_bulk.csv',
              '/tmp/tasks_fy27q1_bulk.csv', '/tmp/tasks_fy26q1_bulk.csv']:
    for row in csv.DictReader(open(fname)):
        wid = row.get('WhatId', '')
        if wid.startswith('006'):
            opp_ids.add(wid)

print(f"Unique opp IDs: {len(opp_ids)}")
```

Query in batches of 200:
```bash
sf data export bulk \
  --target-org mpurkayastha@salesforce.com \
  --wait 10 \
  --output-file /tmp/oli_skus.csv \
  --query "SELECT OpportunityId, Product2.Name, Product2.Family, Product2.ProductCode
           FROM OpportunityLineItem
           WHERE OpportunityId IN ('<OPP_IDS>')"
```

---

## Step 6 — Analysis Python script (opp-linked hours per person)

```python
import csv, re, collections

# SKU classification patterns
D360_PAT = re.compile(
    r'data.?cloud|d360|data 360|data360|datacloud|flex credit|einstein analytics'
    r'|data stream|identity resolution|cdp|customer data platform', re.I)
AF_PAT = re.compile(
    r'agentforce|sela|einstein 1|agent|a4x|a1e|aela|autonomous', re.I)

def classify_sku(name, family, code):
    text = f"{name} {family} {code}"
    is_d360 = bool(D360_PAT.search(text))
    is_af   = bool(AF_PAT.search(text))
    if is_d360 and is_af: return 'Both'
    if is_d360: return 'D360'
    if is_af:   return 'AF'
    return 'Neither'

# Build opp → category map (from OLI SKUs)
opp_category = {}
for row in csv.DictReader(open('/tmp/oli_skus.csv')):
    oid = row['OpportunityId']
    cat = classify_sku(row.get('Product2.Name',''), row.get('Product2.Family',''), row.get('Product2.ProductCode',''))
    prev = opp_category.get(oid, 'Neither')
    merge = {frozenset(['D360','AF']):'Both', frozenset(['Both','D360']):'Both',
             frozenset(['Both','AF']):'Both', frozenset(['Both','Neither']):'Both',
             frozenset(['D360','Neither']):'D360', frozenset(['AF','Neither']):'AF',
             frozenset(['Neither','Neither']):'Neither', frozenset(['D360','D360']):'D360',
             frozenset(['AF','AF']):'AF', frozenset(['Both','Both']):'Both'}
    opp_category[oid] = merge.get(frozenset([prev, cat]), 'Both')

# Build account → category map (from Asset records, Step 3c)
account_category = {r['AccountId']: r['Category']
                    for r in csv.DictReader(open('/tmp/account_category.csv'))}

# Build user map
users = {r['Id']: r for r in csv.DictReader(open('/tmp/defoe_users.csv'))}

DEFOE_ID = '00530000009MYBnAAO'

def get_region(user_id):
    """Walk up manager chain to find DeFoe's direct report."""
    visited = set()
    uid = user_id
    chain = []
    while uid and uid not in visited:
        visited.add(uid)
        u = users.get(uid)
        if not u:
            break
        chain.append(u['Name'])
        if u.get('ManagerId') == DEFOE_ID:
            return u['Name']
        uid = u.get('ManagerId')
    for name in chain:
        if name in {'Aiysha Mubarik', 'Salman Mian', 'Teddy Griffin', 'Anil Dindigal'}:
            return name
    return 'Unknown'

Hours = collections.defaultdict(lambda: collections.defaultdict(float))

def classify_whatid(wid):
    """Opp → OLI SKUs; Account → Asset products; everything else → Neither."""
    if wid.startswith('006'):
        return opp_category.get(wid, 'Neither')
    if wid.startswith('001'):
        return account_category.get(wid, 'Neither')
    return 'Neither'

def process_events(fname, period):
    for row in csv.DictReader(open(fname)):
        uid = row['OwnerId']
        wid = row.get('WhatId', '')
        hrs = float(row.get('DurationInMinutes') or 0) / 60.0
        Hours[uid][f'{period}_{classify_whatid(wid)}'] += hrs

def process_tasks(fname, period):
    for row in csv.DictReader(open(fname)):
        uid = row['OwnerId']
        wid = row.get('WhatId', '')
        hrs = float(row.get('Duration__c') or 0)
        Hours[uid][f'{period}_{classify_whatid(wid)}'] += hrs

process_events('/tmp/events_fy27q1_bulk.csv', 'FY27Q1')
process_events('/tmp/events_fy26q1_bulk.csv', 'FY26Q1')
process_tasks('/tmp/tasks_fy27q1_bulk.csv', 'FY27Q1')
process_tasks('/tmp/tasks_fy26q1_bulk.csv', 'FY26Q1')

PERIODS = ['FY27Q1', 'FY26Q1']  # update each quarter
out_rows = []
for uid, u in users.items():
    region = get_region(uid)
    h = Hours[uid]
    row = {'Region_Manager': region, 'Manager': region, 'Name': u['Name'], 'Title': u['Title']}
    for p in PERIODS:
        d = round(h.get(f'{p}_D360', 0), 1)
        a = round(h.get(f'{p}_AF', 0), 1)
        b = round(h.get(f'{p}_Both', 0), 1)
        n = round(h.get(f'{p}_Neither', 0), 1)
        t = round(d + a + b + n, 1)
        row[f'{p}_D360_hrs'] = d
        row[f'{p}_AF_hrs'] = a
        row[f'{p}_Both_hrs'] = b
        row[f'{p}_Neither_hrs'] = n
        row[f'{p}_Total'] = int(t) if t == int(t) else t
    p1, p2 = PERIODS
    yoy = round(
        (row[f'{p1}_D360_hrs'] + row[f'{p1}_AF_hrs'] + row[f'{p1}_Both_hrs'] + row[f'{p1}_Neither_hrs']) -
        (row[f'{p2}_D360_hrs'] + row[f'{p2}_AF_hrs'] + row[f'{p2}_Both_hrs'] + row[f'{p2}_Neither_hrs']), 1)
    row['YoY_total'] = yoy
    out_rows.append(row)

out_rows.sort(key=lambda r: (r['Region_Manager'], r['Name']))
fields = ['Region_Manager','Manager','Name','Title',
          f'{PERIODS[0]}_D360_hrs',f'{PERIODS[0]}_AF_hrs',f'{PERIODS[0]}_Both_hrs',
          f'{PERIODS[0]}_Neither_hrs',f'{PERIODS[0]}_Total',
          f'{PERIODS[1]}_D360_hrs',f'{PERIODS[1]}_AF_hrs',f'{PERIODS[1]}_Both_hrs',
          f'{PERIODS[1]}_Neither_hrs',f'{PERIODS[1]}_Total','YoY_total']

with open('/tmp/q1_activity_lookback.csv', 'w', newline='') as f:
    w = csv.DictWriter(f, fieldnames=fields)
    w.writeheader()
    w.writerows(out_rows)
print(f"Wrote {len(out_rows)} rows to /tmp/q1_activity_lookback.csv")
```

---

## Step 7 — SE_Activity CRM Analytics breakdown (total utilization)

This queries the SE_Activity dataset for ALL activities (not just opp-linked), giving total utilization hours broken down by D360/AF/Both/Other per person. SE_Activity uses a reversed hierarchy where DeFoe is at `ManagerL05`.

> **Key SE_Activity facts:**
> - Dataset ID: `0Fb30000000TNFvCAO` / Version: `0Fced000009QlCbCAK`
> - Fiscal year/quarter are **strings** in SAQL filters (e.g. `"2027"`, `"1"`)
> - DeFoe's level: `Owner.ManagerL05.Name == "John DeFoe"`
> - `Full.OppId` links activities to opportunities (blank = `-BLANK-`)
> - `Customer_Related` values: `"Customer Related"`, `"Non Customer Related"`, `-BLANK-`
> - `WhatObjectId` prefix: `006`=Opp, `001`=Account, `500`=Case, `a25`=DSR, `a69`=StratInit, `701`=Campaign

```python
import subprocess, json, urllib.request, csv, collections, re

def sf_api(token, instance, saql):
    body = json.dumps({"query": saql}).encode()
    req = urllib.request.Request(
        f"{instance}/services/data/v59.0/wave/query",
        data=body,
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    )
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read()).get('results', {}).get('records', [])

result = subprocess.run(
    ['sf', 'org', 'display', '--target-org', 'mpurkayastha@salesforce.com', '--json'],
    capture_output=True, text=True
)
d = json.loads(result.stdout)['result']
token, instance = d['accessToken'], d['instanceUrl']

DATASET = "0Fb30000000TNFvCAO/0Fced000009QlCbCAK"

# --- Build opp_category dict (same as Step 6) ---
D360_PAT = re.compile(r'data.?cloud|d360|data 360|data360|datacloud|flex credit|einstein analytics|data stream|identity resolution|cdp|customer data platform', re.I)
AF_PAT = re.compile(r'agentforce|sela|einstein 1|agent|a4x|a1e|aela|autonomous', re.I)
opp_category = {}
for row in csv.DictReader(open('/tmp/oli_skus.csv')):
    oid = row['OpportunityId']
    t = f"{row.get('Product2.Name','')} {row.get('Product2.Family','')} {row.get('Product2.ProductCode','')}"
    is_d360, is_af = bool(D360_PAT.search(t)), bool(AF_PAT.search(t))
    cat = 'Both' if (is_d360 and is_af) else ('D360' if is_d360 else ('AF' if is_af else 'Neither'))
    prev = opp_category.get(oid, 'Neither')
    merge = {frozenset(['D360','AF']):'Both', frozenset(['Both','D360']):'Both', frozenset(['Both','AF']):'Both',
             frozenset(['Both','Neither']):'Both', frozenset(['D360','Neither']):'D360',
             frozenset(['AF','Neither']):'AF', frozenset(['Neither','Neither']):'Neither',
             frozenset(['D360','D360']):'D360', frozenset(['AF','AF']):'AF', frozenset(['Both','Both']):'Both'}
    opp_category[oid] = merge.get(frozenset([prev, cat]), 'Both')

# Load geo map
geo_map = {}
for r in csv.DictReader(open('/tmp/defoe_users_geo.csv')):
    raw = r['sfbase__Region__c'] or ''
    mapping = {'LACA': 'LATAM', 'JP': 'APAC', 'AMER': 'AMER', 'EMEA': 'EMEA', 'APAC': 'APAC', 'WW': 'WW', '': 'Unknown'}
    geo_map[r['Name']] = mapping.get(raw, raw)

# Load region manager map
users = {r['Id']: r for r in csv.DictReader(open('/tmp/defoe_users.csv'))}
DEFOE_ID = '00530000009MYBnAAO'
def get_region(user_id):
    visited, uid, chain = set(), user_id, []
    while uid and uid not in visited:
        visited.add(uid); u = users.get(uid)
        if not u: break
        chain.append(u['Name'])
        if u.get('ManagerId') == DEFOE_ID: return u['Name']
        uid = u.get('ManagerId')
    for name in chain:
        if name in {'Aiysha Mubarik','Salman Mian','Teddy Griffin','Anil Dindigal'}: return name
    return 'Unknown'
name_to_region = {u['Name']: get_region(uid) for uid, u in users.items()}

def se_activity_per_person(yr, qtr):
    """Returns dict: owner_name -> {D360, AF, Both, Other, Total} hours"""
    # Get opp-linked hours by owner x opp
    saql = f"""
q = load "{DATASET}";
q = filter q by 'Owner.ManagerL05.Name' == "John DeFoe";
q = filter q by 'ActivityDate_Year_Fiscal' == "{yr}" && 'ActivityDate_Quarter_Fiscal' == "{qtr}";
q = filter q by 'Customer_Related' == "Customer Related";
q = filter q by 'Full.OppId' != "-BLANK-";
q = group q by ('Owner.Name', 'Full.OppId');
q = foreach q generate 'Owner.Name' as owner, 'Full.OppId' as opp_id, sum('DurationInMinutes') as mins;
q = limit q 5000;
"""
    opp_records = sf_api(token, instance, saql)

    # Get all hours by owner (for total)
    saql2 = f"""
q = load "{DATASET}";
q = filter q by 'Owner.ManagerL05.Name' == "John DeFoe";
q = filter q by 'ActivityDate_Year_Fiscal' == "{yr}" && 'ActivityDate_Quarter_Fiscal' == "{qtr}";
q = group q by 'Owner.Name';
q = foreach q generate 'Owner.Name' as owner, sum('DurationInMinutes') as mins;
q = limit q 500;
"""
    total_records = sf_api(token, instance, saql2)

    per_person = collections.defaultdict(lambda: {'D360':0,'AF':0,'Both':0,'Other':0,'Total':0})
    for r in total_records:
        per_person[r['owner']]['Total'] = r['mins']/60

    for r in opp_records:
        hrs = r['mins']/60
        cat = opp_category.get(r['opp_id'], 'Neither')
        owner = r['owner']
        if cat in ('D360','AF','Both'):
            per_person[owner][cat] += hrs
        # Neither opp-linked hours count as Other (handled below)

    # Other = Total - D360 - AF - Both
    for owner, h in per_person.items():
        h['Other'] = max(0, h['Total'] - h['D360'] - h['AF'] - h['Both'])

    return per_person

p1_data = se_activity_per_person("2027", "1")  # FY27Q1 — update each quarter
p2_data = se_activity_per_person("2026", "1")  # FY26Q1 — update each quarter

# Write per-person CSV
out_rows = []
all_names = set(p1_data.keys()) | set(p2_data.keys())
for name in sorted(all_names):
    uid = next((uid for uid, u in users.items() if u['Name'] == name), None)
    title = users[uid]['Title'] if uid else ''
    region = name_to_region.get(name, 'Unknown')
    geo = geo_map.get(name, 'Unknown')
    h1, h2 = p1_data[name], p2_data[name]
    out_rows.append({
        'Region_Manager': region, 'Geo': geo, 'Name': name, 'Title': title,
        'FY27Q1_D360_hrs': round(h1['D360'],1), 'FY27Q1_AF_hrs': round(h1['AF'],1),
        'FY27Q1_Both_hrs': round(h1['Both'],1), 'FY27Q1_Other_hrs': round(h1['Other'],1),
        'FY27Q1_Total_hrs': round(h1['Total'],1),
        'FY26Q1_D360_hrs': round(h2['D360'],1), 'FY26Q1_AF_hrs': round(h2['AF'],1),
        'FY26Q1_Both_hrs': round(h2['Both'],1), 'FY26Q1_Other_hrs': round(h2['Other'],1),
        'FY26Q1_Total_hrs': round(h2['Total'],1),
        'YoY_total_hrs': round(h1['Total'] - h2['Total'], 1),
    })

out_rows.sort(key=lambda r: (r['Region_Manager'], r['Geo'], r['Name']))
fields = ['Region_Manager','Geo','Name','Title',
          'FY27Q1_D360_hrs','FY27Q1_AF_hrs','FY27Q1_Both_hrs','FY27Q1_Other_hrs','FY27Q1_Total_hrs',
          'FY26Q1_D360_hrs','FY26Q1_AF_hrs','FY26Q1_Both_hrs','FY26Q1_Other_hrs','FY26Q1_Total_hrs',
          'YoY_total_hrs']
with open('/tmp/se_activity_breakdown.csv', 'w', newline='') as f:
    w = csv.DictWriter(f, fieldnames=fields)
    w.writeheader()
    w.writerows(out_rows)
print(f"Wrote {len(out_rows)} rows to /tmp/se_activity_breakdown.csv")
```

---

## Step 8 — SE_Activity "Other" drill-down

Breaks down what "Other" consists of: internal meetings, customer meetings on non-D360/AF opps, meetings logged to accounts/no WhatId, etc.

```python
import subprocess, json, urllib.request, csv

result = subprocess.run(
    ['sf', 'org', 'display', '--target-org', 'mpurkayastha@salesforce.com', '--json'],
    capture_output=True, text=True
)
d = json.loads(result.stdout)['result']
token, instance = d['accessToken'], d['instanceUrl']
DATASET = "0Fb30000000TNFvCAO/0Fced000009QlCbCAK"

def sf_api(token, instance, saql):
    body = json.dumps({"query": saql}).encode()
    req = urllib.request.Request(
        f"{instance}/services/data/v59.0/wave/query", data=body,
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    )
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read()).get('results', {}).get('records', [])

def other_breakdown(yr, qtr, period_label):
    # Customer-related opp-linked, by opp (to classify D360/AF/non)
    opp_saql = f"""
q = load "{DATASET}";
q = filter q by 'Owner.ManagerL05.Name' == "John DeFoe";
q = filter q by 'ActivityDate_Year_Fiscal' == "{yr}" && 'ActivityDate_Quarter_Fiscal' == "{qtr}";
q = filter q by 'Customer_Related' == "Customer Related";
q = filter q by 'Full.OppId' != "-BLANK-";
q = group q by 'Full.OppId';
q = foreach q generate 'Full.OppId' as opp_id, sum('DurationInMinutes') as mins;
q = limit q 2000;
"""
    d360=af=both=non_d360af = 0
    for r in sf_api(token, instance, opp_saql):
        hrs = r['mins']/60
        cat = opp_category.get(r['opp_id'], 'Neither')
        if cat == 'D360': d360 += hrs
        elif cat == 'AF': af += hrs
        elif cat == 'Both': both += hrs
        else: non_d360af += hrs

    # Customer-related non-opp, by WhatObjectId type
    nonopp_saql = f"""
q = load "{DATASET}";
q = filter q by 'Owner.ManagerL05.Name' == "John DeFoe";
q = filter q by 'ActivityDate_Year_Fiscal' == "{yr}" && 'ActivityDate_Quarter_Fiscal' == "{qtr}";
q = filter q by 'Customer_Related' == "Customer Related";
q = filter q by 'Full.OppId' == "-BLANK-";
q = group q by 'WhatObjectId';
q = foreach q generate 'WhatObjectId' as wobj, sum('DurationInMinutes') as mins;
q = limit q 50;
"""
    nonopp = {str(r['wobj'])[:3]: r['mins']/60 for r in sf_api(token, instance, nonopp_saql)}
    account = nonopp.get('001', 0)
    no_whatid = nonopp.get('-BL', 0)
    other_nonopp = sum(v for k, v in nonopp.items() if k not in ('001','-BL'))

    # Non-customer-related
    ncr_saql = f"""
q = load "{DATASET}";
q = filter q by 'Owner.ManagerL05.Name' == "John DeFoe";
q = filter q by 'ActivityDate_Year_Fiscal' == "{yr}" && 'ActivityDate_Quarter_Fiscal' == "{qtr}";
q = filter q by 'Customer_Related' == "Non Customer Related";
q = group q by 'Customer_Related';
q = foreach q generate 'Customer_Related' as x, sum('DurationInMinutes') as mins;
q = limit q 5;
"""
    ncr = sum(r['mins']/60 for r in sf_api(token, instance, ncr_saql))

    # Blank
    blk_saql = f"""
q = load "{DATASET}";
q = filter q by 'Owner.ManagerL05.Name' == "John DeFoe";
q = filter q by 'ActivityDate_Year_Fiscal' == "{yr}" && 'ActivityDate_Quarter_Fiscal' == "{qtr}";
q = filter q by 'Customer_Related' == "-BLANK-";
q = group q by 'Customer_Related';
q = foreach q generate 'Customer_Related' as x, sum('DurationInMinutes') as mins;
q = limit q 5;
"""
    blk = sum(r['mins']/60 for r in sf_api(token, instance, blk_saql))

    total = d360 + af + both + non_d360af + account + no_whatid + other_nonopp + ncr + blk
    classified = d360 + af + both
    other_total = total - classified

    return [
        {"Period": period_label, "Category": "Classified", "Subcategory": "Customer meetings — D360 opp", "Hours": round(d360), "Pct_of_Total": f"{100*d360/total:.1f}%"},
        {"Period": period_label, "Category": "Classified", "Subcategory": "Customer meetings — AF opp", "Hours": round(af), "Pct_of_Total": f"{100*af/total:.1f}%"},
        {"Period": period_label, "Category": "Classified", "Subcategory": "Customer meetings — Both D360+AF opp", "Hours": round(both), "Pct_of_Total": f"{100*both/total:.1f}%"},
        {"Period": period_label, "Category": "Classified TOTAL", "Subcategory": "", "Hours": round(classified), "Pct_of_Total": f"{100*classified/total:.1f}%"},
        {"Period": period_label, "Category": "Other", "Subcategory": "Customer meetings — opp not tagged D360/AF", "Hours": round(non_d360af), "Pct_of_Total": f"{100*non_d360af/total:.1f}%"},
        {"Period": period_label, "Category": "Other", "Subcategory": "Customer meetings — linked to Account only (no Opp)", "Hours": round(account), "Pct_of_Total": f"{100*account/total:.1f}%"},
        {"Period": period_label, "Category": "Other", "Subcategory": "Customer meetings — no WhatId logged", "Hours": round(no_whatid), "Pct_of_Total": f"{100*no_whatid/total:.1f}%"},
        {"Period": period_label, "Category": "Other", "Subcategory": "Customer meetings — Case/DSR/StratInit/Campaign", "Hours": round(other_nonopp), "Pct_of_Total": f"{100*other_nonopp/total:.1f}%"},
        {"Period": period_label, "Category": "Other", "Subcategory": "Internal / non-customer meetings", "Hours": round(ncr), "Pct_of_Total": f"{100*ncr/total:.1f}%"},
        {"Period": period_label, "Category": "Other", "Subcategory": "Blank / unclassified", "Hours": round(blk), "Pct_of_Total": f"{100*blk/total:.1f}%"},
        {"Period": period_label, "Category": "Other TOTAL", "Subcategory": "", "Hours": round(other_total), "Pct_of_Total": f"{100*other_total/total:.1f}%"},
        {"Period": period_label, "Category": "GRAND TOTAL", "Subcategory": "", "Hours": round(total), "Pct_of_Total": "100.0%"},
    ]

rows = other_breakdown("2027", "1", "FY27Q1") + other_breakdown("2026", "1", "FY26Q1")
fields = ["Period", "Category", "Subcategory", "Hours", "Pct_of_Total"]
with open('/tmp/se_activity_other_breakdown.csv', 'w', newline='') as f:
    w = csv.DictWriter(f, fieldnames=fields)
    w.writeheader()
    w.writerows(rows)
print(f"Wrote {len(rows)} rows to /tmp/se_activity_other_breakdown.csv")
```

---

## Step 9 — Generate summaries + send to Slack

### 9a — Opp-linked hours: region × manager summary (send as new DM)

```python
import csv, collections

geo_map = {}
for r in csv.DictReader(open('/tmp/defoe_users_geo.csv')):
    raw = r['sfbase__Region__c'] or ''
    mapping = {'LACA': 'LATAM', 'JP': 'APAC', 'AMER': 'AMER', 'EMEA': 'EMEA', 'APAC': 'APAC', 'WW': 'WW', '': 'Unknown'}
    geo_map[r['Id']] = mapping.get(raw, raw)

rows = list(csv.DictReader(open('/tmp/q1_activity_lookback.csv')))
name_to_id = {r['Name']: r['Id'] for r in csv.DictReader(open('/tmp/defoe_users.csv'))}
for r in rows:
    r['Geo'] = geo_map.get(name_to_id.get(r['Name'], ''), 'Unknown')

P1, P2 = 'FY27Q1', 'FY26Q1'

def pct(a, total): return f"{100*a/total:.1f}%" if total else "0%"
def sum_rows(rlist, period):
    d = sum(float(r[f'{period}_D360_hrs']) for r in rlist)
    a = sum(float(r[f'{period}_AF_hrs']) for r in rlist)
    b = sum(float(r[f'{period}_Both_hrs']) for r in rlist)
    n = sum(float(r[f'{period}_Neither_hrs']) for r in rlist)
    return d, a, b, n, d+a+b+n

geo_order = ['AMER', 'EMEA', 'APAC', 'LATAM', 'WW']
lines = [f"*AFD360 Org — {P1} vs {P2} | Activity by Region & Manager*", "",
         "*TOTAL BY REGION (all managers combined)*", "```",
         f"{'Region':<8} {'#':>4}  {'D360%':>7} {'AF%':>6} {'Both%':>6} {'Neith%':>7} {'FY_cur':>7}  {'D360%':>7} {'AF%':>6} {'FY_pr':>7}  {'YoY':>6}",
         "-" * 80]
for geo_val in geo_order:
    geo_rows = [r for r in rows if r.get('Geo') == geo_val]
    if not geo_rows: continue
    d1,a1,b1,n1,t1 = sum_rows(geo_rows, P1)
    d2,a2,b2,n2,t2 = sum_rows(geo_rows, P2)
    lines.append(f"{geo_val:<8} {len(geo_rows):>4}  {pct(d1+b1,t1):>7} {pct(a1+b1,t1):>6} {pct(b1,t1):>6} {pct(n1,t1):>7} {t1:>7.0f}  {pct(d2+b2,t2):>7} {pct(a2+b2,t2):>6} {t2:>7.0f}  {t1-t2:>+6.0f}")
lines.append("```")
# Send via Slack MCP: mcp__slack__slack_send_message channel=D0139UAK29J
# Save message_ts for threading
```

### 9b — Per-manager × region (send as thread replies)

```python
managers = ['Aiysha Mubarik', 'Salman Mian', 'Teddy Griffin', 'Anil Dindigal']
for mgr in managers:
    mgr_rows = [r for r in rows if r['Region_Manager'] == mgr]
    d1,a1,b1,n1,t1 = sum_rows(mgr_rows, P1)
    d2,a2,b2,n2,t2 = sum_rows(mgr_rows, P2)
    lines = [f"*{mgr}'s Team — by Region*",
             f"_Overall: {P1} D360={pct(d1+b1,t1)} AF={pct(a1+b1,t1)} ({t1:.0f}h) | {P2} D360={pct(d2+b2,t2)} AF={pct(a2+b2,t2)} ({t2:.0f}h) | YoY: {t1-t2:+.0f}h_",
             "```"]
    for geo_val in geo_order:
        geo_rows = [r for r in mgr_rows if r.get('Geo') == geo_val]
        if not geo_rows:
            lines.append(f"{geo_val:<8}  —")
            continue
        dg1,ag1,bg1,ng1,tg1 = sum_rows(geo_rows, P1)
        dg2,ag2,bg2,ng2,tg2 = sum_rows(geo_rows, P2)
        lines.append(f"{geo_val:<8} {len(geo_rows):>4}  {pct(dg1+bg1,tg1):>7} {pct(ag1+bg1,tg1):>6} {pct(bg1,tg1):>6} {pct(ng1,tg1):>7} {tg1:>7.0f}  {pct(dg2+bg2,tg2):>7} {pct(ag2+bg2,tg2):>6} {tg2:>7.0f}  {tg1-tg2:>+6.0f}")
    lines.append("```")
    # Send as thread reply: thread_ts=<message_ts from 9a>
```

### 9c — SE_Activity per-person CSV (chunked, thread replies)

```python
import csv

rows = list(csv.DictReader(open('/tmp/se_activity_breakdown.csv')))
fields = list(rows[0].keys())
header = ','.join(fields)

chunks = []
current = [header]
current_len = len(header) + 1
for r in rows:
    line = ','.join(str(r[f]) for f in fields)
    if current_len + len(line) + 1 > 4300 and len(current) > 1:
        chunks.append('\n'.join(current))
        current = [header, line]
        current_len = len(header) + len(line) + 2
    else:
        current.append(line)
        current_len += len(line) + 1
if len(current) > 1:
    chunks.append('\n'.join(current))

# Send each chunk as thread reply with label "SE_Activity breakdown (N/total)"
# First chunk can be a new message (save ts), rest as thread replies
```

### 9d — Other breakdown (new DM + CSV as thread reply)

```python
# Build summary table from /tmp/se_activity_other_breakdown.csv
# Send as new Slack DM message
# Thread the CSV data underneath

lines = ["*AFD360 Org — SE_Activity 'Other' Breakdown | FY27Q1 vs FY26Q1*", "",
         "_'Other' = everything not classified as D360, AF, or Both_", "```",
         f"{'Category':<52} {'FY27Q1':>7}  {'%':>6}  {'FY26Q1':>7}  {'%':>6}",
         "-"*80]
# Add rows from /tmp/se_activity_other_breakdown.csv grouped by Category
# Send via mcp__slack__slack_send_message
# Thread CSV raw data as reply
```

---

## Notes

- **"Unknown" region** entries are Chatter/parenthetical ghost users — safe to ignore.
- **Zero-hour people** haven't logged activities against opps with WhatId populated, or were newly added mid-quarter.
- **Bulk API timeout**: If `--wait 10` isn't enough, use the async + REST pagination approach (Step 3b).
- **Task hours field**: Always use `Duration__c` — verified via `sf sobject describe --sobject Task`.
- **The `Both` category** means the opp has SKUs matching both D360 and AF patterns.
- **SE_Activity vs opp-linked hours**: SE_Activity captures ALL activities (41,723h FY27Q1); opp-linked analysis captures only WhatId=Opp rows (~11,738h). Use SE_Activity for total utilization, opp-linked for deal coverage depth.
- **Account-linked activity classification**: Activities with WhatId=Account (001...) are classified using the account's `Asset` records — only `Installed`, `Active`, `Purchased`, or `In Use` statuses count. This captures customer success, expansion calls, and QBRs on existing D360/AF customers that don't have an open opp.
- **"Other" in SE_Activity** breaks down as: ~36% internal meetings, ~27% customer meetings on non-D360/AF opps, ~25% customer meetings not linked to any opp (Account-only or no WhatId), ~10% blank/unclassified. After adding account asset classification, the Account-only bucket should shrink as more of that time is correctly attributed to D360/AF.
- **FY26Q1 Other sub-buckets**: WhatObjectId is not populated for older SE_Activity records, so Account/no-WhatId/Case/etc. show as zero for FY26Q1 — all non-opp customer time rolls into "opp not tagged D360/AF" for prior year.
- **Anil LATAM anomaly (FY27Q1)**: D360=0.4%/AF=0% despite +147% volume growth vs FY26Q1 — worth investigating whether deal coverage is being logged to correct opps.
- **Slack chunking**: SE_Activity CSV (179 rows) splits into ~5 chunks of ≤4300 chars each. Send as thread replies to avoid channel noise.
