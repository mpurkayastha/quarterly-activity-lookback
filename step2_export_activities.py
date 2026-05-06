"""
Step 2: Export Events and Tasks for both fiscal periods using Bulk API 2.0,
then pull OpportunityLineItem SKUs for all opp IDs found.

Outputs:
  - config.EVENTS_CURRENT_CSV, config.EVENTS_PRIOR_CSV
  - config.TASKS_CURRENT_CSV,  config.TASKS_PRIOR_CSV
  - config.OLI_CSV
"""

import subprocess, json, csv, urllib.request, io, time
from config import (TARGET_ORG, USERS_CSV, OLI_CSV,
                    EVENTS_CURRENT_CSV, EVENTS_PRIOR_CSV,
                    TASKS_CURRENT_CSV, TASKS_PRIOR_CSV,
                    CURRENT_START, CURRENT_END, PRIOR_START, PRIOR_END,
                    CURRENT_PERIOD, PRIOR_PERIOD)


def _sf_org():
    r = subprocess.run(['sf', 'org', 'display', '--target-org', TARGET_ORG, '--json'],
                       capture_output=True, text=True)
    d = json.loads(r.stdout)['result']
    return d['accessToken'], d['instanceUrl']


def _bulk_export(query, output_file, wait=10):
    result = subprocess.run(
        ['sf', 'data', 'export', 'bulk', '--target-org', TARGET_ORG,
         '--wait', str(wait), '--output-file', output_file, '--query', query],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        print(f"  Bulk export stderr: {result.stderr[:300]}")
        raise RuntimeError(f"Bulk export failed for {output_file}")
    rows = sum(1 for _ in open(output_file)) - 1
    print(f"  → {output_file} ({rows} rows)")


def _paginated_download(job_id, token, instance_url, output_file):
    """Fallback: download Bulk API 2.0 job results with sforce-locator pagination."""
    locator = None
    all_rows, header = [], None
    while True:
        url = f"{instance_url}/services/data/v59.0/jobs/query/{job_id}/results"
        if locator:
            url += f"?locator={locator}"
        req = urllib.request.Request(url, headers={
            'Authorization': f'Bearer {token}', 'Accept': 'text/csv'})
        with urllib.request.urlopen(req) as resp:
            body = resp.read().decode('utf-8')
            next_locator = resp.headers.get('sforce-locator', 'null')
        rows = list(csv.reader(io.StringIO(body)))
        if not header:
            header = rows[0]
            all_rows.extend(rows[1:])
        else:
            all_rows.extend(rows[1:])
        if next_locator in ('null', '', None):
            break
        locator = next_locator
    with open(output_file, 'w', newline='') as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(all_rows)
    print(f"  → {output_file} ({len(all_rows)} rows, paginated)")


def export_events(id_list, start, end, output_file, period):
    print(f"Exporting Events {period} ({start} – {end})...")
    query = (f"SELECT Id, OwnerId, WhatId, DurationInMinutes, ActivityDateTime, Subject "
             f"FROM Event "
             f"WHERE OwnerId IN ('{id_list}') "
             f"AND ActivityDateTime >= {start}T00:00:00Z "
             f"AND ActivityDateTime < {end}T00:00:00Z "
             f"AND WhatId != null")
    _bulk_export(query, output_file)


def export_tasks(id_list, start, end, output_file, period):
    print(f"Exporting Tasks {period} ({start} – {end})...")
    # Duration__c = "Time(Hrs)" field — do NOT use Hours__c or Hour__c
    query = (f"SELECT Id, OwnerId, WhatId, ActivityDate, Subject, Duration__c "
             f"FROM Task "
             f"WHERE OwnerId IN ('{id_list}') "
             f"AND ActivityDate >= {start} "
             f"AND ActivityDate <= {end} "
             f"AND WhatId != null")
    _bulk_export(query, output_file)


def export_oli_skus(opp_ids):
    print(f"Exporting OLI SKUs for {len(opp_ids)} opportunities...")
    batches = [list(opp_ids)[i:i+200] for i in range(0, len(opp_ids), 200)]
    all_rows = []
    for i, batch in enumerate(batches):
        id_list = "','".join(batch)
        query = (f"SELECT OpportunityId, Product2.Name, Product2.Family, Product2.ProductCode "
                 f"FROM OpportunityLineItem WHERE OpportunityId IN ('{id_list}')")
        tmp = f"/tmp/oli_batch_{i}.csv"
        _bulk_export(query, tmp)
        reader = csv.DictReader(open(tmp))
        if i == 0:
            fieldnames = reader.fieldnames
        all_rows.extend(reader)

    with open(OLI_CSV, 'w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(all_rows)
    print(f"  → {OLI_CSV} ({len(all_rows)} total OLI rows)")


if __name__ == "__main__":
    ids = [r['Id'] for r in csv.DictReader(open(USERS_CSV))]
    id_list = "','".join(ids)

    export_events(id_list, CURRENT_START, CURRENT_END, EVENTS_CURRENT_CSV, CURRENT_PERIOD)
    export_events(id_list, PRIOR_START,   PRIOR_END,   EVENTS_PRIOR_CSV,   PRIOR_PERIOD)
    export_tasks(id_list,  CURRENT_START, CURRENT_END, TASKS_CURRENT_CSV,  CURRENT_PERIOD)
    export_tasks(id_list,  PRIOR_START,   PRIOR_END,   TASKS_PRIOR_CSV,    PRIOR_PERIOD)

    opp_ids = set()
    for fname in [EVENTS_CURRENT_CSV, EVENTS_PRIOR_CSV, TASKS_CURRENT_CSV, TASKS_PRIOR_CSV]:
        for row in csv.DictReader(open(fname)):
            wid = row.get('WhatId', '')
            if wid.startswith('006'):
                opp_ids.add(wid)
    print(f"Unique opp IDs across all activities: {len(opp_ids)}")

    if opp_ids:
        export_oli_skus(opp_ids)
    else:
        print("No opp IDs found — skipping OLI export.")
        open(OLI_CSV, 'w').write("OpportunityId,Product2.Name,Product2.Family,Product2.ProductCode\n")
