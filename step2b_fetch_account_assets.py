"""
Step 2b: For all accounts linked to activities (WhatId starting with 001),
query their Asset records to classify accounts as D360/AF/Both/Neither
based on purchased/installed products.

This enables account-linked meetings (customer success / expansion work)
to be classified as D360/AF rather than landing in Other/Neither.

Output: config.ACCOUNT_ASSETS_CSV  (AccountId, Product2.Name, Product2.Family, Product2.ProductCode)
        config.ACCOUNT_CATEGORY_CSV (AccountId, Category)
"""

import csv, subprocess, json
from config import (TARGET_ORG, EVENTS_CURRENT_CSV, EVENTS_PRIOR_CSV,
                    TASKS_CURRENT_CSV, TASKS_PRIOR_CSV,
                    ACCOUNT_ASSETS_CSV, ACCOUNT_CATEGORY_CSV)
import re

D360_PAT = re.compile(
    r'data.?cloud|d360|data 360|data360|datacloud|flex credit|einstein analytics'
    r'|data stream|identity resolution|cdp|customer data platform', re.I)
AF_PAT = re.compile(
    r'agentforce|sela|einstein 1|agent|a4x|a1e|aela|autonomous', re.I)

MERGE = {
    frozenset(['D360', 'AF']): 'Both',    frozenset(['Both', 'D360']): 'Both',
    frozenset(['Both', 'AF']): 'Both',    frozenset(['Both', 'Neither']): 'Both',
    frozenset(['D360', 'Neither']): 'D360', frozenset(['AF', 'Neither']): 'AF',
    frozenset(['Neither', 'Neither']): 'Neither', frozenset(['D360', 'D360']): 'D360',
    frozenset(['AF', 'AF']): 'AF',        frozenset(['Both', 'Both']): 'Both',
}

# Asset statuses that indicate the product is active/purchased
ACTIVE_STATUSES = {'Installed', 'Active', 'Purchased', 'In Use'}


def collect_account_ids():
    account_ids = set()
    for fname in [EVENTS_CURRENT_CSV, EVENTS_PRIOR_CSV, TASKS_CURRENT_CSV, TASKS_PRIOR_CSV]:
        try:
            for row in csv.DictReader(open(fname)):
                wid = row.get('WhatId', '')
                if wid.startswith('001'):
                    account_ids.add(wid)
        except FileNotFoundError:
            pass
    return account_ids


def fetch_assets(account_ids):
    print(f"Fetching assets for {len(account_ids)} accounts...")
    all_rows = []
    id_list = list(account_ids)
    batches = [id_list[i:i+200] for i in range(0, len(id_list), 200)]

    for i, batch in enumerate(batches):
        id_str = "','".join(batch)
        query = (f"SELECT AccountId, Product2.Name, Product2.Family, Product2.ProductCode, Status "
                 f"FROM Asset "
                 f"WHERE AccountId IN ('{id_str}')")
        result = subprocess.run(
            ['sf', 'data', 'query', '--target-org', TARGET_ORG,
             '--query', query, '--result-format', 'json'],
            capture_output=True, text=True
        )
        records = json.loads(result.stdout).get('result', {}).get('records', [])
        for r in records:
            all_rows.append({
                'AccountId':           r.get('AccountId', ''),
                'Product2.Name':       r.get('Product2', {}).get('Name', '') if r.get('Product2') else '',
                'Product2.Family':     r.get('Product2', {}).get('Family', '') if r.get('Product2') else '',
                'Product2.ProductCode':r.get('Product2', {}).get('ProductCode', '') if r.get('Product2') else '',
                'Status':              r.get('Status', ''),
            })
        print(f"  Batch {i+1}/{len(batches)}: {len(records)} assets")

    with open(ACCOUNT_ASSETS_CSV, 'w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=['AccountId','Product2.Name','Product2.Family','Product2.ProductCode','Status'])
        w.writeheader()
        w.writerows(all_rows)
    print(f"  → {ACCOUNT_ASSETS_CSV} ({len(all_rows)} rows)")
    return all_rows


def build_account_category(asset_rows):
    """Classify each account based on its installed/active product assets."""
    account_category = {}
    for row in asset_rows:
        # Only count active/installed assets
        if row.get('Status') and row['Status'] not in ACTIVE_STATUSES:
            continue
        aid = row['AccountId']
        if not aid:
            continue
        text = f"{row.get('Product2.Name','')} {row.get('Product2.Family','')} {row.get('Product2.ProductCode','')}"
        is_d360 = bool(D360_PAT.search(text))
        is_af   = bool(AF_PAT.search(text))
        cat = 'Both' if (is_d360 and is_af) else ('D360' if is_d360 else ('AF' if is_af else 'Neither'))
        prev = account_category.get(aid, 'Neither')
        account_category[aid] = MERGE.get(frozenset([prev, cat]), 'Both')

    with open(ACCOUNT_CATEGORY_CSV, 'w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=['AccountId', 'Category'])
        w.writeheader()
        for aid, cat in account_category.items():
            w.writerow({'AccountId': aid, 'Category': cat})

    counts = {}
    for cat in account_category.values():
        counts[cat] = counts.get(cat, 0) + 1
    print(f"  Account classification: {counts}")
    print(f"  → {ACCOUNT_CATEGORY_CSV} ({len(account_category)} accounts)")
    return account_category


if __name__ == "__main__":
    account_ids = collect_account_ids()
    print(f"Found {len(account_ids)} unique account IDs in activity exports")
    if account_ids:
        asset_rows = fetch_assets(account_ids)
        build_account_category(asset_rows)
    else:
        print("No account-linked activities found — writing empty files.")
        open(ACCOUNT_ASSETS_CSV, 'w').write("AccountId,Product2.Name,Product2.Family,Product2.ProductCode,Status\n")
        open(ACCOUNT_CATEGORY_CSV, 'w').write("AccountId,Category\n")
