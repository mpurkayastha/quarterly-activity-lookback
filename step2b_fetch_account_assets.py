"""
Step 2b: For all accounts linked to activities (WhatId starting with 001),
query their OrderItem records to classify accounts as D360/AF/Both/Neither
based on purchased products.

This enables account-linked meetings (customer success / expansion work)
to be classified as D360/AF rather than landing in Other/Neither.

Note: Uses OrderItem (via Order.AccountId) rather than the Asset sObject,
which may not be available in all orgs.

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


def fetch_order_items(account_ids):
    """Query OrderItem for all accounts to get purchased products."""
    print(f"Fetching OrderItem products for {len(account_ids)} accounts...")
    all_rows = []
    id_list = list(account_ids)
    batches = [id_list[i:i+100] for i in range(0, len(id_list), 100)]

    for i, batch in enumerate(batches):
        id_str = "','".join(batch)
        query = (f"SELECT Order.AccountId, Product2.Name, Product2.Family, Product2.ProductCode "
                 f"FROM OrderItem "
                 f"WHERE Order.AccountId IN ('{id_str}')")
        result = subprocess.run(
            ['sf', 'data', 'query', '--target-org', TARGET_ORG,
             '--query', query, '--result-format', 'json'],
            capture_output=True, text=True
        )
        try:
            records = json.loads(result.stdout).get('result', {}).get('records', [])
        except (json.JSONDecodeError, AttributeError):
            records = []
        for r in records:
            order = r.get('Order') or {}
            p2    = r.get('Product2') or {}
            all_rows.append({
                'AccountId':            order.get('AccountId', ''),
                'Product2.Name':        p2.get('Name', ''),
                'Product2.Family':      p2.get('Family', ''),
                'Product2.ProductCode': p2.get('ProductCode', ''),
            })
        if (i + 1) % 10 == 0 or (i + 1) == len(batches):
            print(f"  Batch {i+1}/{len(batches)} done ({len(all_rows)} rows so far)")

    with open(ACCOUNT_ASSETS_CSV, 'w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=['AccountId', 'Product2.Name', 'Product2.Family', 'Product2.ProductCode'])
        w.writeheader()
        w.writerows(all_rows)
    print(f"  → {ACCOUNT_ASSETS_CSV} ({len(all_rows)} rows)")
    return all_rows


def build_account_category(asset_rows):
    """Classify each account based on its purchased OrderItem products."""
    account_category = {}
    for row in asset_rows:
        aid = row.get('AccountId', '')
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
        asset_rows = fetch_order_items(account_ids)
        build_account_category(asset_rows)
    else:
        print("No account-linked activities found — writing empty files.")
        open(ACCOUNT_ASSETS_CSV, 'w').write("AccountId,Product2.Name,Product2.Family,Product2.ProductCode\n")
        open(ACCOUNT_CATEGORY_CSV, 'w').write("AccountId,Category\n")
