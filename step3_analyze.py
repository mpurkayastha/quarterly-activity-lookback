"""
Step 3: Classify opps as D360/AF/Both/Neither, roll up hours per person,
and write the per-person activity CSV.

Account-linked activities (WhatId starts with 001) are classified using
the account's purchased/installed Asset products (from step2b), so customer
success and expansion work is correctly attributed to D360/AF rather than
landing in Neither.

Output: config.LOOKBACK_CSV
"""

import csv, re, collections, os
from config import (USERS_CSV, USERS_GEO_CSV, OLI_CSV, LOOKBACK_CSV,
                    EVENTS_CURRENT_CSV, EVENTS_PRIOR_CSV,
                    TASKS_CURRENT_CSV, TASKS_PRIOR_CSV,
                    ACCOUNT_CATEGORY_CSV,
                    CURRENT_PERIOD, PRIOR_PERIOD,
                    DEFOE_MANAGER_ID, REGION_LEADS)

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


def build_opp_category():
    opp_category = {}
    for row in csv.DictReader(open(OLI_CSV)):
        oid = row['OpportunityId']
        text = f"{row.get('Product2.Name','')} {row.get('Product2.Family','')} {row.get('Product2.ProductCode','')}"
        is_d360 = bool(D360_PAT.search(text))
        is_af   = bool(AF_PAT.search(text))
        cat = 'Both' if (is_d360 and is_af) else ('D360' if is_d360 else ('AF' if is_af else 'Neither'))
        prev = opp_category.get(oid, 'Neither')
        opp_category[oid] = MERGE.get(frozenset([prev, cat]), 'Both')
    print(f"Classified {len(opp_category)} unique opps")
    return opp_category


def build_account_category():
    """Load account → D360/AF/Both/Neither classification from asset data."""
    if not os.path.exists(ACCOUNT_CATEGORY_CSV):
        print("  Warning: account_category.csv not found — run step2b first. Account-linked activities will be Neither.")
        return {}
    account_category = {}
    for row in csv.DictReader(open(ACCOUNT_CATEGORY_CSV)):
        account_category[row['AccountId']] = row['Category']
    d360 = sum(1 for v in account_category.values() if v == 'D360')
    af   = sum(1 for v in account_category.values() if v == 'AF')
    both = sum(1 for v in account_category.values() if v == 'Both')
    neither = sum(1 for v in account_category.values() if v == 'Neither')
    print(f"Account categories: D360={d360}, AF={af}, Both={both}, Neither={neither}")
    return account_category


def build_user_map():
    return {r['Id']: r for r in csv.DictReader(open(USERS_CSV))}


def build_geo_map():
    geo_map = {}
    norm = {'LACA': 'LATAM', 'JP': 'APAC', 'AMER': 'AMER',
            'EMEA': 'EMEA', 'APAC': 'APAC', 'WW': 'WW', '': 'Unknown'}
    for r in csv.DictReader(open(USERS_GEO_CSV)):
        raw = r['sfbase__Region__c'] or ''
        geo_map[r['Id']] = norm.get(raw, raw)
    return geo_map


def get_region(user_id, users):
    visited, uid, chain = set(), user_id, []
    while uid and uid not in visited:
        visited.add(uid)
        u = users.get(uid)
        if not u:
            break
        chain.append(u['Name'])
        if u.get('ManagerId') == DEFOE_MANAGER_ID:
            return u['Name']
        uid = u.get('ManagerId')
    for name in chain:
        if name in REGION_LEADS:
            return name
    return 'Unknown'


def classify_whatid(wid, opp_category, account_category):
    """
    Classify an activity by its WhatId:
      - 006... → opp-linked: use OLI-based opp_category
      - 001... → account-linked: use asset-based account_category
      - anything else → Neither (Case, DSR, StratInit, Campaign, blank)
    """
    if wid.startswith('006'):
        return opp_category.get(wid, 'Neither')
    if wid.startswith('001'):
        return account_category.get(wid, 'Neither')
    return 'Neither'


def accumulate_hours(opp_category, account_category):
    hours = collections.defaultdict(lambda: collections.defaultdict(float))

    def process_events(fname, period):
        for row in csv.DictReader(open(fname)):
            uid = row['OwnerId']
            wid = row.get('WhatId', '')
            hrs = float(row.get('DurationInMinutes') or 0) / 60.0
            cat = classify_whatid(wid, opp_category, account_category)
            hours[uid][f'{period}_{cat}'] += hrs

    def process_tasks(fname, period):
        for row in csv.DictReader(open(fname)):
            uid = row['OwnerId']
            wid = row.get('WhatId', '')
            hrs = float(row.get('Duration__c') or 0)
            cat = classify_whatid(wid, opp_category, account_category)
            hours[uid][f'{period}_{cat}'] += hrs

    process_events(EVENTS_CURRENT_CSV, CURRENT_PERIOD)
    process_events(EVENTS_PRIOR_CSV,   PRIOR_PERIOD)
    process_tasks(TASKS_CURRENT_CSV,   CURRENT_PERIOD)
    process_tasks(TASKS_PRIOR_CSV,     PRIOR_PERIOD)
    return hours


def write_csv(users, geo_map, hours):
    P1, P2 = CURRENT_PERIOD, PRIOR_PERIOD
    out_rows = []
    for uid, u in users.items():
        region = get_region(uid, users)
        geo = geo_map.get(uid, 'Unknown')
        h = hours[uid]
        row = {'Region_Manager': region, 'Manager': region,
               'Name': u['Name'], 'Title': u['Title'], 'Geo': geo}
        for p in [P1, P2]:
            d = round(h.get(f'{p}_D360', 0), 1)
            a = round(h.get(f'{p}_AF', 0), 1)
            b = round(h.get(f'{p}_Both', 0), 1)
            n = round(h.get(f'{p}_Neither', 0), 1)
            t = round(d + a + b + n, 1)
            row[f'{p}_D360_hrs']    = d
            row[f'{p}_AF_hrs']      = a
            row[f'{p}_Both_hrs']    = b
            row[f'{p}_Neither_hrs'] = n
            row[f'{p}_Total']       = int(t) if t == int(t) else t
        yoy = round(
            sum(row[f'{P1}_{c}_hrs'] for c in ['D360','AF','Both','Neither']) -
            sum(row[f'{P2}_{c}_hrs'] for c in ['D360','AF','Both','Neither']), 1)
        row['YoY_total'] = yoy
        out_rows.append(row)

    out_rows.sort(key=lambda r: (r['Region_Manager'], r['Name']))
    fields = ['Region_Manager','Manager','Name','Title','Geo',
              f'{P1}_D360_hrs',f'{P1}_AF_hrs',f'{P1}_Both_hrs',f'{P1}_Neither_hrs',f'{P1}_Total',
              f'{P2}_D360_hrs',f'{P2}_AF_hrs',f'{P2}_Both_hrs',f'{P2}_Neither_hrs',f'{P2}_Total',
              'YoY_total']
    with open(LOOKBACK_CSV, 'w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(out_rows)
    print(f"Wrote {len(out_rows)} rows → {LOOKBACK_CSV}")


if __name__ == "__main__":
    opp_category     = build_opp_category()
    account_category = build_account_category()
    users            = build_user_map()
    geo_map          = build_geo_map()
    hours            = accumulate_hours(opp_category, account_category)
    write_csv(users, geo_map, hours)
