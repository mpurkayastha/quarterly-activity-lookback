"""
Step 4: Query SE_Activity CRM Analytics dataset for total utilization hours,
classified as D360/AF/Both/Other per person, and produce an "Other" drill-down.

Outputs:
  - config.SE_BREAKDOWN_CSV  (per-person D360/AF/Both/Other for both periods)
  - config.SE_OTHER_CSV      (org-level Other breakdown by sub-category)
"""

import subprocess, json, urllib.request, csv, re, collections
from config import (TARGET_ORG, USERS_CSV, USERS_GEO_CSV, OLI_CSV,
                    SE_DATASET, SE_CURRENT_YEAR, SE_CURRENT_QTR,
                    SE_PRIOR_YEAR, SE_PRIOR_QTR,
                    SE_MANAGER_FIELD, SE_MANAGER_NAME,
                    CURRENT_PERIOD, PRIOR_PERIOD,
                    SE_BREAKDOWN_CSV, SE_OTHER_CSV,
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


def _sf_api(token, instance, saql):
    body = json.dumps({"query": saql}).encode()
    req = urllib.request.Request(
        f"{instance}/services/data/v59.0/wave/query", data=body,
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    )
    try:
        with urllib.request.urlopen(req) as resp:
            return json.loads(resp.read()).get('results', {}).get('records', [])
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"SAQL error: {e.read().decode()}")


def build_opp_category():
    opp_category = {}
    for row in csv.DictReader(open(OLI_CSV)):
        oid = row['OpportunityId']
        text = f"{row.get('Product2.Name','')} {row.get('Product2.Family','')} {row.get('Product2.ProductCode','')}"
        is_d360, is_af = bool(D360_PAT.search(text)), bool(AF_PAT.search(text))
        cat = 'Both' if (is_d360 and is_af) else ('D360' if is_d360 else ('AF' if is_af else 'Neither'))
        prev = opp_category.get(oid, 'Neither')
        opp_category[oid] = MERGE.get(frozenset([prev, cat]), 'Both')
    return opp_category


def se_activity_per_person(token, instance, yr, qtr, opp_category):
    """Returns dict: owner_name -> {D360, AF, Both, Other, Total} hours"""
    base_filter = (f"'Owner.ManagerL05.Name' == \"{SE_MANAGER_NAME}\" && "
                   f"'ActivityDate_Year_Fiscal' == \"{yr}\" && "
                   f"'ActivityDate_Quarter_Fiscal' == \"{qtr}\"")

    # Total hours by owner
    total_records = _sf_api(token, instance, f"""
q = load "{SE_DATASET}";
q = filter q by {base_filter};
q = group q by 'Owner.Name';
q = foreach q generate 'Owner.Name' as owner, sum('DurationInMinutes') as mins;
q = limit q 500;
""")

    # Opp-linked customer hours by owner x opp
    opp_records = _sf_api(token, instance, f"""
q = load "{SE_DATASET}";
q = filter q by {base_filter};
q = filter q by 'Customer_Related' == "Customer Related";
q = filter q by 'Full.OppId' != "-BLANK-";
q = group q by ('Owner.Name', 'Full.OppId');
q = foreach q generate 'Owner.Name' as owner, 'Full.OppId' as opp_id, sum('DurationInMinutes') as mins;
q = limit q 5000;
""")

    per_person = collections.defaultdict(lambda: {'D360': 0, 'AF': 0, 'Both': 0, 'Other': 0, 'Total': 0})
    for r in total_records:
        per_person[r['owner']]['Total'] = r['mins'] / 60

    for r in opp_records:
        hrs = r['mins'] / 60
        cat = opp_category.get(r['opp_id'], 'Neither')
        if cat in ('D360', 'AF', 'Both'):
            per_person[r['owner']][cat] += hrs

    for h in per_person.values():
        h['Other'] = max(0, h['Total'] - h['D360'] - h['AF'] - h['Both'])

    return per_person


def other_breakdown(token, instance, yr, qtr, period_label, opp_category):
    base_filter = (f"'Owner.ManagerL05.Name' == \"{SE_MANAGER_NAME}\" && "
                   f"'ActivityDate_Year_Fiscal' == \"{yr}\" && "
                   f"'ActivityDate_Quarter_Fiscal' == \"{qtr}\"")

    # Customer-related opp-linked by opp
    opp_records = _sf_api(token, instance, f"""
q = load "{SE_DATASET}";
q = filter q by {base_filter};
q = filter q by 'Customer_Related' == "Customer Related";
q = filter q by 'Full.OppId' != "-BLANK-";
q = group q by 'Full.OppId';
q = foreach q generate 'Full.OppId' as opp_id, sum('DurationInMinutes') as mins;
q = limit q 2000;
""")
    d360 = af = both = non_d360af = 0
    for r in opp_records:
        hrs = r['mins'] / 60
        cat = opp_category.get(r['opp_id'], 'Neither')
        if cat == 'D360':    d360 += hrs
        elif cat == 'AF':    af += hrs
        elif cat == 'Both':  both += hrs
        else:                non_d360af += hrs

    # Customer-related non-opp by WhatObjectId
    nonopp_records = _sf_api(token, instance, f"""
q = load "{SE_DATASET}";
q = filter q by {base_filter};
q = filter q by 'Customer_Related' == "Customer Related";
q = filter q by 'Full.OppId' == "-BLANK-";
q = group q by 'WhatObjectId';
q = foreach q generate 'WhatObjectId' as wobj, sum('DurationInMinutes') as mins;
q = limit q 50;
""")
    nonopp = {str(r['wobj'])[:3]: r['mins'] / 60 for r in nonopp_records}
    account     = nonopp.get('001', 0)
    no_whatid   = nonopp.get('-BL', 0)
    other_nonopp = sum(v for k, v in nonopp.items() if k not in ('001', '-BL'))

    # Non-customer-related
    ncr_records = _sf_api(token, instance, f"""
q = load "{SE_DATASET}";
q = filter q by {base_filter};
q = filter q by 'Customer_Related' == "Non Customer Related";
q = group q by 'Customer_Related';
q = foreach q generate 'Customer_Related' as x, sum('DurationInMinutes') as mins;
q = limit q 5;
""")
    ncr = sum(r['mins'] / 60 for r in ncr_records)

    # Blank
    blk_records = _sf_api(token, instance, f"""
q = load "{SE_DATASET}";
q = filter q by {base_filter};
q = filter q by 'Customer_Related' == "-BLANK-";
q = group q by 'Customer_Related';
q = foreach q generate 'Customer_Related' as x, sum('DurationInMinutes') as mins;
q = limit q 5;
""")
    blk = sum(r['mins'] / 60 for r in blk_records)

    total      = d360 + af + both + non_d360af + account + no_whatid + other_nonopp + ncr + blk
    classified = d360 + af + both
    other_tot  = total - classified

    def pct(v): return f"{100*v/total:.1f}%" if total else "0.0%"

    return [
        {"Period": period_label, "Category": "Classified",       "Subcategory": "Customer meetings — D360 opp",                          "Hours": round(d360),        "Pct_of_Total": pct(d360)},
        {"Period": period_label, "Category": "Classified",       "Subcategory": "Customer meetings — AF opp",                           "Hours": round(af),          "Pct_of_Total": pct(af)},
        {"Period": period_label, "Category": "Classified",       "Subcategory": "Customer meetings — Both D360+AF opp",                 "Hours": round(both),        "Pct_of_Total": pct(both)},
        {"Period": period_label, "Category": "Classified TOTAL", "Subcategory": "",                                                      "Hours": round(classified),  "Pct_of_Total": pct(classified)},
        {"Period": period_label, "Category": "Other",            "Subcategory": "Customer meetings — opp not tagged D360/AF",           "Hours": round(non_d360af),  "Pct_of_Total": pct(non_d360af)},
        {"Period": period_label, "Category": "Other",            "Subcategory": "Customer meetings — linked to Account only (no Opp)",  "Hours": round(account),     "Pct_of_Total": pct(account)},
        {"Period": period_label, "Category": "Other",            "Subcategory": "Customer meetings — no WhatId logged",                 "Hours": round(no_whatid),   "Pct_of_Total": pct(no_whatid)},
        {"Period": period_label, "Category": "Other",            "Subcategory": "Customer meetings — Case/DSR/StratInit/Campaign",      "Hours": round(other_nonopp),"Pct_of_Total": pct(other_nonopp)},
        {"Period": period_label, "Category": "Other",            "Subcategory": "Internal / non-customer meetings",                     "Hours": round(ncr),         "Pct_of_Total": pct(ncr)},
        {"Period": period_label, "Category": "Other",            "Subcategory": "Blank / unclassified",                                 "Hours": round(blk),         "Pct_of_Total": pct(blk)},
        {"Period": period_label, "Category": "Other TOTAL",      "Subcategory": "",                                                      "Hours": round(other_tot),   "Pct_of_Total": pct(other_tot)},
        {"Period": period_label, "Category": "GRAND TOTAL",      "Subcategory": "",                                                      "Hours": round(total),       "Pct_of_Total": "100.0%"},
    ]


if __name__ == "__main__":
    r = subprocess.run(['sf', 'org', 'display', '--target-org', TARGET_ORG, '--json'],
                       capture_output=True, text=True)
    d = json.loads(r.stdout)['result']
    token, instance = d['accessToken'], d['instanceUrl']

    opp_category = build_opp_category()

    # Load user/geo maps for region+geo lookup
    users = {u['Id']: u for u in csv.DictReader(open(USERS_CSV))}
    geo_norm = {'LACA': 'LATAM', 'JP': 'APAC', 'AMER': 'AMER', 'EMEA': 'EMEA', 'APAC': 'APAC', 'WW': 'WW', '': 'Unknown'}
    geo_map = {r['Name']: geo_norm.get(r['sfbase__Region__c'] or '', r['sfbase__Region__c'] or 'Unknown')
               for r in csv.DictReader(open(USERS_GEO_CSV))}

    def get_region(name):
        uid = next((uid for uid, u in users.items() if u['Name'] == name), None)
        if not uid:
            return 'Unknown'
        visited, curr, chain = set(), uid, []
        while curr and curr not in visited:
            visited.add(curr); u = users.get(curr)
            if not u: break
            chain.append(u['Name'])
            if u.get('ManagerId') == DEFOE_MANAGER_ID: return u['Name']
            curr = u.get('ManagerId')
        for n in chain:
            if n in REGION_LEADS: return n
        return 'Unknown'

    # --- Per-person SE_Activity CSV ---
    print(f"Querying SE_Activity for {CURRENT_PERIOD}...")
    p1 = se_activity_per_person(token, instance, SE_CURRENT_YEAR, SE_CURRENT_QTR, opp_category)
    print(f"Querying SE_Activity for {PRIOR_PERIOD}...")
    p2 = se_activity_per_person(token, instance, SE_PRIOR_YEAR,   SE_PRIOR_QTR,   opp_category)

    all_names = set(p1.keys()) | set(p2.keys())
    out_rows = []
    for name in sorted(all_names):
        uid = next((uid for uid, u in users.items() if u['Name'] == name), None)
        title = users[uid]['Title'] if uid else ''
        out_rows.append({
            'Region_Manager':   get_region(name),
            'Geo':              geo_map.get(name, 'Unknown'),
            'Name':             name,
            'Title':            title,
            f'{CURRENT_PERIOD}_D360_hrs':  round(p1[name]['D360'], 1),
            f'{CURRENT_PERIOD}_AF_hrs':    round(p1[name]['AF'], 1),
            f'{CURRENT_PERIOD}_Both_hrs':  round(p1[name]['Both'], 1),
            f'{CURRENT_PERIOD}_Other_hrs': round(p1[name]['Other'], 1),
            f'{CURRENT_PERIOD}_Total_hrs': round(p1[name]['Total'], 1),
            f'{PRIOR_PERIOD}_D360_hrs':    round(p2[name]['D360'], 1),
            f'{PRIOR_PERIOD}_AF_hrs':      round(p2[name]['AF'], 1),
            f'{PRIOR_PERIOD}_Both_hrs':    round(p2[name]['Both'], 1),
            f'{PRIOR_PERIOD}_Other_hrs':   round(p2[name]['Other'], 1),
            f'{PRIOR_PERIOD}_Total_hrs':   round(p2[name]['Total'], 1),
            'YoY_total_hrs':    round(p1[name]['Total'] - p2[name]['Total'], 1),
        })

    out_rows.sort(key=lambda r: (r['Region_Manager'], r['Geo'], r['Name']))
    cp, pp = CURRENT_PERIOD, PRIOR_PERIOD
    fields = ['Region_Manager','Geo','Name','Title',
              f'{cp}_D360_hrs',f'{cp}_AF_hrs',f'{cp}_Both_hrs',f'{cp}_Other_hrs',f'{cp}_Total_hrs',
              f'{pp}_D360_hrs',f'{pp}_AF_hrs',f'{pp}_Both_hrs',f'{pp}_Other_hrs',f'{pp}_Total_hrs',
              'YoY_total_hrs']
    with open(SE_BREAKDOWN_CSV, 'w', newline='') as f:
        csv.DictWriter(f, fieldnames=fields).writeheader()
        csv.DictWriter(f, fieldnames=fields).writerows(out_rows)
    print(f"Wrote {len(out_rows)} rows → {SE_BREAKDOWN_CSV}")

    # --- Other drill-down CSV ---
    print("Computing Other breakdown...")
    rows = (other_breakdown(token, instance, SE_CURRENT_YEAR, SE_CURRENT_QTR, CURRENT_PERIOD, opp_category) +
            other_breakdown(token, instance, SE_PRIOR_YEAR,   SE_PRIOR_QTR,   PRIOR_PERIOD,   opp_category))
    fields = ["Period", "Category", "Subcategory", "Hours", "Pct_of_Total"]
    with open(SE_OTHER_CSV, 'w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(rows)
    print(f"Wrote {len(rows)} rows → {SE_OTHER_CSV}")
