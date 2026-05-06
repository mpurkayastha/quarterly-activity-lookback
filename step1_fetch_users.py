"""
Step 1: Pull all active users under the manager hierarchy (up to 5 levels deep)
and fetch their geo region from sfbase__Region__c.

Outputs:
  - config.USERS_CSV     (Id, Name, Title, ManagerId)
  - config.USERS_GEO_CSV (Id, Name, sfbase__Region__c, sfbase__Subregion__c)
"""

import subprocess, json, csv
from config import TARGET_ORG, DEFOE_MANAGER_ID, USERS_CSV, USERS_GEO_CSV


def fetch_users():
    query = f"""
        SELECT Id, Name, Title, ManagerId FROM User
        WHERE IsActive = true AND (
            ManagerId = '{DEFOE_MANAGER_ID}'
            OR Manager.ManagerId = '{DEFOE_MANAGER_ID}'
            OR Manager.Manager.ManagerId = '{DEFOE_MANAGER_ID}'
            OR Manager.Manager.Manager.ManagerId = '{DEFOE_MANAGER_ID}'
            OR Manager.Manager.Manager.Manager.ManagerId = '{DEFOE_MANAGER_ID}'
        )
    """.strip()

    print("Fetching DeFoe org users...")
    result = subprocess.run(
        ['sf', 'data', 'query', '--target-org', TARGET_ORG,
         '--query', query, '--result-format', 'csv'],
        capture_output=True, text=True
    )
    with open(USERS_CSV, 'w') as f:
        f.write(result.stdout)

    ids = [r['Id'] for r in csv.DictReader(open(USERS_CSV))]
    print(f"  Found {len(ids)} users → {USERS_CSV}")
    return ids


def fetch_geo(ids):
    print("Fetching user geo regions...")
    all_records = []
    for batch in [ids[:200], ids[200:]]:
        if not batch:
            continue
        id_list = "','".join(batch)
        query = f"SELECT Id, Name, sfbase__Region__c, sfbase__Subregion__c FROM User WHERE Id IN ('{id_list}')"
        result = subprocess.run(
            ['sf', 'data', 'query', '--target-org', TARGET_ORG,
             '--query', query, '--result-format', 'json'],
            capture_output=True, text=True
        )
        all_records.extend(json.loads(result.stdout)['result']['records'])

    with open(USERS_GEO_CSV, 'w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=['Id', 'Name', 'sfbase__Region__c', 'sfbase__Subregion__c'])
        w.writeheader()
        for r in all_records:
            w.writerow({k: r.get(k, '') for k in ['Id', 'Name', 'sfbase__Region__c', 'sfbase__Subregion__c']})

    print(f"  Wrote {len(all_records)} geo rows → {USERS_GEO_CSV}")


if __name__ == "__main__":
    ids = fetch_users()
    fetch_geo(ids)
