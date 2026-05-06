"""
Step 5: Send all results to Slack as DM messages.

Sends:
  1. Opp-linked summary: org-wide region table (new DM)
  2. Per-manager × region breakdown (thread replies)
  3. SE_Activity per-person CSV, chunked (thread replies)
  4. SE_Activity Other breakdown table + CSV (new DM + thread reply)

Requires the Slack MCP server to be configured in Claude Code, or set
SLACK_BOT_TOKEN env var and use the requests-based fallback below.
"""

import csv, os, urllib.request, json
from config import SLACK_CHANNEL, LOOKBACK_CSV, USERS_GEO_CSV, USERS_CSV, \
                   SE_BREAKDOWN_CSV, SE_OTHER_CSV, CURRENT_PERIOD, PRIOR_PERIOD

SLACK_TOKEN = os.environ.get("SLACK_BOT_TOKEN", "")


def slack_post(message, thread_ts=None):
    payload = {"channel": SLACK_CHANNEL, "text": message}
    if thread_ts:
        payload["thread_ts"] = thread_ts
    req = urllib.request.Request(
        "https://slack.com/api/chat.postMessage",
        data=json.dumps(payload).encode(),
        headers={"Authorization": f"Bearer {SLACK_TOKEN}",
                 "Content-Type": "application/json"}
    )
    with urllib.request.urlopen(req) as resp:
        data = json.loads(resp.read())
    if not data.get("ok"):
        raise RuntimeError(f"Slack error: {data.get('error')}")
    return data["ts"]


def pct(a, total):
    return f"{100*a/total:.1f}%" if total else "0%"


def sum_rows(rlist, period):
    d = sum(float(r[f'{period}_D360_hrs']) for r in rlist)
    a = sum(float(r[f'{period}_AF_hrs']) for r in rlist)
    b = sum(float(r[f'{period}_Both_hrs']) for r in rlist)
    n = sum(float(r[f'{period}_Neither_hrs']) for r in rlist)
    return d, a, b, n, d + a + b + n


def build_region_summary(rows, P1, P2):
    geo_order = ['AMER', 'EMEA', 'APAC', 'LATAM', 'WW']
    lines = [f"*AFD360 Org — {P1} vs {P2} | Opp-Linked Activity by Region & Manager*", "",
             "*TOTAL BY REGION (all managers)*", "```",
             f"{'Region':<8} {'#':>4}  {'D360%':>7} {'AF%':>6} {'Both%':>6} {'Neith%':>7} {P1[:6]:>7}  {'D360%':>7} {'AF%':>6} {P2[:6]:>7}  {'YoY':>6}",
             "-" * 80]
    for geo_val in geo_order:
        geo_rows = [r for r in rows if r.get('Geo') == geo_val]
        if not geo_rows:
            continue
        d1, a1, b1, n1, t1 = sum_rows(geo_rows, P1)
        d2, a2, b2, n2, t2 = sum_rows(geo_rows, P2)
        lines.append(f"{geo_val:<8} {len(geo_rows):>4}  {pct(d1+b1,t1):>7} {pct(a1+b1,t1):>6} {pct(b1,t1):>6} {pct(n1,t1):>7} {t1:>7.0f}  {pct(d2+b2,t2):>7} {pct(a2+b2,t2):>6} {t2:>7.0f}  {t1-t2:>+6.0f}")
    lines.append("```")
    return "\n".join(lines)


def build_manager_msg(rows, mgr, P1, P2):
    geo_order = ['AMER', 'EMEA', 'APAC', 'LATAM', 'WW']
    mgr_rows = [r for r in rows if r['Region_Manager'] == mgr]
    d1, a1, b1, n1, t1 = sum_rows(mgr_rows, P1)
    d2, a2, b2, n2, t2 = sum_rows(mgr_rows, P2)
    lines = [f"*{mgr}'s Team — by Region*",
             f"_Overall: {P1} D360={pct(d1+b1,t1)} AF={pct(a1+b1,t1)} ({t1:.0f}h) | "
             f"{P2} D360={pct(d2+b2,t2)} AF={pct(a2+b2,t2)} ({t2:.0f}h) | YoY: {t1-t2:+.0f}h_",
             "```"]
    for geo_val in geo_order:
        geo_rows = [r for r in mgr_rows if r.get('Geo') == geo_val]
        if not geo_rows:
            lines.append(f"{geo_val:<8}  —")
            continue
        dg1, ag1, bg1, ng1, tg1 = sum_rows(geo_rows, P1)
        dg2, ag2, bg2, ng2, tg2 = sum_rows(geo_rows, P2)
        lines.append(f"{geo_val:<8} {len(geo_rows):>4}  {pct(dg1+bg1,tg1):>7} {pct(ag1+bg1,tg1):>6} "
                     f"{pct(bg1,tg1):>6} {pct(ng1,tg1):>7} {tg1:>7.0f}  "
                     f"{pct(dg2+bg2,tg2):>7} {pct(ag2+bg2,tg2):>6} {tg2:>7.0f}  {tg1-tg2:>+6.0f}")
    lines.append("```")
    return "\n".join(lines)


def chunk_csv(filepath, max_chars=4300):
    rows = list(csv.DictReader(open(filepath)))
    if not rows:
        return []
    fields = list(rows[0].keys())
    header = ','.join(fields)
    chunks, current, current_len = [], [header], len(header) + 1
    for r in rows:
        line = ','.join(str(r[f]) for f in fields)
        if current_len + len(line) + 1 > max_chars and len(current) > 1:
            chunks.append('\n'.join(current))
            current, current_len = [header, line], len(header) + len(line) + 2
        else:
            current.append(line)
            current_len += len(line) + 1
    if len(current) > 1:
        chunks.append('\n'.join(current))
    return chunks


def build_other_summary():
    rows = list(csv.DictReader(open(SE_OTHER_CSV)))
    P1 = CURRENT_PERIOD
    P2 = PRIOR_PERIOD
    p1_rows = {r['Category'] + '|' + r['Subcategory']: r for r in rows if r['Period'] == P1}
    p2_rows = {r['Category'] + '|' + r['Subcategory']: r for r in rows if r['Period'] == P2}

    order = [
        ("Classified",       "Customer meetings — D360 opp"),
        ("Classified",       "Customer meetings — AF opp"),
        ("Classified",       "Customer meetings — Both D360+AF opp"),
        ("Classified TOTAL", ""),
        None,
        ("Other",            "Customer meetings — opp not tagged D360/AF"),
        ("Other",            "Customer meetings — linked to Account only (no Opp)"),
        ("Other",            "Customer meetings — no WhatId logged"),
        ("Other",            "Customer meetings — Case/DSR/StratInit/Campaign"),
        ("Other",            "Internal / non-customer meetings"),
        ("Other",            "Blank / unclassified"),
        ("Other TOTAL",      ""),
        None,
        ("GRAND TOTAL",      ""),
    ]

    lines = [f"*AFD360 Org — SE_Activity 'Other' Breakdown | {P1} vs {P2}*", "",
             "_'Other' = everything not classified as D360, AF, or Both_", "```",
             f"{'Category':<52} {P1[:6]:>7}  {'%':>6}  {P2[:6]:>7}  {'%':>6}",
             "-" * 80]
    for item in order:
        if item is None:
            lines.append("")
            continue
        cat, sub = item
        key = f"{cat}|{sub}"
        r1 = p1_rows.get(key, {})
        r2 = p2_rows.get(key, {})
        label = sub if sub else cat
        h1 = r1.get('Hours', 0)
        h2 = r2.get('Hours', 0)
        p1_pct = r1.get('Pct_of_Total', '—')
        p2_pct = r2.get('Pct_of_Total', '—')
        lines.append(f"{label:<52} {str(h1):>7}  {p1_pct:>6}  {str(h2):>7}  {p2_pct:>6}")
    lines += ["```", "",
              "*Key takeaways:*",
              "• *Internal meetings* = largest slice of Other — 1:1s, team standups, internal planning",
              "• *Customer meetings on non-D360/AF opps* = real coverage but for Core Cloud, MuleSoft, etc.",
              "• *Account/no WhatId* = customer time not linked to any deal — not captured in deal metrics",
              "• *Blank* = activities with no Customer_Related flag set"]
    return "\n".join(lines)


if __name__ == "__main__":
    if not SLACK_TOKEN:
        print("ERROR: Set SLACK_BOT_TOKEN environment variable before running.")
        print("  export SLACK_BOT_TOKEN=xoxb-...")
        exit(1)

    P1, P2 = CURRENT_PERIOD, PRIOR_PERIOD

    # Load geo-enriched rows
    geo_norm = {'LACA': 'LATAM', 'JP': 'APAC', 'AMER': 'AMER', 'EMEA': 'EMEA', 'APAC': 'APAC', 'WW': 'WW', '': 'Unknown'}
    name_to_id = {r['Name']: r['Id'] for r in csv.DictReader(open(USERS_CSV))}
    geo_by_id = {r['Id']: geo_norm.get(r['sfbase__Region__c'] or '', 'Unknown')
                 for r in csv.DictReader(open(USERS_GEO_CSV))}
    rows = list(csv.DictReader(open(LOOKBACK_CSV)))
    for r in rows:
        r['Geo'] = geo_by_id.get(name_to_id.get(r['Name'], ''), 'Unknown')

    managers = ['Aiysha Mubarik', 'Salman Mian', 'Teddy Griffin', 'Anil Dindigal']

    # 1. Opp-linked region summary
    print("Sending opp-linked region summary...")
    summary_msg = build_region_summary(rows, P1, P2)
    summary_ts = slack_post(summary_msg)
    print(f"  Sent (ts={summary_ts})")

    # 2. Per-manager × region (threaded)
    for mgr in managers:
        print(f"  Sending {mgr} breakdown...")
        slack_post(build_manager_msg(rows, mgr, P1, P2), thread_ts=summary_ts)

    # 3. SE_Activity per-person CSV (chunked, threaded)
    print("Sending SE_Activity per-person CSV...")
    chunks = chunk_csv(SE_BREAKDOWN_CSV)
    total_chunks = len(chunks)
    se_ts = None
    for i, chunk in enumerate(chunks):
        msg = f"SE_Activity breakdown — per person with D360/AF/Both/Other ({i+1}/{total_chunks})\n```\n{chunk}\n```"
        if i == 0:
            se_ts = slack_post(msg)
        else:
            slack_post(msg, thread_ts=se_ts)
    print(f"  Sent {total_chunks} chunks")

    # 4. Other breakdown summary + CSV (new DM + thread)
    print("Sending Other breakdown...")
    other_ts = slack_post(build_other_summary())
    csv_content = open(SE_OTHER_CSV).read()
    slack_post(f"CSV data:\n```\n{csv_content}\n```", thread_ts=other_ts)
    print("  Done.")
    print("\nAll messages sent.")
