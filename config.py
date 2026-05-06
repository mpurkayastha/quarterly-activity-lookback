# config.py — update these values each quarter before running

# Salesforce
TARGET_ORG = "mpurkayastha@salesforce.com"   # sf CLI alias or username
DEFOE_MANAGER_ID = "00530000009MYBnAAO"       # John DeFoe's Salesforce User ID

# Slack
SLACK_CHANNEL = "D0139UAK29J"                 # Slackbot DM channel ID

# Fiscal periods — update each quarter
CURRENT_PERIOD  = "FY27Q1"
PRIOR_PERIOD    = "FY26Q1"

# SOQL date ranges  (inclusive on both ends for Tasks; exclusive end for Events)
CURRENT_START   = "2026-02-01"
CURRENT_END     = "2026-04-30"
PRIOR_START     = "2025-02-01"
PRIOR_END       = "2025-04-30"

# SE_Activity CRM Analytics dataset
SE_DATASET      = "0Fb30000000TNFvCAO/0Fced000009QlCbCAK"
SE_CURRENT_YEAR = "2027"    # string — fiscal year number in SE_Activity
SE_CURRENT_QTR  = "1"       # string — fiscal quarter number
SE_PRIOR_YEAR   = "2026"
SE_PRIOR_QTR    = "1"
# DeFoe's level in SE_Activity manager hierarchy (CEO=L01 → DeFoe=L05)
SE_MANAGER_FIELD = "Owner.ManagerL05.Name"
SE_MANAGER_NAME  = "John DeFoe"

# DeFoe's 4 direct reports — used to map people to org regions
REGION_LEADS = {"Aiysha Mubarik", "Salman Mian", "Teddy Griffin", "Anil Dindigal"}

# Output files
USERS_CSV           = "/tmp/defoe_users.csv"
USERS_GEO_CSV       = "/tmp/defoe_users_geo.csv"
EVENTS_CURRENT_CSV  = "/tmp/events_current_bulk.csv"
EVENTS_PRIOR_CSV    = "/tmp/events_prior_bulk.csv"
TASKS_CURRENT_CSV   = "/tmp/tasks_current_bulk.csv"
TASKS_PRIOR_CSV     = "/tmp/tasks_prior_bulk.csv"
OLI_CSV             = "/tmp/oli_skus.csv"
ACCOUNT_ASSETS_CSV  = "/tmp/account_assets.csv"
ACCOUNT_CATEGORY_CSV= "/tmp/account_category.csv"
LOOKBACK_CSV        = "/tmp/q1_activity_lookback.csv"
SE_BREAKDOWN_CSV    = "/tmp/se_activity_breakdown.csv"
SE_OTHER_CSV        = "/tmp/se_activity_other_breakdown.csv"
