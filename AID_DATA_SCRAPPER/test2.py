# --- test.py ---

import requests

url = "http://127.0.0.1:8000/get-data"

########################################Payload General#################################

payload = {
    "sources": [
        "worldbank",
        # "ghed",
        # "oecd",
        # "foreignassistance",
        # "sdg",
        # "iati",
        # "fcdo",
        # "bii"
    ],
    "filters": {
        "country": "Colombia",                   # standardized input (mapped internally)
        "reporting_org": "UNICEF",              # will be mapped or ignored based on source
        "sector": "Education",                     # standardized input
        "start_year": 2015,
        "end_year": 2025
    },
    "ingest_only": False,
    "proceed_if_duplicate": True
}

########################################Payload World Bank#################################

# payload = {
#     "sources": ["world_bank"],
#     "filters": {
#         "country_code": "NGA",               # ISO Alpha-3 country code (e.g., NGA for Nigeria)
#         "indicator_code": "SH.XPD.CHEX.GD.ZS",  # World Bank indicator code (e.g., health expenditure % of GDP)
#         "start_year": 2015,
#         "end_year": 2022
#     },
#     "ingest_only": False,
#     "proceed_if_duplicate": False
# }

########################################Payload WHO GHED#################################

# payload = {
#     "sources": ["ghed"],
#     "filters": {
#         "country": "Nigeria",
#         "start_year": 2015,
#         "end_year": 2021
#     },
#     "ingest_only": False,
#     "proceed_if_duplicate": False
# }

########################################Payload OECD#################################

# payload = {
#     "sources": ["oecd"],
#     "filters": {
#         "sector_name": "Health",
#         "countries": ["Afghanistan", "Pakistan"],
#         "start_year": 2018,
#         "end_year": 2022
#     },
#     "ingest_only": False,
#     "proceed_if_duplicate": False
# }

########################################Payload Foreign Assistance#################################

# payload = {
#     "sources": ["foreign_assistance"],
#     "filters": {
#         "Country Name": "Nigeria",
#         "US Sector Name": "Health",
#         "Fiscal Year": 2022
#     },
#     "ingest_only": False,
#     "proceed_if_duplicate": False
# }

########################################Payload SDGS#################################

# payload = {
#     "sources": ["sdg"],
#     "filters": {
#       "indicator": "3.1.1",
#       "country": "Pakistan",
#       "start_year": 2010,
#       "end_year": 2022
#     },
#     "ingest_only": False,
#     "proceed_if_duplicate": True
#   }

########################################Payload IATI#################################

# payload = {
#     "sources": ["iati"],
#     "filters": {
#         "country": "Nigeria",
#         "sector": "Basic Health",
#         "reporting_org": "United Nations Children Fund",
#         "start_year": "2020",
#         "end_year": "2024"
#     },
#    "ingest_only": False
# }

########################################Payload FCDO#################################

# payload = {
#   "sources": ["fcdo"],
#   "filters": {
#     "country": "Nigeria",
#     "sector": "Health",
#     "status": "Closed",
#     "participating_orgs": "Save the Children",
#     "start_year": "2020",
#     "end_year": "2024"
#   },
#   "ingest_only": False
# }

########################################Payload General#################################

# payload = {
#   "sources": ["oecd"],
#   "filters": {
#     "countries": ["Afghanistan"],
#     "start_year": "2018",
#     "end_year": "2022"
#   },
#   "ingest_only": False
# }
print(payload)
response = requests.post(url, json=payload)

# print("Status Code:", response.status_code)
# print("Response JSON:")
# print(response.json())
