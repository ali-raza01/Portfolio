# --- unified_mapping.py ---

# Target columns mapped to preferred source-specific field names
COLUMN_MAPPING = {
    "country_code": {"worldbank": "countrycode", "foreignassistance": "Country Code", "sdg": "geoAreaCode"},
    "donor_id": {"worldbank": "donor_id", "foreignassistance": "Funding Agency ID"},
    "geography": {"worldbank": "geography"},
    "indicator_values": {"worldbank": "indicator.value"},
    "output_indicators": {"worldbank": "indicator"},
    "source_of_data": {"worldbank": "source"},
    "top_donors": {"worldbank": "donor_name"},
    "year_active": {"worldbank": "year_active", "foreignassistance": "Fiscal Year", "sdg": "timePeriodStart"},

    "admin_level_1": {"fcdo": "location_administrative_code"},
    "admin_level_2": {"fcdo": "location_administrative_code"},
    "beneficiary_count": {"fcdo": "planned_disbursement_value"},
    "evaluation_docs": {"fcdo": "document_link_url"},
    "latitude": {"fcdo": "location_point_pos"},
    "logframe_link": {"fcdo": "document_link_url"},
    "longitude": {"fcdo": "location_point_pos"},
    "outcome_indicator": {"fcdo": "result_indicator_*"},
    "total_disbursed_usd": {"fcdo": "transaction_value_usd"},

    "collaboration_type": {"iati": "collaboration_type_code"},

    "country": {"worldbank": "Country", "foreignassistance": "Country Name"},
    "disbursed_amount_usd": {"worldbank": "curr_total_commitment"},
    "document_links": {"worldbank": "projectdocs"},
    "donor": {"worldbank": "donor"},
    "donor_name": {"worldbank": "donor", "foreignassistance": "Funding Agency Name"},
    "end_date": {"worldbank": "closingdate", "foreignassistance": "Activity End Date"},
    "funding_modality": {"worldbank": "funding_modality", "foreignassistance": "Aid Type Group Name"},
    "funding_type": {"worldbank": "funding_type"},
    "implementer_name": {"worldbank": "impagency", "foreignassistance": "Implementing Partner Name"},
    "implementing_partner": {"worldbank": "impagency"},
    "last_updated": {"worldbank": "p2a_updated_date"},
    "project_description": {"worldbank": "projectdocs", "foreignassistance": "Activity Description"},
    "project_id": {"worldbank": "id", "foreignassistance": "Activity ID"},
    "project_status": {"worldbank": "projectstatusdisplay"},
    "project_title": {"worldbank": "project_name", "foreignassistance": "Activity Name"},
    "region": {"worldbank": "regionname", "foreignassistance": "Region Name"},
    "sector": {"worldbank": "sector1Name", "foreignassistance": "US Sector Name"},
    "start_date": {"worldbank": "boardapprovaldate", "foreignassistance": "Activity Start Date"},
    "status": {"worldbank": "status", "foreignassistance": "Transaction Type Name"},
    "subsector": {"worldbank": "sector2Name", "foreignassistance": "International Sector Name"},
    "total_commitment_usd": {"worldbank": "totalamt", "foreignassistance": "Current Dollar Amount"},
    "funding_amount_usd": {"foreignassistance": "activity_budget_amount"},
    "impact_score": {"sdg": "value"},
    "notes": {"sdg": "valueComments"},
    "subnational_area": {"sdg": "geoAreaName"}
}

# General filters to source-specific keys
FILTER_KEY_MAPPING = {
    "sdg": {
        "geoAreaCode": "country_code",
        "geoAreaName": "subnational_area",
        "timePeriodStart": "year_active",
        "value": "impact_score",
        "valueComments": "notes"
    },
    "country": {
        "iati": "country_code",
        "fcdo": "Benefiting Regions",
        "worldbank": "country",
        "foreignassistance": "Country Name",
        "sdg": "areaCode"
    },
    "region": {
        "worldbank": "region",
        "foreignassistance": "Region Name"
    },
    "income_level": {
        "worldbank": "incomeLevel"
    },
    "lending_type": {
        "worldbank": "lendingType"
    },
    "source": {
        "worldbank": "source"
    },
    "topic": {
        "worldbank": "topic"
    },
    "sector": {
        "iati": "sector_group",
        "fcdo": "Sectors",
        "worldbank": "sector",
        "foreignassistance": "US Sector Name"
    },
    "status": {
        "iati": "status_code",
        "fcdo": "Activity Status",
        "worldbank": "status",
        "foreignassistance": "Transaction Type Name"
    },
    "start_date": {
        "iati": "start-date",
        "foreignassistance": "Activity Start Date"
    },
    "end_date": {
        "iati": "end-date",
        "foreignassistance": "Activity End Date"
    },
    "reporting_org": {
        "iati": "reporting_ref"
    },
    "department": {
        "fcdo": "Government Departments"
    },
    "tags": {
        "fcdo": "Tags"
    },
    "participating_orgs": {
        "fcdo": "Participating Orgs"
    },
    "document_category": {
        "fcdo": "Document Categories"
    },
    "agency": {
        "foreignassistance": "Managing Agency Name"
    },
    "fiscal_year": {
        "foreignassistance": "Fiscal Year"
    },
    "indicator": {
        "sdg": "indicator"
    }
}

# Constants
SCRAPE_RUN_ID_FIELD = "scrape_run_id"

# Valid values per filter per source
FILTER_VALUE_FIXES = {
    "FCDO": {
        "Activity Status": {
            "active": "Active",
            "pipeline": "Pipeline",
            "completed": "Completed",
            "closed": "Closed"
        },
        "Government Departments": {
            "fcdo": "FCDO",
            "other": "Other Government Depts"
        },
        "Tags": {
            "climate": "climate",
            "disability": "disability",
            "humanitarian": "humanitarian",
            "education": "education"
        },
        "Sectors": {
            "122": "Health",
            "111": "Education",
            "150": "Government and Civil Society",
            "151": "Humanitarian"
        },
        "Participating Orgs": {
            "adb": "adb",
            "ibrd": "ibrd",
            "ifc": "ifc",
            "psi": "population service international"
        },
        "Benefiting Regions": {
            "NG": "Nigeria",
            "IN": "India",
            "PK": "Pakistan",
            "Africa": "Africa",
            "South Asia": "South Asia",
            "Middle East": "Middle East"
        },
        "Document Categories": {
            "activity": "Activity web page",
            "annual": "Annual report",
            "contract": "Contract",
            "budget": "Budget",
            "evaluation": "Evaluation"
        }
    },

    "World Bank": {
        "region": {
            "1": "East Asia & Pacific",
            "2": "Europe & Central Asia",
            "3": "Latin America & Caribbean",
            "4": "Middle East & North Africa",
            "6": "North America",
            "8": "South Asia",
            "9": "Sub-Saharan Africa"
        },
        "incomeLevel": {
            "HIC": "High income",
            "LIC": "Low income",
            "LMC": "Lower middle income",
            "UMC": "Upper middle income",
            "MIC": "Middle income",
            "LMY": "Low & middle income"
        },
        "lendingType": {
            "IBD": "IBRD",
            "IDB": "Blend",
            "IDX": "IDA"
        },
        "source": {
            "1": "Doing Business",
            "2": "World Development Indicators",
            "6": "International Debt Statistics",
            "16": "Health Nutrition and Population Statistics",
            "46": "Sustainable Development Goals",
            "88": "Food Prices for Nutrition"
        },
        "topic": {
            "1": "Agriculture & Rural Development",
            "2": "Aid Effectiveness",
            "3": "Economy & Growth",
            "4": "Education",
            "5": "Energy & Mining",
            "8": "Health",
            "10": "Social Protection & Labor",
            "11": "Poverty",
            "19": "Climate Change",
            "21": "Trade"
        },
        "Country": {
            "NG": "Nigeria",
            "IN": "India",
            "PK": "Pakistan"
        }
    },

    "IATI": {
        "Country": {
            "NG": "Nigeria",
            "KE": "Kenya",
            "GH": "Ghana",
            "AF": "Afghanistan"
        },
        "Sector Group": {
            "122": "Basic Health",
            "112": "Basic Education",
            "151": "Government & Civil Society"
        },
        "Reporting Organization": {
            "GB-GOV-1": "Foreign, Commonwealth & Development Office",
            "DK-CVR-31378028": "Danish International Development Agency"
        },
        "Activity Status": {
            "active": "2",
            "completed": "3",
            "planned": "1"
        }
    }
}

def validate_and_normalize_filters(source: str, filters: dict):
    source_mapping = FILTER_VALUE_FIXES.get(source)
    if not source_mapping:
        raise ValueError(f"No mapping found for source: {source}")

    corrected = {}
    for key, val in filters.items():
        mapping = source_mapping.get(key, {})
        corrected[key] = mapping.get(val, val)
    return corrected


FILTER_KEY_MAPPING["indicator"]     = { **FILTER_KEY_MAPPING.get("indicator", {}),
                                        "sdg": "indicator" }

FILTER_KEY_MAPPING["start_year"] = {
    "sdg": "timePeriodStart",
    "iati": "year_min"
}
FILTER_KEY_MAPPING["end_year"] = {
    "sdg": "timePeriodEnd",
    "iati": "year_max"
}
FILTER_KEY_MAPPING["area_code"]     = { "sdg": "areaCode" }

FILTER_KEY_MAPPING["country"]["foreignassistance"] = "Country Name"
FILTER_KEY_MAPPING["sector"]["foreignassistance"] = "US Sector Name"
FILTER_KEY_MAPPING["reporting_org"]["foreignassistance"] = "Funding Agency Name"
FILTER_KEY_MAPPING["start_year"]["foreignassistance"] = "Fiscal Year"
FILTER_KEY_MAPPING["end_year"]["foreignassistance"] = "Fiscal Year"