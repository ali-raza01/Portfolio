import json
from pathlib import Path


class CountryNotFoundError(Exception):
    """Raised when a country name is not found in world_bank_countries.txt."""


class TopicNotFoundError(Exception):
    """Raised when a topic name is not found in world_bank_indicators.txt."""


# ------------------- Caching Layer -------------------
_INDICATORS = None
_COUNTRIES = None


def _load_indicators() -> list[dict]:
    global _INDICATORS
    if _INDICATORS is None:
        meta, page1 = json.load(
            open(Path(__file__).with_name("world_bank_indicators.txt"), "r", encoding="utf-8")
        )
        _INDICATORS = page1
    return _INDICATORS


def _load_countries() -> list[dict]:
    global _COUNTRIES
    if _COUNTRIES is None:
        meta, countries = json.load(
            open(Path(__file__).with_name("world_bank_countries.txt"), "r", encoding="utf-8")
        )
        _COUNTRIES = countries
    return _COUNTRIES


# ------------------- Main Public Utilities -------------------

def get_iso2_from_country(country_name: str) -> str:
    """
    Return the iso2Code for a country, given the full country name.
    Raises CountryNotFoundError if not found.
    """
    country_name = country_name.strip().lower()
    for country in _load_countries():
        if country["name"].strip().lower() == country_name:
            return country["iso2Code"]
    raise CountryNotFoundError(f"Country not found: '{country_name}'. Check spelling or update file.")


def get_indicator_code_from_topic(topic: str) -> str:
    """
    Return the first indicator ID whose topic value matches the input.
    Only topics are matched (not keyword searching in names).
    Raises TopicNotFoundError if not found.
    """
    topic_lc = topic.strip().lower()
    for row in _load_indicators():
        for t in row.get("topics", []):
            if t["value"].strip().lower() == topic_lc:
                return _trim_indicator(row["id"])
    raise TopicNotFoundError(f"No indicator found for topic '{topic}'. Update your file or correct spelling.")


def _trim_indicator(indicator_id: str) -> str:
    """Keep only the first 3 dot-separated segments of an indicator ID."""
    return ".".join(indicator_id.split(".")[:3])
