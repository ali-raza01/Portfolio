import pytest
from fastapi.testclient import TestClient

# Import the FastAPI app and the DB dependency from the merged API file
from main import app, get_db


class FakeResult:
    """Minimal object mimicking SQLAlchemy Result."""

    def __init__(self, data=None):
        # a list of dict-like rows (empty list by default)
        self._data = data or []

    def fetchall(self):
        return self._data


class FakeSession:
    """Stub SQLAlchemy session that always returns an empty result set."""

    def execute(self, *args, **kwargs):
        # Return FakeResult regardless of the query/params
        return FakeResult()

    def close(self):
        pass


@pytest.fixture(autouse=True)
def override_get_db():
    """Override the real DB dependency with a fake in every test."""

    def _get_db():
        yield FakeSession()

    # Apply the override
    app.dependency_overrides[get_db] = _get_db

    yield  # run the test

    # Remove overrides after each test
    app.dependency_overrides.clear()


def client():
    """Return a TestClient bound to the FastAPI app."""
    return TestClient(app)


# ---------------------------------------------------------------------------
# API 1: year_country_funding_summary
# ---------------------------------------------------------------------------

def test_year_country_funding_summary_success():
    c = client()
    resp = c.get(
        "/api/v1/projects/summary/year-country",
        params={"start_year": 2015, "end_year": 2020},
    )
    assert resp.status_code == 200
    assert isinstance(resp.json(), list)


def test_year_country_funding_summary_invalid_fund_col():
    c = client()
    resp = c.get(
        "/api/v1/projects/summary/year-country",
        params={
            "start_year": 2015,
            "end_year": 2020,
            "fund_col": "invalid_column",
        },
    )
    assert resp.status_code == 400


# ---------------------------------------------------------------------------
# API 2: project_title_description_list
# ---------------------------------------------------------------------------

def test_project_title_description_list_defaults():
    c = client()
    resp = c.get("/api/v1/projects/title-description")
    assert resp.status_code == 200
    assert isinstance(resp.json(), list)


# ---------------------------------------------------------------------------
# API 3: age_group_beneficiary_summary
# ---------------------------------------------------------------------------

def test_age_group_beneficiary_summary_invalid_bins():
    c = client()
    resp = c.get(
        "/api/v1/beneficiaries/summary/age-group",
        params={"bins": "abc"},
    )
    assert resp.status_code == 400


def test_age_group_beneficiary_summary_defaults():
    c = client()
    resp = c.get("/api/v1/beneficiaries/summary/age-group")
    assert resp.status_code == 200
    assert isinstance(resp.json(), list)


# ---------------------------------------------------------------------------
# API 4: funding_group_breakdown
# ---------------------------------------------------------------------------

def test_funding_group_breakdown_success():
    c = client()
    resp = c.get("/api/v1/funding/summary/by-group")
    assert resp.status_code == 200
    assert isinstance(resp.json(), list)


def test_funding_group_breakdown_invalid_fund_col():
    c = client()
    resp = c.get(
        "/api/v1/funding/summary/by-group",
        params={"fund_col": "not_a_column"},
    )
    assert resp.status_code == 400
