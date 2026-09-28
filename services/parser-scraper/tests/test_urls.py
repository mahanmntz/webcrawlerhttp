import json
from pathlib import Path

import pytest

from app.urls import canonicalize, in_scope

VECTORS = json.loads(
    (Path(__file__).resolve().parents[3] / "shared/contracts/url_canonicalization.json").read_text()
)


@pytest.mark.parametrize("case", VECTORS["canonicalize"], ids=lambda c: c["input"] or "<empty>")
def test_canonicalize_matches_shared_contract(case):
    assert canonicalize(case["input"]) == case["expected"]


@pytest.mark.parametrize("case", VECTORS["scope"], ids=lambda c: f'{c["host"]}~{c["scope_host"]}')
def test_in_scope_matches_shared_contract(case):
    assert in_scope(case["host"], case["scope_host"]) is case["in_scope"]
