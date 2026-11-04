from unittest.mock import AsyncMock, patch

import pytest


@pytest.fixture(autouse=True)
def no_field_metadata(request):
    """Create/update resolve picklists against the entity's field metadata,
    which is one more Kylas call. Tests that mock only the write get none, so
    they keep testing what they were written for; tests of the resolution
    itself (marked `fields`) supply their own."""
    from app import references

    references.forget_fields()
    if "fields" in request.keywords:
        yield None
        return
    with patch("app.references._fields", new=AsyncMock(return_value=[])) as fields:
        yield fields
    references.forget_fields()


def pytest_configure(config):
    config.addinivalue_line("markers", "fields: the test supplies entity field metadata itself")
