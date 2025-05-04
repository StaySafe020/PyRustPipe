import pytest
import pyrustpipe
def test_is_valid_email():
    assert pyrustpipe.is_valid_email("test@example.com")
    assert not pyrustpipe.is_valid_email("invalid.email")
def test_normalize_url():
    assert pyrustpipe.normalize_url("HTTPS://Example.COM/path") == "https://example.com/path"