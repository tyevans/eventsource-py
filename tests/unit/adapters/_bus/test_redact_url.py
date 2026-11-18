"""Blackbox tests for broker URL redaction and sanitize_url (TASK-0480)."""

from __future__ import annotations

import pytest

from eventsource.adapters.rabbitmq.connection_ssl import sanitize_url


class TestSanitizeUrl:
    def test_masks_plain_credentials(self) -> None:
        assert sanitize_url("amqp://user:pass@host:5672/") == "amqp://***:***@host:5672/"

    def test_password_containing_at_is_fully_masked(self) -> None:
        sanitized = sanitize_url("amqp://u:p@ss@host/")
        assert "p@ss" not in sanitized
        assert "ss@host" not in sanitized
        assert sanitized == "amqp://***:***@host/"

    def test_password_with_url_delimiters_is_fully_masked(self) -> None:
        sanitized = sanitize_url("amqp://admin:p@ssw0rd!#$@host:5672/")
        assert "ssw0rd" not in sanitized
        assert sanitized == "amqp://***:***@host:5672/"

    def test_url_without_credentials_is_unchanged(self) -> None:
        assert sanitize_url("amqp://host:5672/") == "amqp://host:5672/"

    def test_vhost_is_preserved(self) -> None:
        assert sanitize_url("amqp://u:pw@h:5672/myvhost") == "amqp://***:***@h:5672/myvhost"


@pytest.mark.parametrize(
    "url",
    [
        "amqp://u:p@ss@host/",
        "amqp://admin:P@ssw0rd!#$%^&*()@rabbitmq.example.com:5672/",
        "redis://:s3cr@t@cache.example.com:6379/0",
    ],
)
def test_redacted_url_never_contains_password_tail(url: str) -> None:
    """Whatever follows the scheme up to the last '@' must not reach the output."""
    sanitized = sanitize_url(url)
    secret = url.split("://", 1)[1].rsplit("@", 1)[0]
    assert secret not in sanitized
    assert "***:***@" in sanitized
