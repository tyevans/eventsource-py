"""Broker connection-string helpers shared by the RabbitMQ and Redis adapters.

Covers three concerns that must behave the same for every broker URL:

- ``redact_url``: hide userinfo before a URL reaches a log or a repr.
- ``redacted_repr``: build a dataclass-style repr that redacts URL fields.
- ``warn_if_plaintext_remote``: warn when a non-TLS scheme points at a
  non-loopback host.

URLs are not parsed with ``urllib.parse``. Its ``urlsplit`` ends the authority
at the first ``/``, ``?`` or ``#``. Those characters can appear unencoded in a
password, so ``urlsplit`` would leave part of the password in the output. The
helpers here split on the last ``@`` instead, which fails closed: any text
before the last ``@`` is treated as credentials.
"""

from __future__ import annotations

import ipaddress
import logging
import re
from dataclasses import fields
from typing import Any

_SCHEME_SEP = "://"
_AUTHORITY_END = re.compile(r"[/?#]")


def _scheme_end(url: str) -> int:
    """Return the index just past '://', or 0 when the URL has no scheme."""
    idx = url.find(_SCHEME_SEP)
    return idx + len(_SCHEME_SEP) if idx >= 0 else 0


def redact_url(url: str) -> str:
    """Replace the userinfo of a URL with ``***:***``.

    Everything before the last ``@`` after the scheme is treated as
    credentials. A URL without an ``@`` is returned unchanged.

    Example: ``amqp://u:p@ss@host/`` becomes ``amqp://***:***@host/``.
    """
    start = _scheme_end(url)
    at = url.rfind("@")
    if at < start:
        return url
    return f"{url[:start]}***:***@{url[at + 1 :]}"


def _host_and_user(url: str) -> tuple[str, str]:
    """Return ``(host, username)`` for a URL, using the same split as ``redact_url``."""
    start = _scheme_end(url)
    rest = url[start:]
    user = ""
    at = rest.rfind("@")
    if at >= 0:
        user = rest[:at].split(":", 1)[0]
        rest = rest[at + 1 :]
    authority = _AUTHORITY_END.split(rest, maxsplit=1)[0]
    if authority.startswith("["):
        host = authority[1 : authority.find("]")] if "]" in authority else authority[1:]
    else:
        host = authority.split(":", 1)[0]
    return host, user


def is_loopback_host(host: str) -> bool:
    """Return True for ``localhost``, loopback IP addresses, or an empty host.

    An empty host means the client uses its own default, which is localhost.
    """
    name = host.lower()
    if name in ("", "localhost"):
        return True
    try:
        return ipaddress.ip_address(name).is_loopback
    except ValueError:
        return False


def url_host(url: str) -> str:
    """Return the host part of a URL, without userinfo or port."""
    return _host_and_user(url)[0]


def url_username(url: str) -> str:
    """Return the username part of a URL, or an empty string."""
    return _host_and_user(url)[1]


def warn_if_plaintext_remote(
    logger: logging.Logger, url: str, *, plaintext_schemes: frozenset[str], broker: str
) -> None:
    """Log a WARNING when a non-TLS scheme targets a non-loopback host."""
    if _SCHEME_SEP not in url:
        return
    scheme = url[: url.find(_SCHEME_SEP)].lower()
    if scheme not in plaintext_schemes:
        return
    host, _ = _host_and_user(url)
    if is_loopback_host(host):
        return
    logger.warning(
        f"{broker} URL uses plaintext transport ({scheme}://) to non-loopback host {host!r} "
        "- credentials and messages sent in plain text. Use a TLS scheme for remote brokers.",
        extra={"broker": broker, "scheme": scheme, "host": host},
    )


def redacted_repr(obj: object, url_fields: tuple[str, ...] = ()) -> str:
    """Build a dataclass-style repr, redacting the named URL fields.

    Fields declared with ``repr=False`` are omitted, as ``dataclasses`` does.
    """
    parts: list[str] = []
    for f in fields(obj):  # type: ignore[arg-type]
        if not f.repr:
            continue
        value: Any = getattr(obj, f.name)
        if f.name in url_fields and isinstance(value, str):
            value = redact_url(value)
        parts.append(f"{f.name}={value!r}")
    return f"{type(obj).__qualname__}({', '.join(parts)})"


__all__ = [
    "is_loopback_host",
    "redact_url",
    "redacted_repr",
    "url_host",
    "url_username",
    "warn_if_plaintext_remote",
]
