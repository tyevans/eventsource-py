"""SSL and URL utilities for RabbitMQ connections.

Provides SSLContext creation and URL sanitization for logging.
"""

from __future__ import annotations

import logging
import re
import ssl
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from eventsource.adapters.rabbitmq.config import RabbitMQEventBusConfig

logger = logging.getLogger("eventsource.adapters.rabbitmq.connection")


def sanitize_url(url: str) -> str:
    """Remove credentials from URL for logging.

    Args:
        url: The RabbitMQ connection URL

    Returns:
        URL with credentials replaced by ***
    """
    return re.sub(r"://[^:]+:[^@]+@", "://***:***@", url)


def create_ssl_context(config: RabbitMQEventBusConfig) -> ssl.SSLContext | None:
    """Create SSL context from configuration.

    Creates an SSL context based on the TLS configuration options. The
    method follows this priority order:
    1. Use ssl_context if explicitly provided
    2. Create context from ca_file/cert_file/key_file if provided
    3. Create default context if URL uses amqps://
    4. Return None for non-TLS connections

    Args:
        config: RabbitMQ event bus configuration.

    Returns:
        SSLContext if TLS is configured or required by URL, None otherwise.

    Raises:
        ssl.SSLError: If certificate files cannot be loaded
        FileNotFoundError: If certificate files don't exist
    """
    # Check if URL is amqps:// (TLS required)
    is_tls_url = config.rabbitmq_url.startswith("amqps://")

    # Determine if TLS is needed
    needs_tls = (
        is_tls_url
        or config.ssl_context is not None
        or config.ca_file is not None
        or config.cert_file is not None
    )

    if not needs_tls:
        return None

    # Use provided context if available (takes precedence)
    if config.ssl_context is not None:
        logger.debug("Using pre-configured SSL context")
        return config.ssl_context

    # Create context from configuration
    ctx = ssl.create_default_context(
        purpose=ssl.Purpose.SERVER_AUTH,
    )

    # Load CA certificate if provided
    if config.ca_file:
        logger.debug(
            "Loading CA certificate",
            extra={"ca_file": config.ca_file},
        )
        ctx.load_verify_locations(cafile=config.ca_file)

    # Load client certificate for mTLS
    if config.cert_file and config.key_file:
        logger.debug(
            "Loading client certificate for mutual TLS",
            extra={
                "cert_file": config.cert_file,
                "key_file": config.key_file,
            },
        )
        ctx.load_cert_chain(
            certfile=config.cert_file,
            keyfile=config.key_file,
        )
    elif config.cert_file or config.key_file:
        # Warn if only one of cert_file/key_file is provided
        logger.warning(
            "Both cert_file and key_file must be provided for mutual TLS. "
            "Client certificate authentication will not be used.",
            extra={
                "cert_file": config.cert_file,
                "key_file": config.key_file,
            },
        )

    # Configure verification
    if not config.verify_ssl:
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        logger.warning(
            "SSL certificate verification disabled - NOT RECOMMENDED for production",
            extra={"verify_ssl": False},
        )

    return ctx
