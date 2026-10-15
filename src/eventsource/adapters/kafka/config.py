"""Configuration for the Kafka event bus."""

from __future__ import annotations

import logging
import socket
import ssl
import uuid
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger("eventsource.bus.kafka")

_SANITIZED = (
    "bootstrap_servers topic_prefix consumer_group consumer_name security_protocol "
    "sasl_mechanism sasl_username ssl_cafile ssl_certfile ssl_check_hostname "
    "enable_dlq enable_tracing enable_metrics"
)


@dataclass
class KafkaEventBusConfig:
    """Configuration for Kafka event bus (connection, producer/consumer, retry, DLQ, security)."""

    bootstrap_servers: str = "localhost:9092"
    topic_prefix: str = "events"
    consumer_group: str = "default"
    consumer_name: str | None = None
    acks: str = "all"
    compression_type: str | None = "gzip"
    producer_max_batch_bytes: int = 16384
    linger_ms: int = 5
    auto_offset_reset: str = "earliest"
    session_timeout_ms: int = 30000
    heartbeat_interval_ms: int = 10000
    max_poll_interval_ms: int = 300000
    fetch_max_bytes: int = 52428800  # aiokafka default (50 MiB)
    max_partition_fetch_bytes: int = 1048576  # aiokafka default (1 MiB)
    max_retries: int = 3
    retry_base_delay: float = 1.0
    retry_max_delay: float = 60.0
    retry_jitter: float = 0.1
    enable_dlq: bool = True
    dlq_topic_suffix: str = ".dlq"
    dlq_consumer_group: str | None = None
    dlq_max_replay_attempts: int = 3
    security_protocol: str = "PLAINTEXT"
    sasl_mechanism: str | None = None
    sasl_username: str | None = None
    sasl_password: str | None = None
    ssl_cafile: str | None = None
    ssl_certfile: str | None = None
    ssl_keyfile: str | None = None
    ssl_check_hostname: bool = True
    enable_tracing: bool = True
    enable_metrics: bool = True
    shutdown_timeout: float = 30.0

    def __post_init__(self) -> None:
        if self.consumer_name is None:
            self.consumer_name = f"{socket.gethostname()}-{uuid.uuid4().hex[:8]}"
        self._validate_security_config()

    def _validate_security_config(self) -> None:
        protos = {"PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"}
        if (sp := self.security_protocol) not in protos:
            raise ValueError(f"Invalid security_protocol: {sp}. Must be one of: {protos}")
        if sp.startswith("SASL_"):
            mechs = {"PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"}
            if not (sm := self.sasl_mechanism):
                raise ValueError(f"sasl_mechanism required for {sp}")
            if sm not in mechs:
                raise ValueError(f"Invalid sasl_mechanism: {sm}. Must be one of: {mechs}")
            if not self.sasl_username or not self.sasl_password:
                raise ValueError("sasl_username and sasl_password required for SASL authentication")
        if sp in ("SSL", "SASL_SSL"):
            if self.ssl_certfile and not self.ssl_keyfile:
                raise ValueError("ssl_keyfile required when ssl_certfile is provided (mTLS)")
            if self.ssl_keyfile and not self.ssl_certfile:
                raise ValueError("ssl_certfile required when ssl_keyfile is provided (mTLS)")
        if sp == "PLAINTEXT":
            logger.warning("Using PLAINTEXT security protocol - not recommended for production")
        if sp == "SASL_PLAINTEXT":
            logger.warning("Using SASL without SSL - credentials sent in plain text")
        if "SSL" in sp and not self.ssl_check_hostname:
            logger.warning("SSL hostname verification disabled - vulnerable to MITM attacks")

    @property
    def topic_name(self) -> str:
        return f"{self.topic_prefix}.stream"

    @property
    def dlq_topic_name(self) -> str:
        return f"{self.topic_name}{self.dlq_topic_suffix}"

    def get_producer_config(self) -> dict[str, Any]:
        bs, c = self.bootstrap_servers, self.compression_type
        cfg: dict[str, Any] = {"bootstrap_servers": bs, "acks": self.acks, "compression_type": c}
        cfg.update(max_batch_size=self.producer_max_batch_bytes, linger_ms=self.linger_ms)
        self._add_security_config(cfg)
        return cfg

    def get_consumer_config(self, **overrides: Any) -> dict[str, Any]:
        bs, grp, cid = self.bootstrap_servers, self.consumer_group, self.consumer_name
        cfg: dict[str, Any] = {"bootstrap_servers": bs, "group_id": grp, "client_id": cid}
        keys = "auto_offset_reset session_timeout_ms heartbeat_interval_ms max_poll_interval_ms"
        for k in (*keys.split(), "fetch_max_bytes", "max_partition_fetch_bytes"):
            cfg[k] = getattr(self, k)
        cfg["enable_auto_commit"] = False
        self._add_security_config(cfg)
        cfg.update(overrides)
        return cfg

    def get_security_config(self) -> dict[str, Any]:
        config: dict[str, Any] = {}
        self._add_security_config(config)
        return config

    def _add_security_config(self, config: dict[str, Any]) -> None:
        m, u, p = self.sasl_mechanism, self.sasl_username, self.sasl_password
        ca, cert, key = self.ssl_cafile, self.ssl_certfile, self.ssl_keyfile
        opt = {"sasl_mechanism": m, "sasl_plain_username": u, "sasl_plain_password": p}
        opt.update(ssl_cafile=ca, ssl_certfile=cert, ssl_keyfile=key)
        config["security_protocol"] = self.security_protocol
        config.update({k: v for k, v in opt.items() if v})
        if "SSL" in self.security_protocol:
            config["ssl_check_hostname"] = self.ssl_check_hostname

    def create_ssl_context(self) -> ssl.SSLContext | None:
        if "SSL" not in self.security_protocol:
            return None
        context = ssl.create_default_context()
        if self.ssl_cafile:
            context.load_verify_locations(self.ssl_cafile)
        if self.ssl_certfile and self.ssl_keyfile:
            context.load_cert_chain(certfile=self.ssl_certfile, keyfile=self.ssl_keyfile)
        context.check_hostname = bool(self.ssl_check_hostname)
        context.verify_mode = ssl.CERT_REQUIRED if self.ssl_check_hostname else ssl.CERT_NONE
        return context

    def get_sanitized_config(self) -> dict[str, Any]:
        res = {k: getattr(self, k) for k in _SANITIZED.split()}
        res["sasl_password"] = "***" if self.sasl_password else None
        res["ssl_keyfile"] = "***" if self.ssl_keyfile else None
        return res
