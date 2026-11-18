"""Kafka config repr hides the SASL password (TASK-0480)."""

from __future__ import annotations

from eventsource.adapters.kafka import KafkaEventBusConfig

SECRET = "s3cret-kafka-pw"  # pragma: allowlist secret


def test_repr_hides_sasl_password() -> None:
    config = KafkaEventBusConfig(
        security_protocol="SASL_SSL",
        sasl_mechanism="PLAIN",
        sasl_username="svc",
        sasl_password=SECRET,
    )
    text = repr(config)
    assert SECRET not in text
    assert "sasl_username='svc'" in text
