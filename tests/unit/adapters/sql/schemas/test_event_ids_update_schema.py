"""Operator script updates/006 creates the same event_ids side table as the template.

The partitioned events schema creates event_ids (events_partitioned.sql). Existing
partitioned databases get it from updates/006_add_event_ids_side_table.sql. The two
definitions must agree, or the adapter's probe and inserts disagree with the table.
"""

from eventsource.adapters.sql.schemas import _PACKAGE_DIR, _TEMPLATES_DIR

_EVENT_IDS_DDL = "CREATE TABLE IF NOT EXISTS event_ids (\n    event_id UUID PRIMARY KEY\n);"


def test_partitioned_template_creates_event_ids() -> None:
    assert _EVENT_IDS_DDL in (_TEMPLATES_DIR / "events_partitioned.sql").read_text()


def test_update_script_creates_the_same_side_table() -> None:
    script = (_PACKAGE_DIR / "updates" / "006_add_event_ids_side_table.sql").read_text()
    assert _EVENT_IDS_DDL in script


def test_update_script_backfills_from_existing_events() -> None:
    script = (_PACKAGE_DIR / "updates" / "006_add_event_ids_side_table.sql").read_text()
    assert "INSERT INTO event_ids (event_id)" in script
    assert "SELECT DISTINCT event_id FROM events" in script
    assert "ON CONFLICT DO NOTHING" in script
