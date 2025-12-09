"""
Integration tests for subscription management.

This package tests the full subscription workflow including:
- Catch-up from beginning with historical events
- Live event processing
- Catch-up to live transition (gap-free event delivery)
- Checkpoint persistence and recovery
- Multiple concurrent subscribers
- Error handling and recovery
"""
