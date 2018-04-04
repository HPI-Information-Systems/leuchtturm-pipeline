"""Test deduplication module."""

from src.deduplication import EmailDeduplication


def test_dummy_email_deduplication():
    """Bc of missing spark context, only test correct init of task."""
    dedup = EmailDeduplication()
    assert dedup is not None
