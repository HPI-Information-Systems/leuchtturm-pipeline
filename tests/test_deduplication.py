"""Test deduplication module."""

from src.deduplication import EmailDeduplication
from config.config import Config


def test_dummy_email_deduplication():
    """Bc of missing spark context, only test correct init of task."""
    conf = Config(['-c', 'config/testconfig.ini'])
    dedup = EmailDeduplication(conf)
    assert dedup is not None
