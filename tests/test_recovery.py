import os
import pytest
from reliability.demo import run

def test_postgres_recovery():
    dsn = os.getenv('TEST_DATABASE_URL')
    if not dsn: pytest.skip('Set TEST_DATABASE_URL to an isolated PostgreSQL database')
    report = run(dsn)
    assert report['accepted_events'] == report['summary_total'] == 2
