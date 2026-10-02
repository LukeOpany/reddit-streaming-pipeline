from datetime import datetime, timezone
import json
import pytest
from reliability.processor import parse_event

NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)

@pytest.mark.parametrize('raw', [None, 'bad', '[]', '{}', json.dumps(dict(id='a',subreddit='s',text='x',created_at='2026-01-01'))])
def test_rejects_invalid_contract(raw):
    with pytest.raises(ValueError): parse_event(raw, NOW)

def test_normalizes_timezone():
    _, stamp = parse_event(json.dumps(dict(id='a',subreddit='s',text='x',created_at='2026-01-01T03:00:00+03:00')), NOW)
    assert stamp == NOW
