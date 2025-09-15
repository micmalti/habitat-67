import pytest
from datetime import datetime, date, timedelta
import pytz
from check_in.utils import datetime_from_midnight

@pytest.mark.parametrize("dt_input,timezone,day_offset,expected", [
    # Test with None input (current date at midnight UTC)
    (None, 'UTC', None, datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)),
    # Test with datetime object
    # (datetime(2023, 12, 25, 14, 30, 45), 'UTC', None, datetime(2023, 12, 25, 0, 0, 0)),
    # Test with timestamp
    # (1703543400, 'UTC', None, datetime(2023, 12, 25, 0, 0, 0)),
    # Test with day_offset
    # (datetime(2023, 12, 25, 14, 30, 45), 'UTC', 1, datetime(2023, 12, 24, 0, 0, 0)),
    # Test with different timezone
    # (datetime(2023, 12, 25, 14, 30, 45), 'US/Eastern', None, datetime(2023, 12, 25, 0, 0, 0)),
    # Test with missing day_offset
    pytest.param(
        datetime(2023, 12, 25, 14, 30, 45),
        'UTC',
        None,
        datetime(2023, 12, 25, 14, 30, 45),
        marks=pytest.mark.xfail
    ),
])

def test_datetime_from_midnight(dt_input, timezone, day_offset, expected):
    result = datetime_from_midnight(dt=dt_input, timezone=timezone, day_offset=day_offset)
    if dt_input is None:
        now = datetime.now()
        expected = now.replace(hour=0, minute=0, second=0, microsecond=0)
        assert result.date() == expected.date()
        assert result.hour == 0
        assert result.minute == 0
        assert result.second == 0
        assert result.microsecond == 0
    else:
        assert result == expected
