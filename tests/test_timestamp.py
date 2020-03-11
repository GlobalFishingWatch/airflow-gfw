import pytest
import pytz

from datetime import datetime
from datetime import timedelta
from airflow_ext.gfw.operators.timestamp import daterange


@pytest.mark.filterwarnings('ignore:Using fallback coder:UserWarning')
@pytest.mark.filterwarnings('ignore:The compiler package is deprecated and removed in Python 3.x.:DeprecationWarning')
class TestTimestampTools():

    def test_timestamp_daterange(self):
        now = datetime.now(tz=pytz.UTC)
        yesterday=now - timedelta(days=1)
        gen=daterange(yesterday, now)
        assert next(gen) == yesterday
        assert next(gen) == now
