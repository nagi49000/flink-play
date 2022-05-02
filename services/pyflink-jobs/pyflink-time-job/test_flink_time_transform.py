from pyflink.testing import (
    source_sink_utils,
    test_case_utils
)
from flink_time_transform import (
    iso_to_unix_secs,
    run_flink_time_transform
)

def test_iso_to_unix_secs():
    s = '{"time": "2022-02-05T15:20:09.429963Z"}'
    r = iso_to_unix_secs(s)
    assert r == '{"usecs": 1644074409429963}'


class Testing(test_case_utils.PyFlinkStreamingTestCase):
    # this harness allows running of the flink job in a unit test harness. Need to worry about mocking kafka source and sink
    def test_run_flink_time_transform(self):
        pass
