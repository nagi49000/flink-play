from flink_time_transform import (
    iso_to_unix_secs,
    run_flink_time_transform
)
#from pyflink.testing import (
#    source_sink_utils,
#    test_case_utils
#)


def test_iso_to_unix_secs():
    s = '{"time": "2022-02-05T15:20:09.429963Z"}'
    r = iso_to_unix_secs(s)
    assert r == '{"usecs": 1644074409429963}'


def test_run_flink_time_transform():
    pass
