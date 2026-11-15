"""build_info (CODE-06) and the Python runtime metrics (OBS-20), read back through a real SDK with an in-memory reader.

The global meter provider is not touched (it can only be set once per process): each test builds its own provider.
The uptime clock is injected, no sleeping.
"""

import pytest
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader

import runtime_metrics


@pytest.fixture()
def reader_and_meter():
    reader = InMemoryMetricReader()
    provider = MeterProvider(metric_readers=[reader])
    yield reader, provider.get_meter("test")
    provider.shutdown()


def collect(reader):
    """{metric name: [(attributes, value), ...]}"""
    out = {}
    data = reader.get_metrics_data()
    for resource_metrics in data.resource_metrics:
        for scope in resource_metrics.scope_metrics:
            for metric in scope.metrics:
                out[metric.name] = [(dict(p.attributes), p.value) for p in metric.data.data_points]
    return out


def test_build_info_is_one_with_service_version_and_commit(reader_and_meter):
    reader, meter = reader_and_meter
    runtime_metrics.register_build_info(meter, "email-service", "1.2.3", "abc1234")
    assert collect(reader)["build_info"] == [({"service": "email-service", "version": "1.2.3", "commit": "abc1234"}, 1)]


def test_runtime_metrics_are_reported(reader_and_meter):
    reader, meter = reader_and_meter
    runtime_metrics.register_runtime_metrics(meter)
    metrics = collect(reader)
    assert metrics["python_threads"][0][1] >= 1
    assert metrics["python_process_cpu_seconds_total"][0][1] >= 0
    assert metrics["python_process_resident_memory_bytes"][0][1] > 0  # /proc exists on the Linux CI and dev hosts
    assert metrics["python_process_open_fds"][0][1] > 0
    generations = sorted(attrs["generation"] for attrs, _ in metrics["python_gc_collections_total"])
    assert generations == ["0", "1", "2"]
    assert sorted(a["generation"] for a, _ in metrics["python_gc_objects_collected_total"]) == ["0", "1", "2"]


def test_uptime_follows_the_injected_clock(reader_and_meter):
    reader, meter = reader_and_meter
    now = [1000.0]
    runtime_metrics.register_runtime_metrics(meter, monotonic=lambda: now[0])
    assert collect(reader)["python_process_uptime_seconds"][0][1] == 0
    now[0] += 90.5
    assert collect(reader)["python_process_uptime_seconds"][0][1] == 90.5


def test_missing_proc_means_the_gauge_is_left_out_not_an_error(tmp_path):
    assert runtime_metrics._resident_memory_bytes(str(tmp_path / "nope")) is None
    assert runtime_metrics._open_fds(str(tmp_path / "nope")) is None
    bad = tmp_path / "statm"
    bad.write_text("garbage")
    assert runtime_metrics._resident_memory_bytes(str(bad)) is None
    good = tmp_path / "statm_ok"
    good.write_text("100 25 0 0 0 0 0")
    import os

    assert runtime_metrics._resident_memory_bytes(str(good)) == 25 * os.sysconf("SC_PAGE_SIZE")
