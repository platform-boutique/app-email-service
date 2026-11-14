"""Process metrics for the OpenTelemetry meter: build_info (CODE-06) and Python runtime gauges (OBS-20).

Only the standard library is used (no psutil). Every instrument is an observable callback, so nothing runs between
two exports, and with telemetry off the meter is the OpenTelemetry API no-op. Memory and open-file figures come from
/proc and are simply left out where /proc does not exist. The uptime clock is injected (CODE-07).

Names (labels in brackets):
  build_info{service,version,commit}               always 1
  python_process_resident_memory_bytes             resident set size
  python_process_cpu_seconds_total                 CPU time of this process (user + system)
  python_process_uptime_seconds                    seconds since register_runtime_metrics() ran
  python_process_open_fds                          open file descriptors
  python_threads                                   live threads
  python_gc_collections_total{generation}          garbage collector runs
  python_gc_objects_collected_total{generation}    objects freed by the garbage collector
"""

import gc
import os
import threading
import time
from collections.abc import Callable, Iterable

from opentelemetry.metrics import CallbackOptions, Meter, Observation

_PROC_STATM = "/proc/self/statm"
_PROC_FD = "/proc/self/fd"


def register_build_info(meter: Meter, service: str, version: str, commit: str) -> None:
    """build_info{service,version,commit} = 1, so a dashboard can show which build runs where."""
    attributes = {"service": service, "version": version, "commit": commit}

    def observe(_options: CallbackOptions) -> Iterable[Observation]:
        yield Observation(1, attributes)

    meter.create_observable_gauge(
        "build_info", callbacks=[observe], description="Build information of the running service, always 1"
    )


def _resident_memory_bytes(statm_path: str = _PROC_STATM) -> int | None:
    try:
        with open(statm_path) as f:
            return int(f.read().split()[1]) * os.sysconf("SC_PAGE_SIZE")
    except (OSError, ValueError, IndexError):
        return None


def _open_fds(fd_dir: str = _PROC_FD) -> int | None:
    try:
        return len(os.listdir(fd_dir))
    except OSError:
        return None


def register_runtime_metrics(meter: Meter, monotonic: Callable[[], float] = time.monotonic) -> None:
    """Python runtime gauges and counters (see the module docstring)."""
    started = monotonic()

    def memory(_options: CallbackOptions) -> Iterable[Observation]:
        rss = _resident_memory_bytes()
        if rss is not None:
            yield Observation(rss)

    def cpu(_options: CallbackOptions) -> Iterable[Observation]:
        yield Observation(time.process_time())

    def uptime(_options: CallbackOptions) -> Iterable[Observation]:
        yield Observation(monotonic() - started)

    def fds(_options: CallbackOptions) -> Iterable[Observation]:
        count = _open_fds()
        if count is not None:
            yield Observation(count)

    def threads(_options: CallbackOptions) -> Iterable[Observation]:
        yield Observation(threading.active_count())

    def gc_collections(_options: CallbackOptions) -> Iterable[Observation]:
        for generation, stats in enumerate(gc.get_stats()):
            yield Observation(stats["collections"], {"generation": str(generation)})

    def gc_collected(_options: CallbackOptions) -> Iterable[Observation]:
        for generation, stats in enumerate(gc.get_stats()):
            yield Observation(stats["collected"], {"generation": str(generation)})

    meter.create_observable_gauge(
        "python_process_resident_memory_bytes", callbacks=[memory], unit="By", description="Resident memory size"
    )
    meter.create_observable_counter(
        "python_process_cpu_seconds_total", callbacks=[cpu], unit="s", description="CPU time, user and system"
    )
    meter.create_observable_gauge(
        "python_process_uptime_seconds", callbacks=[uptime], unit="s", description="Process uptime"
    )
    meter.create_observable_gauge("python_process_open_fds", callbacks=[fds], description="Open file descriptors")
    meter.create_observable_gauge("python_threads", callbacks=[threads], description="Live threads")
    meter.create_observable_counter(
        "python_gc_collections_total", callbacks=[gc_collections], description="Garbage collector runs"
    )
    meter.create_observable_counter(
        "python_gc_objects_collected_total",
        callbacks=[gc_collected],
        description="Objects freed by the garbage collector",
    )
