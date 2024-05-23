#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2021-present MagicStack Inc. and the EdgeDB authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


"""An implementation of prometheus metrics protocol.

Key differences from the official "prometheus_client" package:

1. Not thread-safe. We're an async application and don't use threads
   so there's no need for thread-safety.

2. The code is as simple as possible; no complicated polymorphism
   that can slow down metrics collection in runtime.

3. It's more than 4x faster, likely because of (1) and (2).

4. No global state. All metrics are explicitly contained in an
   explicitly created "registry" instance.

5. This code can be potentially cythonized (or mypyc-ified) for extra
   performance.

6. The tests (tests/common/test_prometheus.py) ensure that the output
   is exactly equal to what prometheus_client generates. It's a bug
   otherwise.

See more for details:

* Open Metrics standard:
  https://github.com/OpenObservability/OpenMetrics/blob/main/specification/OpenMetrics.md

* Prometheus documentation:
  https://prometheus.io/docs/practices/naming/

* Prometheus official Python client:
  https://github.com/prometheus/client_python
"""

from __future__ import annotations

import bisect
import enum
import functools
import math
import time
import typing


__all__ = ('Registry', 'Unit', 'calc_buckets')


def calc_buckets(
    start: float, upper_bound: float, /, *, increment_ratio: float = 1.20
) -> tuple[float, ...]:
    """Calculate histogram buckets on a logarithmic scale."""
    # See https://amplitude.com/blog/2014/08/06/optimal-streaming-histograms
    # for more details.
    result: list[float] = []
    while start <= upper_bound:
        result.append(start)
        start *= increment_ratio
    return tuple(result)


class Unit(enum.Enum):

    # https://prometheus.io/docs/practices/naming/#base-units

    SECONDS = 'seconds'
    CELSIUS = 'celsius'
    METERS = 'meters'
    BYTES = 'bytes'
    RATIO = 'ratio'
    VOLTS = 'volts'
    AMPERES = 'amperes'
    JOULES = 'joules'
    GRAMS = 'grams'


class Registry:

    _metrics: list[BaseMetric]
    _metrics_names: set[str]
    _prefix: str | None

    def __init__(self, *, prefix: str | None = None):
        self._metrics = []
        self._metrics_names = set()
        self._prefix = prefix

    def _add_metric(self, metric: BaseMetric) -> None:
        name = metric.get_name()
        if name in self._metrics_names:
            raise ValueError(
                f'a metric with a name {name!r} has already been registered')
        self._metrics.append(metric)
        self._metrics_names.add(name)

    def now(self) -> float:
        return time.time()

    def set_info(self, name: str, desc: str, /, **kwargs: str) -> None:
        self._add_metric(Info(self, name, desc, **kwargs))

    def new_counter(
        self,
        name: str,
        desc: str,
        /,
        *,
        unit: Unit | None = None,
    ) -> Counter:
        counter = Counter(self, name, desc, unit)
        self._add_metric(counter)
        return counter

    def new_labeled_counter(
        self,
        name: str,
        desc: str,
        /,
        *,
        labels: tuple[str, ...],
        unit: Unit | None = None,
    ) -> LabeledCounter:
        counter = LabeledCounter(self, name, desc, unit, labels=labels)
        self._add_metric(counter)
        return counter

    def new_gauge(
        self,
        name: str,
        desc: str,
        /,
        *,
        unit: Unit | None = None,
    ) -> Gauge:
        gauge = Gauge(self, name, desc, unit)
        self._add_metric(gauge)
        return gauge

    def new_labeled_gauge(
        self,
        name: str,
        desc: str,
        /,
        *,
        unit: Unit | None = None,
        labels: tuple[str, ...],
    ) -> LabeledGauge:
        gauge = LabeledGauge(self, name, desc, unit, labels=labels)
        self._add_metric(gauge)
        return gauge

    def new_histogram(
        self,
        name: str,
        desc: str,
        /,
        *,
        unit: Unit | None = None,
        buckets: list[float] | None = None,
    ) -> Histogram:
        hist = Histogram(self, name, desc, unit, buckets=buckets)
        self._add_metric(hist)
        return hist

    def new_labeled_histogram(
        self,
        name: str,
        desc: str,
        /,
        *,
        unit: Unit | None = None,
        buckets: list[float] | None = None,
        labels: tuple[str, ...],
    ) -> LabeledHistogram:
        hist = LabeledHistogram(
            self, name, desc, unit, buckets=buckets, labels=labels
        )
        self._add_metric(hist)
        return hist

    def generate(self, **label_filters: str) -> str:
        buffer: list[str] = []
        for metric in self._metrics:
            metric._generate(buffer, **label_filters)
        buffer.append('')
        return '\n'.join(buffer)


class BaseMetric:

    _type: str

    _name: str
    _desc: str
    _unit: Unit | None
    _created: float

    _registry: Registry

    PROHIBITED_SUFFIXES = (
        '_count', '_created', '_total', '_sum', '_bucket',
        '_gcount', '_gsum', '_info',
    )

    PROHIBITED_PREFIXES = (
        '_', 'python_', 'prometheus_',
    )

    PROHIBITED_LABELS = (
        'quantile', 'le'
    )

    def __init__(
        self,
        registry: Registry,
        name: str,
        desc: str,
        unit: Unit | None = None,
        /,
    ) -> None:
        self._registry = registry
        name = self._augment_metric_name(name)
        self._validate_name(name)
        if unit is not None:
            name += '_' + unit.value
        self._name = name
        self._desc = desc
        self._unit = unit
        self._created = registry.now()

    def _augment_metric_name(self, name: str) -> str:
        if self._registry._prefix is not None:
            name = f'{self._registry._prefix}_{name}'
        return name

    def get_name(self) -> str:
        return self._name

    def _validate_name(self, name: str) -> None:
        if (name.startswith(self.PROHIBITED_PREFIXES) or
                name.endswith(self.PROHIBITED_SUFFIXES)):
            raise ValueError(f'invalid metrics name: {name!r}')

    def _validate_label_names(self, labels: tuple[str, ...]) -> None:
        for label in labels:
            if label.startswith('_') or label in self.PROHIBITED_LABELS:
                raise ValueError(f'invalid label name: {label!r}')

    def _validate_label_values(
        self, labels: tuple[str, ...], values: tuple[str, ...]
    ) -> None:
        if len(values) != len(labels):
            raise ValueError(
                f'missing values for labels: {labels[len(values):]!r}')
        for name, val in zip(labels, values):
            if not val:
                raise ValueError(f'empty value for label {name!r}')

    def _make_label_filter(
        self,
        labels: tuple[str, ...],
        label_filters: dict[str, str],
    ) -> typing.Callable[[tuple[str, ...]], bool]:
        if not label_filters:
            return lambda _: True

        try:
            label_by_idx = [
                (labels.index(label), label_val)
                for label, label_val in label_filters.items()
            ]
        except ValueError:
            return lambda _: False

        def label_filter(label_values: tuple[str, ...]) -> bool:
            for idx, label_val in label_by_idx:
                if label_values[idx] != label_val:
                    return False
            return True

        return label_filter

    def _generate(self, buffer: list[str], **label_filters: str) -> None:
        raise NotImplementedError


class Info(BaseMetric):

    _type = 'info'

    _name: str
    _desc: str
    _registry: Registry
    _labels: dict[str, str]

    def __init__(self, *args: typing.Any, **labels: str) -> None:
        super().__init__(*args)
        self._validate_label_names(tuple(labels.keys()))
        self._labels = labels

    def _generate(self, buffer: list[str], **label_filters: str) -> None:
        if label_filters:
            return

        desc = _format_desc(self._desc)

        buffer.append(f'# HELP {self._name}_info {desc}')
        buffer.append(f'# TYPE {self._name}_info gauge')

        fmt_label = ','.join(
            f'{label}="{_format_label_val(value)}"'
            for label, value in self._labels.items()
        )
        buffer.append(f'{self._name}_info{{{fmt_label}}} 1.0')


class BaseCounter(BaseMetric):

    _type = 'counter'

    _suffix = '_total'
    _render_created = True

    _value: float

    def __init__(self, *args: typing.Any) -> None:
        super().__init__(*args)
        self._value = 0

    def inc(self, value: float = 1.0) -> None:
        if value < 0:
            raise ValueError(
                'counter cannot be incremented with a negative value')
        self._value += value

    def _generate(self, buffer: list[str], **label_filters: str) -> None:
        if label_filters:
            return

        desc = _format_desc(self._desc)

        buffer.append(f'# HELP {self._name}{self._suffix} {desc}')
        buffer.append(f'# TYPE {self._name}{self._suffix} {self._type}')
        buffer.append(f'{self._name}{self._suffix} {float(self._value)}')

        if self._render_created:
            buffer.append(f'# HELP {self._name}_created {desc}')
            buffer.append(f'# TYPE {self._name}_created gauge')
            buffer.append(f'{self._name}_created {float(self._created)}')


class BaseLabeledCounter(BaseMetric):

    _type = 'counter'

    _suffix = '_total'
    _render_created = True

    _labels: tuple[str, ...]
    _metric_values: dict[tuple[str, ...], float]
    _metric_created: dict[tuple[str, ...], float]

    def __init__(self, *args: typing.Any, labels: tuple[str, ...]) -> None:
        super().__init__(*args)
        self._validate_label_names(labels)
        self._labels = labels
        self._metric_values = {}
        self._metric_created = {}

    def inc(self, value: float = 1.0, *labels: str) -> None:
        self._validate_label_values(self._labels, labels)
        if value < 0:
            raise ValueError(
                'counter cannot be incremented with a negative value')
        try:
            self._metric_values[labels] += value
        except KeyError:
            self._metric_values[labels] = value
            self._metric_created[labels] = self._registry.now()

    def _generate(self, buffer: list[str], **label_filters: str) -> None:
        desc = _format_desc(self._desc)

        buffer.append(f'# HELP {self._name}{self._suffix} {desc}')
        buffer.append(f'# TYPE {self._name}{self._suffix} {self._type}')

        filter_func = self._make_label_filter(self._labels, label_filters)
        for labels, value in self._metric_values.items():
            if not filter_func(labels):
                continue
            fmt_label = ','.join(
                f'{label}="{_format_label_val(label_val)}"'
                for label, label_val in zip(self._labels, labels)
            )
            buffer.append(
                f'{self._name}{self._suffix}{{{fmt_label}}} {float(value)}'
            )

        if self._render_created and self._metric_values:
            buffer.append(f'# HELP {self._name}_created {desc}')
            buffer.append(f'# TYPE {self._name}_created gauge')

            for labels, value in self._metric_created.items():
                if not filter_func(labels):
                    continue
                fmt_label = ','.join(
                    f'{label}="{_format_label_val(label_val)}"'
                    for label, label_val in zip(self._labels, labels)
                )
                buffer.append(
                    f'{self._name}_created{{{fmt_label}}} {float(value)}'
                )


class _TotalMixin(BaseMetric):

    def _augment_metric_name(self, name: str) -> str:
        name = super()._augment_metric_name(name)
        if not name.endswith('_total'):
            raise TypeError('counter metric name require the "_total" suffix')
        name = name[:-len('_total')]
        return name


class Counter(_TotalMixin, BaseCounter):
    pass


class LabeledCounter(_TotalMixin, BaseLabeledCounter):
    pass


class Gauge(BaseCounter):

    _type = 'gauge'

    _render_created = False
    _suffix = ''

    def inc(self, value: float = 1.0) -> None:
        self._value += value

    def dec(self, value: float = 1.0) -> None:
        self._value -= value

    def set(self, value: float) -> None:
        self._value = value


class LabeledGauge(BaseLabeledCounter):

    _type = 'gauge'

    _render_created = False
    _suffix = ''

    def inc(self, value: float = 1.0, *labels: str) -> None:
        self._validate_label_values(self._labels, labels)
        try:
            self._metric_values[labels] += value
        except KeyError:
            self._metric_values[labels] = value
            self._metric_created[labels] = self._registry.now()

    def dec(self, value: float = 1.0, *labels: str) -> None:
        self.inc(-value, *labels)

    def set(self, value: float = 1.0, *labels: str) -> None:
        self._validate_label_values(self._labels, labels)
        self._metric_values[labels] = value
        try:
            self._metric_created[labels]
        except KeyError:
            self._metric_created[labels] = self._registry.now()


class BaseHistogram(BaseMetric):

    _type = 'histogram'

    _buckets: list[float]

    # Default buckets that many standard prometheus client libraries use.
    DEFAULT_BUCKETS = [
        0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75,
        1.0, 2.5, 5.0, 7.5, 10.0,
    ]

    def __init__(
        self, *args: typing.Any, buckets: list[float] | None = None
    ) -> None:
        if buckets is None:
            buckets = self.DEFAULT_BUCKETS
        else:
            buckets = list(buckets)  # copy, just in case

        if buckets != sorted(buckets):
            raise ValueError('*buckets* must be sorted')
        if len(buckets) < 2:
            raise ValueError('*buckets* must have at least 2 numbers')
        if not math.isinf(buckets[-1]):
            buckets += [float('+inf')]

        super().__init__(*args)

        self._buckets = buckets


class Histogram(BaseHistogram):

    _values: list[float]
    _sum: float

    def __init__(
        self, *args: typing.Any, buckets: list[float] | None = None
    ) -> None:
        super().__init__(*args, buckets=buckets)
        self._sum = 0.0
        self._values = [0.0] * len(self._buckets)

    def observe(self, value: float) -> None:
        idx = bisect.bisect_left(self._buckets, value)
        self._values[idx] += 1.0
        self._sum += value

    def _generate(self, buffer: list[str], **label_filters: str) -> None:
        if label_filters:
            return

        desc = _format_desc(self._desc)

        buffer.append(f'# HELP {self._name} {desc}')
        buffer.append(f'# TYPE {self._name} histogram')

        accum = 0.0
        for buck, val in zip(self._buckets, self._values):
            accum += val

            if math.isinf(buck):
                if buck > 0:
                    buckf = '+Inf'
                else:
                    buckf = '-Inf'
            else:
                buckf = str(buck)

            buffer.append(f'{self._name}_bucket{{le="{buckf}"}} {accum}')

        buffer.append(f'{self._name}_count {accum}')
        buffer.append(f'{self._name}_sum {self._sum}')

        buffer.append(f'# HELP {self._name}_created {desc}')
        buffer.append(f'# TYPE {self._name}_created gauge')
        buffer.append(f'{self._name}_created {float(self._created)}')


class LabeledHistogram(BaseHistogram):

    _labels: tuple[str, ...]
    _metric_values: dict[tuple[str, ...], list[float | list[float]]]
    _metric_created: dict[tuple[str, ...], float]

    def __init__(
        self,
        *args: typing.Any,
        buckets: list[float] | None = None,
        labels: tuple[str, ...],
    ) -> None:
        super().__init__(*args, buckets=buckets)
        self._labels = labels
        self._metric_values = {}
        self._metric_created = {}

    def observe(self, value: float, *labels: str) -> None:
        self._validate_label_values(self._labels, labels)

        try:
            metric = self._metric_values[labels]
        except KeyError:
            metric = [0.0, [0.0] * len(self._buckets)]
            self._metric_values[labels] = metric
            self._metric_created[labels] = self._registry.now()

        idx = bisect.bisect_left(self._buckets, value)
        metric[1][idx] += 1.0  # type: ignore
        metric[0] += value  # type: ignore

    def _generate(self, buffer: list[str], **label_filters: str) -> None:
        desc = _format_desc(self._desc)

        buffer.append(f'# HELP {self._name} {desc}')
        buffer.append(f'# TYPE {self._name} histogram')

        filter_func = self._make_label_filter(self._labels, label_filters)
        for labels, values in self._metric_values.items():
            if not filter_func(labels):
                continue
            fmt_label = ','.join(
                f'{label}="{_format_label_val(label_val)}"'
                for label, label_val in zip(self._labels, labels)
            )
            accum = 0.0
            for buck, val in zip(self._buckets, values[1]):  # type: ignore
                accum += val

                if math.isinf(buck):
                    if buck > 0:
                        buckf = '+Inf'
                    else:
                        buckf = '-Inf'
                else:
                    buckf = str(buck)

                buffer.append(
                    f'{self._name}_bucket{{le="{buckf}",{fmt_label}}} {accum}'
                )

            buffer.append(f'{self._name}_count{{{fmt_label}}} {accum}')
            buffer.append(f'{self._name}_sum{{{fmt_label}}} {values[0]}')

        if self._metric_values:
            buffer.append(f'# HELP {self._name}_created {desc}')
            buffer.append(f'# TYPE {self._name}_created gauge')
            for labels, value in self._metric_created.items():
                if not filter_func(labels):
                    continue
                fmt_label = ','.join(
                    f'{label}="{_format_label_val(label_val)}"'
                    for label, label_val in zip(self._labels, labels)
                )
                buffer.append(
                    f'{self._name}_created{{{fmt_label}}} {float(value)}'
                )


@functools.lru_cache(maxsize=1024)
def _format_desc(desc: str) -> str:
    return desc.replace('\\', r'\\').replace('\n', r'\n')


@functools.lru_cache(maxsize=1024)
def _format_label_val(desc: str) -> str:
    return (
        desc.replace('\\', r'\\').replace('\n', r'\n').replace('"', r'\"')
    )
