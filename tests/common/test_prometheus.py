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

import unittest

from edb.common import prometheus as prom

try:
    import prometheus_client as pmc_root
    import prometheus_client.registry as pmc_reg
except ImportError:
    PMC = None  # type: ignore
else:
    class PMC:  # type: ignore

        class Registry(pmc_reg.CollectorRegistry):
            def __init__(self):
                super().__init__(auto_describe=True)

        def generate(registry):
            return pmc_root.generate_latest(registry).decode()

        class _RequiredRegistryMixin:
            def __init__(self, *args, registry, **kwargs):
                super().__init__(*args, registry=registry, **kwargs)
                self._kwargs['registry'] = registry

            def _metric_init(self):
                super()._metric_init()
                self._created = CREATED_AT

        class Counter(_RequiredRegistryMixin, pmc_root.Counter):
            pass

        class Gauge(_RequiredRegistryMixin, pmc_root.Gauge):
            pass

        class Histogram(_RequiredRegistryMixin, pmc_root.Histogram):
            pass

        class Info(_RequiredRegistryMixin, pmc_root.Info):
            pass


class EP:

    class Registry(prom.Registry):

        def now(self):
            return CREATED_AT


CREATED_AT = -1142.11


@unittest.skipIf(PMC is None, 'prometheus_client package is not installed')
class TestPrometheusClient(unittest.TestCase):

    def test_prometheus_01(self):

        def run_pmc():
            registry = PMC.Registry()

            test_counter = PMC.Counter(
                'test_counter', 'A test counter"',
                registry=registry)
            test_labeled_counter = PMC.Counter(
                'test_labeled_counter', 'A test labeled counter',
                labelnames=['t1', 't2'], registry=registry)
            test_labeled_gauge = PMC.Gauge(
                'test_labeled_gauge', 'test labeled gauge\'"\n',
                labelnames=['g1'], registry=registry)
            test_gauge = PMC.Gauge(
                'test_gauge', 'A test      gauge',
                registry=registry)

            r0 = PMC.generate(registry)

            test_counter.inc()
            test_counter.inc(1.2)

            test_gauge.inc()
            test_gauge.dec()
            test_gauge.set(1.2)

            test_labeled_counter.labels('aaa', 'bbb"').inc()
            test_labeled_counter.labels('aaa', 'zzz').inc(1.3)

            test_labeled_gauge.labels('l1').inc(1.3)

            r1 = PMC.generate(registry)

            test_labeled_counter.labels('aaa', 'ezi').inc(1.4)
            test_gauge.set(111.2)

            test_labeled_gauge.labels('l1').dec(0.1)
            test_labeled_gauge.labels('l2').set(42)

            r2 = PMC.generate(registry)

            return [r0, r1, r2]

        def run_emc():
            r = EP.Registry()

            test_counter = r.new_counter(
                'test_counter_total', 'A test counter"'
            )

            test_labeled_counter = r.new_labeled_counter(
                'test_labeled_counter_total', 'A test labeled counter',
                labels=('t1', 't2')
            )

            test_labeled_gauge = r.new_labeled_gauge(
                'test_labeled_gauge', 'test labeled gauge\'"\n',
                labels=('g1',)
            )

            test_gauge = r.new_gauge(
                'test_gauge', 'A test      gauge',
            )

            r0 = r.generate()

            test_counter.inc()
            test_counter.inc(1.2)

            test_gauge.inc()
            test_gauge.dec()
            test_gauge.set(1.2)

            test_labeled_counter.inc(1.0, 'aaa', 'bbb"')
            test_labeled_counter.inc(1.3, 'aaa', 'zzz')

            test_labeled_gauge.inc(1.3, 'l1')

            r1 = r.generate()

            test_labeled_counter.inc(1.4, 'aaa', 'ezi')
            test_gauge.set(111.2)

            test_labeled_gauge.dec(0.1, 'l1')
            test_labeled_gauge.set(42, 'l2')

            r2 = r.generate()

            return [r0, r1, r2]

        pmc_r = run_pmc()
        emc_r = run_emc()
        self.assertEqual(pmc_r, emc_r)

    def test_prometheus_02(self):

        def run_pmc():
            registry = PMC.Registry()

            test_info = PMC.Info(
                'test', 'A  test info',
                registry=registry)

            test_info.info(dict(blah='blahaaah', spam='ha"\nm'))
            r1 = PMC.generate(registry)
            return [r1]

        def run_emc():
            r = EP.Registry()

            r.set_info(
                'test', 'A  test info',
                blah='blahaaah', spam='ha"\nm'
            )

            r1 = r.generate()

            return [r1]

        pmc_r = run_pmc()
        emc_r = run_emc()
        self.assertEqual(pmc_r, emc_r)

    def test_prometheus_03(self):

        def run_pmc():
            registry = PMC.Registry()

            test_hist = PMC.Histogram(
                'test_hist', 'A  test info',
                registry=registry)

            r0 = PMC.generate(registry)

            test_hist.observe(0.22)
            test_hist.observe(0.44)
            test_hist.observe(0.66)
            test_hist.observe(0.43)
            test_hist.observe(2.0)

            r1 = PMC.generate(registry)

            test_hist.observe(-1)
            test_hist.observe(0.0001)
            test_hist.observe(0.43)

            r2 = PMC.generate(registry)

            return [r0, r1, r2]

        def run_emc():
            r = EP.Registry()

            test_hist = r.new_histogram(
                'test_hist', 'A  test info',
            )

            r0 = r.generate()

            test_hist.observe(0.22)
            test_hist.observe(0.44)
            test_hist.observe(0.66)
            test_hist.observe(0.43)
            test_hist.observe(2.0)

            r1 = r.generate()

            test_hist.observe(-1)
            test_hist.observe(0.0001)
            test_hist.observe(0.43)

            r2 = r.generate()

            return [r0, r1, r2]

        pmc_r = run_pmc()
        emc_r = run_emc()
        self.assertEqual(pmc_r, emc_r)

    def test_prometheus_04(self):

        def run_pmc():
            registry = PMC.Registry()

            test_hist = PMC.Histogram(
                'test_hist_seconds', 'A  test info',
                registry=registry)

            test_hist.observe(0.22)

            r1 = PMC.generate(registry)
            return [r1]

        def run_emc():
            r = EP.Registry()

            test_hist = r.new_histogram(
                'test_hist', 'A  test info',
                unit=prom.Unit.SECONDS
            )

            test_hist.observe(0.22)
            r1 = r.generate()
            return [r1]

        pmc_r = run_pmc()
        emc_r = run_emc()
        self.assertEqual(pmc_r, emc_r)

    def test_prometheus_05(self):
        # basic sanity checks
        bs = prom.calc_buckets(0.05, 10.0, increment_ratio=1.20)
        self.assertEqual(len(bs), 30)
        self.assertEqual(len(set(bs)), len(bs))
        self.assertEqual(bs, tuple(sorted(bs)))

    def test_prometheus_06(self):
        r = EP.Registry(prefix='edgedb')

        test = r.new_counter(
            'test_total', 'A  test info'
        )
        test.inc()
        r1 = r.generate()

        self.assertIn('\nedgedb_test_total ', r1)
        self.assertIn('\nedgedb_test_created ', r1)

        with self.assertRaisesRegex(
                ValueError, "metric with a name 'edgedb_test'"):
            r.new_counter(
                'test_total', 'A  test info'
            )

    def test_prometheus_07(self):

        def run_pmc():
            registry = PMC.Registry()

            test_labeled_counter = PMC.Counter(
                'test_labeled_counter', 'A test labeled counter',
                labelnames=['t1', 't2'], registry=registry)
            test_labeled_gauge = PMC.Gauge(  # NoQA
                'test_labeled_gauge', 'test labeled gauge\'"\n',
                labelnames=['g1'], registry=registry)

            r1 = PMC.generate(registry)

            test_labeled_counter.labels('blah', 'spam').inc()

            r2 = PMC.generate(registry)

            test_labeled_counter.labels('blah', 'ham').inc()

            r3 = PMC.generate(registry)

            return [r1, r2, r3]

        def run_emc():
            r = EP.Registry()

            test_labeled_counter = r.new_labeled_counter(
                'test_labeled_counter_total', 'A test labeled counter',
                labels=('t1', 't2')
            )

            test_labeled_gauge = r.new_labeled_gauge(  # NoQA
                'test_labeled_gauge', 'test labeled gauge\'"\n',
                labels=('g1',)
            )

            r1 = r.generate()

            test_labeled_counter.inc(1.0, 'blah', 'spam')

            r2 = r.generate()

            test_labeled_counter.inc(1.0, 'blah', 'ham')

            r3 = r.generate()

            return [r1, r2, r3]

        pmc_r = run_pmc()
        emc_r = run_emc()
        self.assertEqual(pmc_r, emc_r)

    def test_prometheus_08(self):

        def run_pmc():
            registry = PMC.Registry()

            test_hist = PMC.Histogram(
                'test_hist', 'A  test info',
                labelnames=['tenant'], registry=registry)

            r0 = PMC.generate(registry)

            test_hist.labels('1').observe(0.22)
            test_hist.labels('2').observe(0.44)
            test_hist.labels('1').observe(0.66)
            test_hist.labels('2').observe(0.43)
            test_hist.labels('1').observe(2.0)

            r1 = PMC.generate(registry)

            test_hist.labels('2').observe(-1)
            test_hist.labels('1').observe(0.0001)
            test_hist.labels('2').observe(0.43)

            r2 = PMC.generate(registry)

            return [r0, r1, r2]

        def run_emc():
            r = EP.Registry()

            test_hist = r.new_labeled_histogram(
                'test_hist', 'A  test info', labels=('tenant',)
            )

            r0 = r.generate()

            test_hist.observe(0.22, '1')
            test_hist.observe(0.44, '2')
            test_hist.observe(0.66, '1')
            test_hist.observe(0.43, '2')
            test_hist.observe(2.0, '1')

            r1 = r.generate()

            test_hist.observe(-1, '2')
            test_hist.observe(0.0001, '1')
            test_hist.observe(0.43, '2')

            r2 = r.generate()

            return [r0, r1, r2]

        pmc_r = run_pmc()
        emc_r = run_emc()
        self.assertEqual(pmc_r, emc_r)
