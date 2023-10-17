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


from edb.common import prometheus as prom


registry = prom.Registry(prefix='edgedb_server')

compiler_process_spawns = registry.new_counter(
    'compiler_process_spawns_total',
    'Total number of compiler processes spawned.'
)

current_compiler_processes = registry.new_gauge(
    'compiler_processes_current',
    'Current number of active compiler processes.'
)

total_backend_connections = registry.new_labeled_counter(
    'backend_connections_total',
    'Total number of backend connections established.',
    labels=('tenant',),
)

current_backend_connections = registry.new_labeled_gauge(
    'backend_connections_current',
    'Current number of active backend connections.',
    labels=('tenant',),
)

backend_connection_establishment_errors = registry.new_labeled_counter(
    'backend_connection_establishment_errors_total',
    'Number of times the server could not establish a backend connection.',
    labels=('tenant',),
)

backend_connection_establishment_latency = registry.new_labeled_histogram(
    'backend_connection_establishment_latency',
    'Time it takes to establish a backend connection.',
    unit=prom.Unit.SECONDS,
    labels=('tenant',),
)

backend_connection_aborted = registry.new_labeled_counter(
    'backend_connections_aborted_total',
    'Number of aborted backend connections.',
    labels=('tenant', 'pgcode')
)

backend_query_duration = registry.new_labeled_histogram(
    'backend_query_duration',
    'Time it takes to run a query on a backend connection.',
    unit=prom.Unit.SECONDS,
    labels=('tenant',),
)

total_client_connections = registry.new_labeled_counter(
    'client_connections_total',
    'Total number of clients.',
    labels=('tenant',),
)

current_client_connections = registry.new_labeled_gauge(
    'client_connections_current',
    'Current number of active clients.',
    labels=('tenant',),
)

idle_client_connections = registry.new_labeled_counter(
    'client_connections_idle_total',
    'Total number of forcefully closed idle client connections.',
    labels=('tenant',),
)

edgeql_query_compilations = registry.new_labeled_counter(
    'edgeql_query_compilations_total',
    'Number of compiled/cached queries or scripts.',
    labels=('tenant', 'path')
)

edgeql_query_compilation_duration = registry.new_labeled_histogram(
    'edgeql_query_compilation_duration',
    'Time it takes to compile an EdgeQL query or script.',
    unit=prom.Unit.SECONDS,
    labels=('tenant',),
)

background_errors = registry.new_labeled_counter(
    'background_errors_total',
    'Number of unhandled errors in background server routines.',
    labels=('tenant', 'source')
)

ha_events_total = registry.new_labeled_counter(
    "ha_events_total",
    "Number of each high-availability watch event.",
    labels=("dsn", "event"),
)
