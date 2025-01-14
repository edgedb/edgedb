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

import os
import sys

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

current_branches = registry.new_labeled_gauge(
    'branches_current',
    'Current number of branches.',
    labels=('tenant',),
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

client_connection_duration = registry.new_labeled_histogram(
    'client_connection_duration',
    'Time a client connection is open.',
    unit=prom.Unit.SECONDS,
    labels=('tenant', 'interface'),
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

graphql_query_compilations = registry.new_labeled_counter(
    'graphql_query_compilations_total',
    'Number of compiled/cached GraphQL queries.',
    labels=('tenant', 'path')
)

query_compilation_duration = registry.new_labeled_histogram(
    'query_compilation_duration',
    'Time it takes to compile a query or script.',
    unit=prom.Unit.SECONDS,
    labels=('tenant', 'interface'),
)

sql_queries = registry.new_labeled_counter(
    'sql_queries_total',
    'Number of SQL queries.',
    labels=('tenant',)
)

sql_compilations = registry.new_labeled_counter(
    'sql_compilations_total',
    'Number of SQL compilations.',
    labels=('tenant',)
)

queries_per_connection = registry.new_labeled_histogram(
    'queries_per_connection',
    'Number of queries per connection.',
    labels=('tenant', 'interface'),
)

query_size = registry.new_labeled_histogram(
    'query_size',
    'The size of a query.',
    unit=prom.Unit.BYTES,
    labels=('tenant', 'interface'),
)

background_errors = registry.new_labeled_counter(
    'background_errors_total',
    'Number of unhandled errors in background server routines.',
    labels=('tenant', 'source')
)

transaction_serialization_errors = registry.new_labeled_counter(
    'transaction_serialization_errors_total',
    'Number of transaction serialization errors.',
    labels=('tenant',)
)

connection_errors = registry.new_labeled_counter(
    'connection_errors_total',
    'Number of network connection errors.',
    labels=('tenant',)
)

ha_events_total = registry.new_labeled_counter(
    "ha_events_total",
    "Number of each high-availability watch event.",
    labels=("dsn", "event"),
)

auth_api_calls = registry.new_labeled_counter(
    "auth_api_calls_total",
    "Number of API calls to the Auth extension.",
    labels=("tenant",),
)

auth_ui_renders = registry.new_labeled_counter(
    "auth_ui_renders_total",
    "Number of UI pages rendered by the Auth extension.",
    labels=("tenant",),
)

auth_providers = registry.new_labeled_gauge(
    'auth_providers',
    'Number of Auth providers configured.',
    labels=('tenant', 'branch'),
)

extension_used = registry.new_labeled_gauge(
    'extension_used_branch_count_current',
    'How many branches an extension is used by.',
    labels=('tenant', 'extension'),
)

feature_used = registry.new_labeled_gauge(
    'feature_used_branch_count_current',
    'How many branches a schema feature is used by.',
    labels=('tenant', 'feature'),
)

auth_successful_logins = registry.new_labeled_counter(
    "auth_successful_logins_total",
    "Number of successful logins in the Auth extension.",
    labels=("tenant",),
)

mt_tenants_total = registry.new_gauge(
    'mt_tenants_current',
    'Total number of currently-registered tenants.',
)

mt_config_reloads = registry.new_counter(
    'mt_config_reloads_total',
    'Total number of the main multi-tenant config file reloads.',
)

mt_config_reload_errors = registry.new_counter(
    'mt_config_reload_errors_total',
    'Total number of the main multi-tenant config file reload errors.',
)

mt_tenant_add_total = registry.new_labeled_counter(
    'mt_tenant_add_total',
    'Total number of new tenants the server attempted to add.',
    labels=("tenant",),
)

mt_tenant_add_errors = registry.new_labeled_counter(
    'mt_tenant_add_errors_total',
    'Total number of tenants the server failed to add.',
    labels=("tenant",),
)

mt_tenant_remove_total = registry.new_labeled_counter(
    'mt_tenant_remove_total',
    'Total number of tenants the server attempted to remove.',
    labels=("tenant",),
)

mt_tenant_remove_errors = registry.new_labeled_counter(
    'mt_tenant_remove_errors_total',
    'Total number of tenants the server failed to remove.',
    labels=("tenant",),
)

mt_tenant_reload_total = registry.new_labeled_counter(
    'mt_tenant_reload_total',
    'Total number of tenants the server attempted to reload.',
    labels=("tenant",),
)

mt_tenant_reload_errors = registry.new_labeled_counter(
    'mt_tenant_reload_errors_total',
    'Total number of tenants the server failed to reload.',
    labels=("tenant",),
)

if os.name == 'posix' and (sys.platform == 'linux' or sys.platform == 'darwin'):
    open_fds = registry.new_gauge(
        'open_fds',
        'Number of open file descriptors.',
    )

    max_open_fds = registry.new_gauge(
        'max_open_fds',
        'Maximum number of open file descriptors.',
    )

# Implement a function that monitors the number of open file descriptors
# and updates the metrics accordingly. This will be replaced with a more
# efficient implementation in Rust at a later date.


def monitor_open_fds_linux():
    import time
    while True:
        max_open_fds.set(os.sysconf('SC_OPEN_MAX'))
        # To get the current number of open files, stat /proc/self/fd/
        # and get the size. If zero, count the number of entries in the
        # directory.
        #
        # This is supported in modern Linux kernels.
        # https://github.com/torvalds/linux/commit/f1f1f2569901ec5b9d425f2e91c09a0e320768f3
        try:
            st = os.stat('/proc/self/fd/')
            if st.st_size == 0:
                open_fds.set(len(os.listdir('/proc/self/fd/')))
            else:
                open_fds.set(st.st_size)
        except Exception:
            open_fds.set(-1)

        time.sleep(30)


def monitor_open_fds_macos():
    import time
    while True:
        max_open_fds.set(os.sysconf('SC_OPEN_MAX'))
        # Iterate the contents of /dev/fd to list all entries.
        # We assume that MacOS isn't going to be running a large installation
        # of EdgeDB on a single machine.
        try:
            open_fds.set(len(os.listdir('/dev/fd')))
        except Exception:
            open_fds.set(-1)

        time.sleep(30)


def start_monitoring_open_fds():
    import threading

    # Supported only on Linux and macOS.
    if os.name == 'posix':
        if sys.platform == 'darwin':
            threading.Thread(
                target=monitor_open_fds_macos,
                name='open_fds_monitor',
                daemon=True
            ).start()
        elif sys.platform == 'linux':
            threading.Thread(
                target=monitor_open_fds_linux,
                name='open_fds_monitor',
                daemon=True
            ).start()


start_monitoring_open_fds()
