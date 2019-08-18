#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2017-present MagicStack Inc. and the EdgeDB authors.
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


from __future__ import annotations

import click


__all__ = ()


marker_passed = lambda t: t
marker_errored = lambda t: click.style(t, fg='red', bold=True)
marker_skipped = lambda t: click.style(t, fg='yellow')
marker_failed = lambda t: click.style(t, fg='red', bold=True)
marker_xfailed = lambda t: t
marker_not_implemented = lambda t: t
marker_upassed = lambda t: click.style(t, fg='yellow')

status = lambda t: click.style(t, fg='white', bold=True)
warning = lambda t: click.style(t, fg='yellow')
