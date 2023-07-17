#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2008-present MagicStack Inc. and the EdgeDB authors.
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

"""Declarations of supported range functions"""

COLUMNS = {
    'json_array_elements': ['value'],
    'json_array_elements_text': ['value'],
    'json_each': ['key', 'value'],
    'json_each_text': ['key', 'value'],
    'jsonb_array_elements': ['value'],
    'jsonb_array_elements_text': ['value'],
    'jsonb_each': ['key', 'value'],
    'jsonb_each_text': ['key', 'value'],
    'pg_options_to_table': ['option_name', 'option_value']
}

# retrieved with
r'''
WITH
    procedures AS (
        SELECT *
        FROM pg_proc
        WHERE proname NOT ILIKE 'pg\_%'
          AND proname NOT ILIKE 'ts\_%'
          AND proname NOT ILIKE '\_%'
          AND proname != 'unnest'
          AND proname != 'aclexplode'
    ),
    pro_args AS (
        SELECT proname,
            UNNEST(proargnames) AS argname,
            UNNEST(proargmodes) AS argmode,
            GENERATE_SERIES(0, 10, 1) AS argn
        FROM procedures
    ),
    pro_outputs AS (
        SELECT *
        FROM pro_args
        WHERE argmode = 'o'
        ORDER BY proname, argn
    )
SELECT proname, ARRAY_AGG(argname)
FROM pro_outputs
GROUP BY proname;
'''
