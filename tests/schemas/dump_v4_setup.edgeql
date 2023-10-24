#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2023-present MagicStack Inc. and the EdgeDB authors.
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


set module default;

for x in range_unpack(range(1, 1000))
union (
    # Large, varied, but deterministic dataset.
    insert L2 {vec := [x % 10, math::ln(x), x / 7 % 13]}
);


CONFIGURE CURRENT DATABASE SET ext::_conf::Config::config_name := 'ready';
CONFIGURE CURRENT DATABASE SET ext::_conf::Config::secret := 'secret';

CONFIGURE CURRENT DATABASE INSERT ext::_conf::Obj {
    name := '1',
    value := 'foo',
};
CONFIGURE CURRENT DATABASE INSERT ext::_conf::Obj {
    name := '2',
    value := 'bar',
};
CONFIGURE CURRENT DATABASE INSERT ext::_conf::SubObj {
    extra := 42,
    name := '3',
    value := 'baz',
};
CONFIGURE CURRENT DATABASE INSERT ext::_conf::SecretObj {
    name := '4',
    value := 'spam',
    secret := '123456',
};
