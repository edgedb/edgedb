#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2020-present MagicStack Inc. and the EdgeDB authors.
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

# If the hashes break, it is fine to change them, I think?
CREATE MIGRATION m1xpafeaeinvq562zlqkqgcjgdpqds45jr6eybmxm5kzmpzadvvamq
ONTO m1nnh3uhlwn5vfe7dfhyyxxjafsxniljyuzov6avzqeyddw2qpkw7q {
    SET message := "test";
    CREATE TYPE default::Test1;
};

CREATE TYPE default::Test2;
