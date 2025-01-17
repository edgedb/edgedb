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



abstract type Base {
    required p_str: str;
}

type Gin extending Base {
    index ext::pg_trgm::gin on (.p_str);
}

type Gin2 extending Base {
    p_str_2: str;
    index ext::pg_trgm::gist on ((.p_str, .p_str_2));
}

type Gist extending Base {
    index ext::pg_trgm::gist on (.p_str);
}

type Gist2 extending Base {
    p_str_2: str;
    index ext::pg_trgm::gist on ((.p_str, .p_str_2));
}
