#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2019-present MagicStack Inc. and the EdgeDB authors.
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


CREATE FUNCTION test::vol_immutable() -> float64 {
    SET volatility := 'IMMUTABLE';
    USING SQL $$
        SELECT random();
    $$;
};

CREATE FUNCTION test::vol_stable() -> float64 {
    SET volatility := 'STABLE';
    USING SQL $$
        SELECT random();
    $$;
};

CREATE FUNCTION test::vol_volatile() -> float64 {
    SET volatility := 'VOLATILE';
    USING SQL $$
        SELECT random();
    $$;
};

CREATE FUNCTION test::err_immutable() -> float64 {
    SET volatility := 'IMMUTABLE';
    USING SQL $$
        SELECT random()/0;
    $$;
};

CREATE FUNCTION test::err_stable() -> float64 {
    SET volatility := 'STABLE';
    USING SQL $$
        SELECT random()/0;
    $$;
};

CREATE FUNCTION test::err_volatile() -> float64 {
    SET volatility := 'VOLATILE';
    USING SQL $$
        SELECT random()/0;
    $$;
};


INSERT test::Obj { n := 1 };
INSERT test::Obj { n := 2 };
INSERT test::Obj { n := 3 };
