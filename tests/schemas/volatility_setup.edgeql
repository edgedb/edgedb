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


CREATE FUNCTION vol_immutable() -> float64 {
    SET volatility := 'Immutable';
    USING SQL $$
        SELECT random();
    $$;
};

CREATE FUNCTION vol_stable() -> float64 {
    SET volatility := 'Stable';
    USING SQL $$
        SELECT random();
    $$;
};

CREATE FUNCTION vol_volatile() -> float64 {
    SET volatility := 'Volatile';
    USING SQL $$
        SELECT random();
    $$;
};

CREATE FUNCTION err_immutable() -> float64 {
    SET volatility := 'Immutable';
    USING SQL $$
        SELECT random()/0;
    $$;
};

CREATE FUNCTION err_stable() -> float64 {
    SET volatility := 'Stable';
    USING SQL $$
        SELECT random()/0;
    $$;
};

CREATE FUNCTION err_volatile() -> float64 {
    SET volatility := 'Volatile';
    USING SQL $$
        SELECT random()/0;
    $$;
};

CREATE FUNCTION rand_int(top: int64) -> int64 {
    USING (<int64>(random() * top))
};


INSERT Obj { n := 1 };
INSERT Obj { n := 2 };
INSERT Obj { n := 3 };
INSERT Tgt { n := 1 };
INSERT Tgt { n := 2 };
INSERT Tgt { n := 3 };
INSERT Tgt { n := 4 };
