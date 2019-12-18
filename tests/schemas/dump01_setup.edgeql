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


SET MODULE default;

INSERT A {
    p_bool := True,
    p_str := 'Hello',
    p_datetime := <datetime>'2018-05-07T20:01:22.306916+00:00',
    p_local_datetime := <cal::local_datetime>'2018-05-07T20:01:22.306916',
    p_local_date := <cal::local_date>'2018-05-07',
    p_local_time := <cal::local_time>'20:01:22.306916',
    p_duration := <duration>'20 hrs',
    p_int16 := 12345,
    p_int32 := 1234567890,
    p_int64 := 1234567890123,
    p_float32 := 2.5,
    p_float64 := 2.5,
    p_bigint := 123456789123456789123456789n,
    p_decimal := 123456789123456789123456789.123456789123456789123456789n,
    p_json := to_json('[{"a": null, "b": true}, 1, 2.5, "foo"]'),
    p_bytes := b'Hello',
};


INSERT B {
    p_bool := {True, False},
    p_str := {'Hello', 'world'},
    p_datetime := {
        <datetime>'2018-05-07T20:01:22.306916+00:00',
        <datetime>'2019-05-07T20:01:22.306916+00:00',
    },
    p_local_datetime := {
        <cal::local_datetime>'2018-05-07T20:01:22.306916',
        <cal::local_datetime>'2019-05-07T20:01:22.306916',
    },
    p_local_date := {
        <cal::local_date>'2018-05-07',
        <cal::local_date>'2019-05-07',
    },
    p_local_time := {
        <cal::local_time>'20:01:22.306916',
        <cal::local_time>'20:02:22.306916',
    },
    p_duration := {<duration>'20 hrs', <duration>'20 sec'},
    p_int16 := {12345, -42},
    p_int32 := {1234567890, -42},
    p_int64 := {1234567890123, -42},
    p_float32 := {2.5, -42},
    p_float64 := {2.5, -42},
    p_bigint := {
        123456789123456789123456789n,
        -42n,
    },
    p_decimal := {
        123456789123456789123456789.123456789123456789123456789n,
        -42n,
    },
    p_json := {
        to_json('[{"a": null, "b": true}, 1, 2.5, "foo"]'),
        <json>'bar',
        <json>False,
    },
    p_bytes := {b'Hello', b'world'},
};


FOR x IN {{'D', 'E', 'F'} ++ {'00', '01', '02', '03'}}
UNION (
    INSERT C {
        val := x,
    }
);


INSERT D {
    num := 0,
};
INSERT D {
    num := 1,
    single_link := (SELECT C FILTER .val = 'D00'),
};
INSERT D {
    num := 2,
    multi_link := (SELECT C FILTER .val IN {'D01', 'D02'}),
};
INSERT D {
    num := 3,
    single_link := (SELECT C FILTER .val = 'D00'),
    multi_link := (SELECT C FILTER .val IN {'D01', 'D02', 'D03'}),
};


INSERT E {
    num := 4,
};
INSERT E {
    num := 5,
    single_link := (SELECT C FILTER .val = 'E00'),
};
INSERT E {
    num := 6,
    multi_link := (SELECT C FILTER .val IN {'E01', 'E02'}),
};
INSERT E {
    num := 7,
    single_link := (
        WITH val := 'E00'
        SELECT C {@lp0 := val}
        FILTER .val = val
    ),
    multi_link := (
        FOR val IN {'E01', 'E02', 'E03'}
        UNION (
            SELECT C {@lp1 := val}
            FILTER .val = val
        )
    ),
};


INSERT F {
    num := 8,
    single_link := (SELECT C FILTER .val = 'F00'),
    multi_link := (SELECT C FILTER .val IN {'F01', 'F02', 'F03'}),
};


INSERT G;
INSERT H;
INSERT I;
INSERT J;

INSERT K {k := 'k0'};
INSERT L {l0 := 'l0_0', l1 := 'l1_0'};

INSERT M {m0 := 10, m1 := 'm1'};
INSERT N {n0 := 10, n1 := 'n1'};

INSERT O {o0 := 'ipsum'};

INSERT P {
    plink0 := (
        SELECT C{@p0 := ['hello', 'world']} FILTER .val = 'E00'
    ),
    plink1 := (
        SELECT C{@p1 := [2.5, -4.25]} FILTER .val = 'E00'
    ),
    p2 := ['hello', 'world'],
    p3 := [2.5, -4.25],
};

INSERT Q {
    q0 := (2, False),
    q1 := ('p3', 3.33n),
    q2 := (x := 2, y := False),
    q3 := ('p11', 3.33n),
};

INSERT S {name:= 'name0', s := 's0'};
INSERT T {name:= 'name0', t := 't0'};
INSERT V {name:= 'name1', s := 's1', t := 't1', u := 'u1'};

INSERT W {name := 'w0'};
INSERT W {name := 'w2'};
INSERT W {name := 'w1', w := (SELECT DETACHED W FILTER .name = 'w2')};
INSERT W {name := 'w3'};
INSERT W {name := 'w4', w := (SELECT DETACHED W FILTER .name = 'w3')};
UPDATE W
FILTER .name = 'w3'
SET {
    w := (SELECT DETACHED W FILTER .name = 'w4')
};

INSERT X {name := 'x0'};
INSERT Y {name := 'y0', x := (SELECT X LIMIT 1)};
UPDATE X SET {y := (SELECT Y LIMIT 1)};

INSERT Z {
    ck := (SELECT C FILTER .val = 'F00'),
    stw := (SELECT S FILTER .name = 'name0'),
};
INSERT Z {
    ck := (SELECT K LIMIT 1),
    stw := {
        (SELECT S FILTER .name = 'name0'),
        (SELECT W FILTER .name = 'w1' LIMIT 1),
        (SELECT T FILTER .name = 'name0'),
    }
};

# cross-module data
INSERT DefA {a := 'DefA'};
INSERT test::TestB {b := 'TestB', blink := (SELECT DefA LIMIT 1)};
INSERT DefB {other := (SELECT test::TestB LIMIT 1)};
INSERT test::TestC {c := 'TestC'};
INSERT DefC {other := (SELECT test::TestC LIMIT 1)};
UPDATE test::TestC
SET {clink := (SELECT DefC LIMIT 1)};

# on delete
INSERT TargetA {name := 't0'};
INSERT TargetA {name := 't1'};
INSERT TargetA {name := 't2'};
INSERT TargetA {name := 't3'};
INSERT SourceA {name := 's0', link0 := (SELECT TargetA FILTER .name = 't0')};
INSERT SourceA {name := 's1', link1 := (SELECT TargetA FILTER .name = 't1')};
INSERT SourceA {name := 's2', link2 := (SELECT TargetA FILTER .name = 't2')};
INSERT SourceA {name := 's3', link3 := (SELECT TargetA FILTER .name = 't3')};
