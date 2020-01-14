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


SET MODULE test;

INSERT Tree {val := '0'};
INSERT Tree {
    val := '00', parent := (SELECT DETACHED Tree FILTER .val = '0')};
INSERT Tree {
    val := '01', parent := (SELECT DETACHED Tree FILTER .val = '0')};
INSERT Tree {
    val := '02', parent := (SELECT DETACHED Tree FILTER .val = '0')};
INSERT Tree {
    val := '000', parent := (SELECT DETACHED Tree FILTER .val = '00')};
INSERT Tree {
    val := '010', parent := (SELECT DETACHED Tree FILTER .val = '01')};

INSERT Tree {val := '1'};
INSERT Tree {
    val := '10', parent := (SELECT DETACHED Tree FILTER .val = '1')};
INSERT Tree {
    val := '11', parent := (SELECT DETACHED Tree FILTER .val = '1')};
INSERT Tree {
    val := '12', parent := (SELECT DETACHED Tree FILTER .val = '1')};
INSERT Tree {
    val := '13', parent := (SELECT DETACHED Tree FILTER .val = '1')};


# same structure using a different tree type
INSERT Eert {val := '000'};
INSERT Eert {val := '010'};

INSERT Eert {
    val := '00', children := (SELECT DETACHED Eert FILTER .val = '000')};
INSERT Eert {
    val := '01', children := (SELECT DETACHED Eert FILTER .val = '010')};
INSERT Eert {val := '02'};

INSERT Eert {
    val := '0',
    children := (SELECT DETACHED Eert FILTER .val IN {'00', '01', '02'}),
};

INSERT Eert {val := '10'};
INSERT Eert {val := '11'};
INSERT Eert {val := '12'};
INSERT Eert {val := '13'};

INSERT Eert {
    val := '1',
    children := (SELECT DETACHED Eert FILTER .val IN {'10', '11', '12', '13'}),
};
