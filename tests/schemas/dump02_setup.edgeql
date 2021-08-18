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


SET MODULE default;

CREATE MIGRATION m14pt32wg7zijus7jmmnvbtf7dftt34xxgbkrtmys46pmrailnkcia
ONTO m1bzqjsa6r7e3prynbm52zutm3jo42ybgulhsjlnxbd43m4sqdmcoq {
    CREATE TYPE default::Migrated;
    create type default::Migrated2 {};
};

# Not sure if the esdl filename will handle this on all systems, so
# I'm adding stuff here.
CREATE MODULE `💯💯💯`;

CREATE FUNCTION `💯💯💯`::`🚀🙀🚀`(`🤞`: default::`🚀🚀🚀`) -> `🚀🚀🚀`
USING (
    SELECT <`🚀🚀🚀`>(`🤞` ++ 'Ł🙀')
);
# end of DDL

INSERT `S p a M` {
    `🚀` := 42
};

INSERT A {
    `s p A m 🤞` := assert_single((SELECT `S p a M` FILTER .`🚀` = 42))
};

INSERT Łukasz;

INSERT Łukasz {
    `Ł🤞` := 'simple 🚀',
    `Ł💯` := (
        SELECT A
        # {
        #     `🙀🚀🚀🚀🙀`:= 'Łink prop 🙀🚀🚀🚀🙀',
        #     `🙀مرحبا🙀`:=
        #         `💯💯💯`::`🚀🙀🚀`('Łink prop 🙀مرحبا🙀'),
        # }
        FILTER .`s p A m 🤞`.`🚀` = 42
        LIMIT 1
    )
};
