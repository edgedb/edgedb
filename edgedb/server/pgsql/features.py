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


"""Database features."""

import asyncpg.exceptions

from edgedb.server.pgsql import deltadbops


class UuidFeature(deltadbops.Feature):
    source = '%(pgpath)s/contrib/uuid-ossp.sql'

    def __init__(self, schema='edgedb'):
        super().__init__(name='uuid', schema=schema)

    def get_extension_name(self):
        return 'uuid-ossp'


class HstoreFeature(deltadbops.Feature):
    source = '%(pgpath)s/contrib/hstore.sql'

    def __init__(self, schema='edgedb'):
        super().__init__(name='hstore', schema=schema)

    @classmethod
    def init_feature(cls, db):
        try:
            db.typio.identify(contrib_hstore='edgedb.hstore')
        except asyncpg.exceptions.InvalidSchemaNameError:
            pass


class CryptoFeature(deltadbops.Feature):
    source = '%(pgpath)s/contrib/pgcrypto.sql'

    def __init__(self, schema='edgedb'):
        super().__init__(name='pgcrypto', schema=schema)


class FuzzystrmatchFeature(deltadbops.Feature):
    source = '%(pgpath)s/contrib/fuzzystrmatch.sql'

    def __init__(self, schema='edgedb'):
        super().__init__(name='fuzzystrmatch', schema=schema)


class ProductAggregateFeature(deltadbops.Feature):
    def __init__(self, schema='edgedb'):
        super().__init__(name='agg_product', schema=schema)

    async def code(self, context):
        return """
            CREATE AGGREGATE {schema}.agg_product(double precision)
                (SFUNC=float8mul, STYPE=double precision, INITCOND=1);
            CREATE AGGREGATE {schema}.agg_product(numeric)
                (SFUNC=numeric_mul, STYPE=numeric, INITCOND=1);
        """.format(schema=self.schema)


class KnownRecordMarkerFeature(deltadbops.Feature):
    def __init__(self, schema='edgedb'):
        super().__init__(name='known_record_marker_t', schema=schema)

    async def code(self, context):
        return """
            CREATE DOMAIN {schema}.known_record_marker_t AS text;
        """.format(schema=self.schema)

    @classmethod
    def init_feature(cls, db):
        ps = db.prepare(
            '''
            SELECT
                t.oid
            FROM
                pg_type t INNER JOIN pg_namespace ns
                    ON t.typnamespace = ns.oid
            WHERE
                t.typname = 'known_record_marker_t'
                AND ns.nspname = 'edgedb'
        ''')
        oid = ps.first()
        if oid is not None:
            db._sx_known_record_marker_oid_ = oid


class GisFeature(deltadbops.Feature):
    def __init__(self, schema='edgedb_aux_feat_gis'):
        super().__init__(name='postgis', schema=schema)

    @classmethod
    def init_feature(cls, db):
        cls.reset_connection(db)

        for typ in ('box2d', 'box3d', 'geometry', 'geography'):
            try:
                db.typio.identify(
                    contrib_postgis='{}.{}'.format('edgedb_aux_feat_gis', typ))
            except asyncpg.exceptions.InvalidSchemaNameError:
                pass

    @classmethod
    def reset_connection(cls, connection):
        search_path = connection.settings['search_path']

        if 'edgedb_aux_feat_gis' not in search_path:
            connection.settings[
                'search_path'] = search_path + ',edgedb_aux_feat_gis'
