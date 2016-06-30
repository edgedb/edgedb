##
# Copyright (c) 2008-2012 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


"""Database features."""

import postgresql.exceptions

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
        except postgresql.exceptions.SchemaNameError:
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
            CREATE AGGREGATE {schema}.agg_product(double precision) (SFUNC=float8mul, STYPE=double precision, INITCOND=1);
            CREATE AGGREGATE {schema}.agg_product(numeric) (SFUNC=numeric_mul, STYPE=numeric, INITCOND=1);
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
        ps = db.prepare('''
            SELECT
                t.oid
            FROM
                pg_type t INNER JOIN pg_namespace ns ON t.typnamespace = ns.oid
            WHERE
                t.typname = 'known_record_marker_t'
                AND ns.nspname = 'edgedb'
        ''')
        oid = ps.first()
        if oid is not None:
            db._sx_known_record_marker_oid_ = oid


class GisFeature(deltadbops.Feature):
    def __init__(self, schema='caos_aux_feat_gis'):
        super().__init__(name='postgis', schema=schema)

    @classmethod
    def init_feature(cls, db):
        cls.reset_connection(db)

        for typ in ('box2d', 'box3d', 'geometry', 'geography'):
            try:
                db.typio.identify(contrib_postgis='{}.{}'.format('caos_aux_feat_gis', typ))
            except postgresql.exceptions.SchemaNameError:
                pass

    @classmethod
    def reset_connection(cls, connection):
        search_path = connection.settings['search_path']

        if 'caos_aux_feat_gis' not in search_path:
            connection.settings['search_path'] = search_path + ',caos_aux_feat_gis'
