from edb.common import devmode
from edb.testbase import lang

from edb.pgsql import parser as pg_parser
from edb.pgsql import resolver as pg_resolver

SCHEMA = '''
type Person {
  required property first_name -> str;
  property last_name -> str;
}

type Movie {
  required property title -> str;
  property release_year -> int64;
  multi link actors -> Person;
  link director -> Person;
}
'''

def main():
    devmode.enable_dev_mode()  # for std schema caching
    
    schema = lang.BaseSchemaTest.load_schema(SCHEMA, modname='default')

    [query] = pg_parser.parse('SELECT title FROM Movie')
    resolved = pg_resolver.resolve(query, schema)
    resolved.dump_sql()


if __name__ == '__main__':
    main()