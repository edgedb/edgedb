
import sys
import json

from edb.pgsql.parser import pg_parse, build_queries

sql_bytes = bytes(sys.argv[1], encoding='UTF8')

ast_json = pg_parse(sql_bytes)
print(ast_json)

ast = json.loads(ast_json, strict=False)

queries = build_queries(ast)

queries[0].dump()
queries[0].dump_sql()