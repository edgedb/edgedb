#!/usr/bin/env python3

import edgedb
import json
import sys


def main(argv):
    con = edgedb.create_client()

    dbs = con.query('''
        select sys::Database.name
    ''')

    con.close()

    datas = {}
    for db in dbs:
        con = edgedb.create_client(database=db)
        output = json.loads(con.query_single('''
            administer prepare_upgrade()
        '''))
        datas[db] = output

    print(json.dumps(datas))


if __name__ == '__main__':
    sys.exit(main(sys.argv))
