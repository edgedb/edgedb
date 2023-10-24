# Create databases on the older edgedb version

import edgedb
import os
import subprocess

version = os.getenv('EDGEDB_VERSION')
cmd = [
    f'edgedb-server-{version}/bin/edgedb-server', '-D' 'test-dir',
    '--testmode', '--security', 'insecure_dev_mode', '--port', '10000',
]
proc = subprocess.Popen(cmd)

try:
    db = edgedb.create_client(
        host='localhost', port=10000, tls_security='insecure'
    )
    for name in ['json', 'functions', 'expressions', 'casts', 'policies', 'vector', 'scope']:
        db.execute(f'create database {name};')

    # For the scope database, let's actually migration to it.  This
    # will test that the migrations can still work after the upgrade.
    db2 = edgedb.create_client(
        host='localhost', port=10000, tls_security='insecure', database='scope'
    )
    with open("tests/schemas/cards.esdl") as f:
        body = f.read()
    db2.execute(f'''
        START MIGRATION TO {{
            module default {{
                {body}
            }}
        }};
        POPULATE MIGRATION;
        COMMIT MIGRATION;
    ''')

finally:
    proc.terminate()
    proc.wait()
