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

finally:
    proc.terminate()
    proc.wait()
