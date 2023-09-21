# Compute prior minor versions to test upgrading from

import json
import os
import re
from urllib import request

base = 'https://packages.edgedb.com'
u = f'{base}/archive/.jsonindexes/x86_64-unknown-linux-gnu.json'
data = json.loads(request.urlopen(u).read())

u = f'{base}/archive/.jsonindexes/x86_64-unknown-linux-gnu.testing.json'
data_testing = json.loads(request.urlopen(u).read())


branch = os.getenv('GITHUB_BASE_REF') or os.getenv('GITHUB_REF_NAME')
print("BRANCH", branch)
version = int(re.findall(r'\d+', branch)[0])

versions = []
for obj in data['packages'] + data_testing['packages']:
    if (
        obj['version_details']['major'] == version
        and (
            not obj['version_details']['prerelease']
            or obj['version_details']['prerelease'][0]['phase'] in ('beta', 'rc')
        )
    ):
        versions.append((obj['version'], base + obj['installrefs'][0]['ref']))

matrix = {
    "include": [
        {"edgedb-version": v, "edgedb-url": url, "make-dbs": mk}
        for v, url in versions
        for mk in [True, False]
    ]
}

print("matrix:", matrix)
if output := os.getenv('GITHUB_OUTPUT'):
    with open(output, 'a') as f:
        print(f'matrix={json.dumps(matrix)}', file=f)
