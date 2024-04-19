#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2019-present MagicStack Inc. and the EdgeDB authors.
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


from __future__ import annotations

import base64


_react_ver = '16.8.3'
_graphiql_ver = '0.12.0'


_edgedb_logo = base64.b64encode(br'''
<svg viewBox="0 0 110 90" xmlns="http://www.w3.org/2000/svg">
  <path d="M93.17 44.76c0 7.59-3.043 8.95-6.445 8.95h-7.233V35.808h7.233c3.402 0 6.446 1.361 6.446 8.952zm-3.688 0c0-5.3-1.61-5.551-3.938-5.551h-2.256v11.1h2.256c2.327 0 3.938-.25 3.938-5.55zM51.17 53.71V35.808h11.386v3.402h-7.59v3.652h5.728v3.366h-5.729v4.082h7.591v3.402H51.17zm17.76 35.808h3.796V0H68.93v89.518zm31.833-43.756v4.547h3.15c1.97 0 2.471-1.289 2.471-2.256 0-.752-.358-2.291-3.043-2.291h-2.578zm0-6.553v3.402h2.578c1.468 0 2.327-.645 2.327-1.72 0-1.073-.86-1.682-2.327-1.682h-2.578zm-3.796-3.402h7.305c3.831 0 4.977 2.686 4.977 4.62 0 1.79-1.146 3.079-1.934 3.437 2.292 1.11 2.686 3.366 2.686 4.512 0 1.504-.752 5.335-5.73 5.335h-7.304V35.807zM29.362 44.76c0 7.591-3.044 8.952-6.445 8.952h-7.233V35.807h7.233c3.401 0 6.445 1.361 6.445 8.952zm11.172 5.693c1.933 0 2.936-.644 3.294-1.074v-1.97h-3.08V44.33h6.124v7.126c-.537.824-3.474 2.435-6.16 2.435-4.403 0-8.127-1.719-8.127-9.31s3.76-8.952 7.161-8.952c5.335 0 6.66 2.793 7.09 5.264l-3.151.716c-.18-1.146-1.182-2.578-3.473-2.578-2.328 0-3.94.25-3.94 5.55s1.684 5.872 4.262 5.872zm-14.86-5.693c0-5.3-1.611-5.55-3.939-5.55h-2.256v11.1h2.256c2.328 0 3.939-.25 3.939-5.55zM0 53.711V35.807h11.387v3.402H3.796v3.652h5.729v3.366h-5.73v4.082h7.592v3.402H0z" fill="#4A4A4A" fill-rule="evenodd"/>
</svg>''').decode()  # NoQA


EXPLORE_HTML = (r'''
<!DOCTYPE html>
<html>
  <head>
    <meta charset="UTF-8" />
    <link rel="icon" href="data:;base64,=" />

    <style>
      body {
        height: 100%;
        margin: 0;
        width: 100%;
        overflow: hidden;
      }
      #graphiql {
        height: 100vh;
        width: 100wh;
      }
    </style>

    <script src="//cdnjs.cloudflare.com/ajax/libs/react/''' +
        _react_ver + r'''/umd/react.production.min.js"></script>
    <script src="//cdnjs.cloudflare.com/ajax/libs/react-dom/''' +
        _react_ver + r'''/umd/react-dom.production.min.js"></script>
    <script src="//cdnjs.cloudflare.com/ajax/libs/graphiql/''' +
        _graphiql_ver + r'''/graphiql.min.js"></script>
    <link href="//cdnjs.cloudflare.com/ajax/libs/graphiql/''' +
        _graphiql_ver + r'''/graphiql.min.css" rel="stylesheet" />
  </head>
  <body>
    <div id="graphiql">Loading...</div>
    <script><!--
      function graphQLFetcher(graphQLParams) {
        const root = window.location.toString().replace(/\/explore[\/]*$/, '');
        return fetch(root, {
          method: 'post',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(graphQLParams),
        }).then(function(response) { return response.json() });
      }

      ReactDOM.render(
        React.createElement(
          GraphiQL,
          {
            fetcher: graphQLFetcher,
          },
          React.createElement(
            GraphiQL.Logo,
            {},
            React.createElement(
              'div',
              {
                style: {
                  backgroundImage: 'url("data:image/svg+xml;base64,''' +
                    _edgedb_logo + r'''")',
                  backgroundSize: 'cover',
                  backgroundRepeat: 'no-repeat',
                  backgroundPosition: 'center center',
                  height: 40,
                  width: 50,
                }
              }
            )
          )
        ),
        document.getElementById('graphiql')
      );
    //-->
    </script>
  </body>
</html>
''').encode()
