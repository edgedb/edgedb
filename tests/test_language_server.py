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


import threading
import io
import unittest
import time

try:
    from edb.language_server import main as ls_main
except ImportError:
    ls_main = None  # type: ignore
    pass


@unittest.skipIf(ls_main is None, 'edgedb-ls dependencies are missing')
class TestLanguageServer(unittest.TestCase):

    def test_language_server_01(self):

        message = '''{
            "jsonrpc": "2.0",
            "method": "initialize",
            "id": 1,
            "params": {
                "processId": null,
                "rootUri": null,
                "capabilities": { }
            }
        }'''

        bytes_msg = bytes(message, encoding='utf-8')
        stream_in = io.BytesIO(
            bytes(f"Content-Length: {len(bytes_msg)}\r\n\r\n", encoding='ascii')
            + bytes_msg
        )
        stream_out = io.BytesIO()

        ls = ls_main.init()

        def stop_server():
            time.sleep(1)
            ls.shutdown()

        threading.Thread(target=stop_server).start()
        ls.start_io(stdin=stream_in, stdout=stream_out)

        expected = (
            'Content-Length: 95\r\n'
            'Content-Type: application/vscode-jsonrpc; charset=utf-8\r\n'
            '\r\n'
            '{"params": {"type": 4, "message": "Starting"}, "method": '
            '"window/logMessage", "jsonrpc": "2.0"}Content-Length: 94\r\n'
            'Content-Type: application/vscode-jsonrpc; charset=utf-8\r\n'
            '\r\n'
            '{"params": {"type": 4, "message": "Started"}, "method": '
            '"window/logMessage", "jsonrpc": "2.0"}Content-Length: 425\r\n'
            'Content-Type: application/vscode-jsonrpc; charset=utf-8\r\n'
            '\r\n'
            '{"id": 1, "jsonrpc": "2.0", "result": {"capabilities": '
            '{"positionEncoding": "utf-16", "textDocumentSync": {"openClose": '
            'true, "change": 2, "save": false}, "completionProvider": '
            '{"triggerCharacters": [","]}, "executeCommandProvider": '
            '{"commands": []}, "workspace": {"workspaceFolders": {"supported":'
            ' true, "changeNotifications": true}, "fileOperations": {}}}, '
            '"serverInfo": {"name": "Gel Language Server", "version": "v0.1"'
            '}}}'
        )

        self.assertEqual(str(stream_out.getvalue(), encoding='ascii'), expected)
