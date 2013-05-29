##
# Copyright (c) 2012 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import os

from metamagic import test
from metamagic.utils.debug import assert_raises

try:
    from metamagic.utils import filemagic
except ImportError:
    _SKIP_MAGIC = True
else:
    _SKIP_MAGIC = False


class TestUtilsFileMagic:
    @test.skipif(_SKIP_MAGIC)
    def test_utils_filemagic_mime(self):
        items = {
            'pdf_test': 'application/pdf',
            'bin_test': 'application/octet-stream',
            'png_test': 'image/png'
        }

        for filename, expected_mime in items.items():
            path = os.path.join(os.path.dirname(__file__), 'testdata', filename)

            with open(path, 'rb') as f:
                mime = filemagic.get_mime_from_buffer(f.read())
                assert mime == expected_mime

                mime = filemagic.get_mime_from_path(path)
                assert mime == expected_mime

                f.seek(0)

                mime = filemagic.get_mime_from_fileno(f.fileno())
                assert mime == expected_mime
