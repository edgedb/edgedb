from semantix.utils.json import _encoder
from semantix.utils.json._encoder import Encoder
import sys
from decimal import Decimal
from uuid import UUID

exec(''.join(sys.argv[1:]), locals(), globals())

