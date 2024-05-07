import typing

class SyntaxError(Exception): ...

class ParserResult:
    out: typing.Optional[CSTNode | typing.List[OpaqueToken]]
    errors: typing.List[
        typing.Tuple[
            str,
            typing.Tuple[int, typing.Optional[int]],
            typing.Optional[str],
            typing.Optional[str],
        ]
    ]

    def pack(self) -> bytes: ...

class Hasher:
    @staticmethod
    def start_migration(parent_id: str) -> Hasher: ...
    def add_source(self, data: str) -> None: ...
    def make_migration_id(self) -> str: ...

unreserved_keywords: typing.FrozenSet[str]
partial_reserved_keywords: typing.FrozenSet[str]
future_reserved_keywords: typing.FrozenSet[str]
current_reserved_keywords: typing.FrozenSet[str]

class Entry:
    key: bytes

    tokens: typing.List[OpaqueToken]

    extra_blobs: typing.List[bytes]

    first_extra: typing.Optional[int]

    extra_counts: typing.List[int]

    def get_variables(self) -> typing.Dict[str, typing.Any]: ...
    def pack(self) -> bytes: ...

def normalize(text: str) -> Entry: ...
def parse(
    start_token_name: str, tokens: typing.List[OpaqueToken]
) -> typing.Tuple[
    ParserResult, typing.List[typing.Tuple[typing.Type, typing.Callable]]
]: ...
def preload_spec(spec_filepath: str) -> None: ...
def save_spec(spec_json: str, dst: str) -> None: ...

class CSTNode:
    production: typing.Optional[Production]
    terminal: typing.Optional[Terminal]

class Production:
    id: int
    args: typing.List[CSTNode]

class Terminal:
    text: str
    value: typing.Any
    start: int
    end: int

class SourcePoint:
    line: int
    zero_based_line: int
    column: int
    utf16column: int
    offset: int
    char_offset: int

    @staticmethod
    def from_offsets(
        data: bytes, offsets: typing.List[int]
    ) -> typing.List[SourcePoint]: ...

def offset_of_line(text: str, target: int) -> int: ...

class OpaqueToken: ...

def tokenize(s: str) -> ParserResult: ...
def unpickle_token(bytes: bytes) -> OpaqueToken: ...
def unpack(serialized: bytes) -> Entry | typing.List[OpaqueToken]: ...
