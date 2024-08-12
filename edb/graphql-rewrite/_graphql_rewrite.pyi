from typing import Any, Optional, Tuple, Dict, List

class Entry:
    key: str
    key_vars: List[str]
    variables: Dict[str, Any]
    substitutions: Dict[str, Tuple[str, int, int]]

    def tokens(self) -> List[Tuple[Any, int, int, int, int, Any]]: ...

def rewrite(operation: Optional[str], text: str) -> Entry: ...
