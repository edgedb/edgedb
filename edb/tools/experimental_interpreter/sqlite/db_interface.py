
from typing import *
from ..data.expr_ops import *

# id class
EdgeID = int
class EdgeDatabaseInterface:

    def queryIdsForAType(self, tp: str) -> List[EdgeID]:
        raise NotImplementedError()

    # project a property/link from an object
    def project(self, id: EdgeID, property: str) -> MultiSetVal:
        raise NotImplementedError()

    # insert an object into the database, returns the inserted object id
    def insert(self, tp: str, props : Dict[str, MultiSetVal]) -> EdgeID:
        raise NotImplementedError()
    
    # replace an object's properties in the database
    def replace(self, id: EdgeID, newProps : Dict[str, MultiSetVal]) -> None:
        raise NotImplementedError()

    # delete an object in the database
    def delete(self, id: EdgeID) -> None:
        raise NotImplementedError()

    # transactional evaluation: commit all inserts/updates/deletes
    def commitDML(self) -> None:
        raise NotImplementedError()

