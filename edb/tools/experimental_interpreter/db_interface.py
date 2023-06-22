
from typing import *
from .data.expr_ops import *
from .data.type_ops import *
from .data.data_ops import *

# id class
EdgeID = int
class EdgeDatabaseInterface:

    def queryIdsForAType(self, tp: str) -> List[EdgeID]:
        raise NotImplementedError()

    def getTypeForAnId(self, id: EdgeID) -> str:
        raise NotImplementedError()

    # determines whether the object with a specific id has a property/link that can be projected
    # i.e. if the property/link is a computable, then this should return false and subsequent 
    # calls to project will throw an error
    def isProjectable(self, id: EdgeID, property: str) -> bool:
        raise NotImplementedError()

    # project a property/link from an object
    def project(self, id: EdgeID, property: str) -> MultiSetVal:
        raise NotImplementedError()

    # get all reverse links for a given object in a set of 
    # objects, including its link properties
    # That is, return a list of ids which has a link (via the given property) 
    # to any object in the given set
    def reverseProject(self, ids: Sequence[EdgeID], property: str) -> MultiSetVal:
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

    def getSchema(self) -> DBSchema:
        raise NotImplementedError()


class InMemoryEdgeDatabase(EdgeDatabaseInterface):

    def __init__(self, schema) -> None:
        super().__init__()
        self.schema = schema
        self.db = DB({})
        self.to_delete = DB({})
        self.to_update = DB({})

    def queryIdsForAType(self, tp: str) -> List[EdgeID]:
        return [id for id in self.db.dbdata.keys() if self.db.dbdata[id].tp.name == tp]

    def getDBEntryForId(self, id: EdgeID) -> DBEntry:
        if id in self.db.dbdata.keys():
            return self.db.dbdata[id]
        elif id in self.to_update.dbdata.keys():
            return self.to_update.dbdata[id]
        elif id in self.to_delete.dbdata.keys():
            return self.to_delete.dbdata[id]
        else:
            raise ValueError(f"ID {id} not found in database")

    
    def getTypeForAnId(self, id: EdgeID) -> str:
        return self.getDBEntryForId(id).tp.name
    
    def isProjectable(self, id: EdgeID, prop: str) -> bool:
        return StrLabel(prop) in self.getDBEntryForId(id).data.val.keys()
    
    def project(self, id: EdgeID, prop: str) -> MultiSetVal:
        return self.getDBEntryForId(id).data.val[StrLabel(prop)][1]

    def reverseProject(self, subject_ids: Sequence[EdgeID], prop: str) -> MultiSetVal:
        results: List[Val] = []
        for (id, obj) in self.db.dbdata.items():
            if StrLabel(prop) in obj.data.val.keys():
                object_vals = obj.data.val[StrLabel(prop)][1].vals
                if all(isinstance(object_val, LinkPropVal)
                        for object_val in object_vals):
                    object_id_mapping = {
                        object_val.refid: object_val.linkprop
                        for object_val in object_vals
                        if isinstance(object_val, LinkPropVal)}
                    for (object_id,
                            obj_linkprop_val) in object_id_mapping.items():
                        if object_id in subject_ids:
                            results = [
                                *results,
                                LinkPropVal(
                                    refid=id,
                                    linkprop=obj_linkprop_val)]
        return MultiSetVal(results)
    

    
        