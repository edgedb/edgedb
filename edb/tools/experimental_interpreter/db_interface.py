
from typing import *
from .data.expr_ops import *
from .data.type_ops import *
from .data.data_ops import *

# id class
EdgeID = int
class EdgeDatabaseInterface:

    def query_ids_for_a_type(self, tp: str) -> List[EdgeID]:
        raise NotImplementedError()

    def get_type_for_an_id(self, id: EdgeID) -> str:
        raise NotImplementedError()

    def get_props_for_id(self, id: EdgeID) -> Dict[str, MultiSetVal]:
        raise NotImplementedError()

    # determines whether the object with a specific id has a property/link that can be projected
    # i.e. if the property/link is a computable, then this should return false and subsequent 
    # calls to project will throw an error
    def is_projectable(self, id: EdgeID, property: str) -> bool:
        raise NotImplementedError()

    # project a property/link from an object
    def project(self, id: EdgeID, property: str) -> MultiSetVal:
        raise NotImplementedError()

    # get all reverse links for a given object in a set of 
    # objects, including its link properties
    # That is, return a list of ids which has a link (via the given property) 
    # to any object in the given set
    def reverse_project(self, ids: Sequence[EdgeID], property: str) -> MultiSetVal:
        raise NotImplementedError()

    # insert an object into the database, returns the inserted object id
    def insert(self, tp: str, props : Dict[str, MultiSetVal]) -> EdgeID:
        raise NotImplementedError()

    # updates an object's properties in the database, unspecified properties are not changed
    # the update must be able to apply the insert object in the SAME transaction
    def update(self, id: EdgeID, props : Dict[str, MultiSetVal]) -> None:
        raise NotImplementedError()

    # delete an object in the database
    def delete(self, id: EdgeID) -> None:
        raise NotImplementedError()

    # transactional evaluation: commit all inserts/updates/deletes
    def commit_dml(self) -> None:
        raise NotImplementedError()

    def get_schema(self) -> DBSchema:
        raise NotImplementedError()

    # retrieves an next id for object assignment, this id will 
    # not be identical to any id that is currently anywhere
    def next_id(self) -> EdgeID:
        raise NotImplementedError()

    # this function should close the database connection, to release resources
    # it is a no-op if the database is not an actual database
    # after closing, no operation shall be invoked
    def close(self) -> None:
        raise NotImplementedError()


class InMemoryEdgeDatabase(EdgeDatabaseInterface):

    def __init__(self, schema) -> None:
        super().__init__()
        self.schema = schema
        self.db = DB({})
        self.to_delete : List[EdgeID] = []
        self.to_update : Dict[EdgeID, Dict[str, MultiSetVal]]= {}
        self.to_insert = DB({})
        self.next_id_to_return = 1

    def query_ids_for_a_type(self, tp: str) -> List[EdgeID]:
        return [id for id in self.db.dbdata.keys() if self.db.dbdata[id].tp == tp]

    def get_props_for_id(self, id: EdgeID) -> Dict[str, MultiSetVal]:
        if id in self.db.dbdata.keys():
            return self.db.dbdata[id].data
        # updates are queried before insert as we are able to update an inserted object
        elif id in self.to_update.keys():
            return self.to_update[id]
        elif id in self.to_insert.dbdata.keys():
            return self.to_insert.dbdata[id].data
        # updates and deletes are all in db.dbdata
        else:
            raise ValueError(f"ID {id} not found in database")

    
    def get_type_for_an_id(self, id: EdgeID) -> str:
        if id in self.db.dbdata.keys():
            return self.db.dbdata[id].tp
        elif id in self.to_insert.dbdata.keys():
            return self.to_insert.dbdata[id].tp
        # updates and deletes are all in db or to_insert
        else:
            raise ValueError(f"ID {id} not found in database")
    
    def is_projectable(self, id: EdgeID, prop: str) -> bool:
        return prop in self.get_props_for_id(id).keys()
    
    def project(self, id: EdgeID, prop: str) -> MultiSetVal:
        return self.get_props_for_id(id)[prop]

    def reverse_project(self, subject_ids: Sequence[EdgeID], prop: str) -> MultiSetVal:
        results: List[Val] = []
        for (id, obj) in self.db.dbdata.items():
            if prop in obj.data.keys():
                object_vals = obj.data[prop].vals
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

    def delete(self, id: EdgeID) -> None:
        self.to_delete.append(id)

    def insert(self, tp: str, props : Dict[str, MultiSetVal]) -> EdgeID:
        id = self.next_id()
        self.to_insert.dbdata[id] = DBEntry(tp, props)
        return id

    def update(self, id: EdgeID, props : Dict[str, MultiSetVal]) -> None:
        self.to_update[id] = props
    
    def commit_dml(self) -> None:
        # updates must happen after insert because it may update inserted data
        for (id, insert_obj) in self.to_insert.dbdata.items():
            self.db.dbdata[id] = insert_obj
        for (id, obj) in self.to_update.items():
            if id not in self.db.dbdata.keys():
                raise ValueError(f"ID {id} not found in database")
            self.db.dbdata[id] = DBEntry(
                tp=self.db.dbdata[id].tp,
                data={
                    **self.db.dbdata[id].data,
                    **obj
                }
            )
        # delete happens last, you may also delete an inserted object
        for id in self.to_delete:
            del self.db.dbdata[id]
        self.to_delete = []
        self.to_update = {}
        self.to_insert = DB({})
        
    def get_schema(self) -> DBSchema:
        return self.schema
    
    def next_id(self) -> EdgeID:
        id = self.next_id_to_return
        self.next_id_to_return += 1
        return id
    
    def close(self) -> None:
        pass
    