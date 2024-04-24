from __future__ import annotations
from typing import List, Dict, Tuple, Sequence
from .data import data_ops as e
from .data import expr_ops as eops
import copy


class EdgeDatabaseStorageProviderInterface:

    # filters have intersection semantics
    def query_ids_for_a_type(
        self, tp: e.QualifiedName, filters: e.EdgeDatabaseSelectFilter
    ) -> List[e.EdgeID]:
        raise NotImplementedError()

    def get_schema(self) -> e.DBSchema:
        raise NotImplementedError()

    def next_id(self) -> e.EdgeID:
        raise NotImplementedError()

    def insert(
        self,
        id: e.EdgeID,
        tp: e.QualifiedName,
        props: Dict[str, e.MultiSetVal],
    ) -> None:
        raise NotImplementedError()

    def delete(self, id: e.EdgeID, tp: e.QualifiedName) -> None:
        raise NotImplementedError()

    def update(
        self,
        id: e.EdgeID,
        tp: e.QualifiedName,
        props: Dict[str, e.MultiSetVal],
    ) -> None:
        raise NotImplementedError()

    def check_id_present(self, id: e.EdgeID) -> bool:
        raise NotImplementedError()

    # project a property/link from an object
    def project(
        self, id: e.EdgeID, tp: e.QualifiedName, property: str
    ) -> e.MultiSetVal:
        raise NotImplementedError()

    # get all reverse links for a given object in a set of
    # objects, including its link properties
    # That is, return a list of ids which has a link (via the given property)
    # to any object in the given set
    def reverse_project(
        self, ids: Sequence[e.EdgeID], property: str
    ) -> e.MultiSetVal:
        raise NotImplementedError()

    def dump_state(self) -> object:
        raise NotImplementedError()

    def restore_state(self, dumped_state) -> None:
        raise NotImplementedError()

    def commit(self) -> None:
        raise NotImplementedError()


class InMemoryEdgeDatabaseStorageProvider(
    EdgeDatabaseStorageProviderInterface
):

    def __init__(self, schema) -> None:
        super().__init__()
        self.schema = schema
        self.db = e.DB({})
        self.next_id_to_return = 1

    def get_schema(self) -> e.DBSchema:
        return self.schema

    def query_ids_for_a_type(
        self, tp: e.QualifiedName, filters: e.EdgeDatabaseSelectFilter
    ) -> List[e.EdgeID]:
        def check_filter(filter: e.EdgeDatabaseEqFilter, id: e.EdgeID) -> bool:
            data_to_check = self.db.dbdata[id].data
            target_vals = data_to_check[filter.propname]
            assert isinstance(filter.arg, e.MultiSetVal)
            return any(
                eops.val_eq(v, vv)
                for v in target_vals.getVals()
                for vv in filter.arg.getVals()
            )

        def check_filter_top(
            filter: e.EdgeDatabaseSelectFilter, id: e.EdgeID
        ) -> bool:
            match filter:
                case e.EdgeDatabaseEqFilter(propname, arg):
                    return check_filter(
                        e.EdgeDatabaseEqFilter(propname, arg), id
                    )
                case e.EdgeDatabaseConjunctiveFilter(filters):
                    return all(check_filter_top(f, id) for f in filters)
                case e.EdgeDatabaseDisjunctiveFilter(filters):
                    return any(check_filter_top(f, id) for f in filters)
                case e.EdgeDatabaseTrueFilter():
                    return True
                case _:
                    raise ValueError("Unsupported filter type")

        return [
            id
            for id in self.db.dbdata.keys()
            if self.db.dbdata[id].tp == tp and check_filter_top(filters, id)
        ]

    def dump_state(self) -> object:
        return {
            "db": copy.deepcopy(self.db.dbdata),
            "next_id_to_return": self.next_id_to_return,
        }

    def restore_state(self, dumped_state) -> None:
        self.db = e.DB(copy.copy(dumped_state["db"]))
        self.next_id_to_return = dumped_state["next_id_to_return"]

    def project(
        self, id: e.EdgeID, tp: e.QualifiedName, prop: str
    ) -> e.MultiSetVal:
        if id in self.db.dbdata.keys():
            props = self.db.dbdata[id].data
        else:
            raise ValueError(f"ID {id} not found in database")
        if prop in props:
            return props[prop]
        else:
            raise ValueError(f"Property {prop} not found in object {id}")

    def check_id_present(self, id: e.EdgeID) -> bool:
        return id in self.db.dbdata.keys()

    def reverse_project(
        self, subject_ids: Sequence[e.EdgeID], prop: str
    ) -> e.MultiSetVal:
        results: List[e.Val] = []
        for id, obj in self.db.dbdata.items():
            if prop in obj.data.keys():
                object_vals = obj.data[prop].getVals()
                if all(
                    isinstance(object_val, e.RefVal)
                    for object_val in object_vals
                ):
                    object_id_mapping = {
                        object_val.refid: object_val.val
                        for object_val in object_vals
                        if isinstance(object_val, e.RefVal)
                    }
                    for (
                        object_id,
                        obj_linkprop_val,
                    ) in object_id_mapping.items():
                        if not all(
                            isinstance(lbl, e.LinkPropLabel)
                            for lbl in obj_linkprop_val.val.keys()
                        ):
                            raise ValueError(
                                "Expecting only link prop vals in store"
                            )
                        if object_id in subject_ids:
                            results = [
                                *results,
                                e.RefVal(
                                    refid=id,
                                    tpname=obj.tp,
                                    val=obj_linkprop_val,
                                ),
                            ]
        return e.ResultMultiSetVal(results)

    def next_id(self) -> e.EdgeID:
        id = self.next_id_to_return
        self.next_id_to_return += 1
        return id

    def insert(
        self,
        id: e.EdgeID,
        tp: e.QualifiedName,
        props: Dict[str, e.MultiSetVal],
    ) -> None:
        self.db.dbdata[id] = e.DBEntry(tp, props)

    def delete(self, id: e.EdgeID, tp: e.QualifiedName) -> None:
        del self.db.dbdata[id]

    def update(
        self,
        id: e.EdgeID,
        tp: e.QualifiedName,
        props: Dict[str, e.MultiSetVal],
    ) -> None:
        if id not in self.db.dbdata.keys():
            raise ValueError(f"ID {id} not found in database")
        self.db.dbdata[id] = e.DBEntry(
            tp=self.db.dbdata[id].tp, data={**self.db.dbdata[id].data, **props}
        )

    def commit(self) -> None:
        pass


class EdgeDatabase:

    def __init__(self, storage: EdgeDatabaseStorageProviderInterface) -> None:
        super().__init__()
        self.storage = storage
        self.to_delete: List[Tuple[e.QualifiedName, e.EdgeID]] = []
        self.to_update: Dict[
            e.EdgeID, Tuple[e.QualifiedName, Dict[str, e.MultiSetVal]]
        ] = {}
        self.to_insert = e.DB({})

    def dump_state(self) -> object:
        return {
            "storage": self.storage.dump_state(),
            "to_delete": copy.deepcopy(self.to_delete),
            "to_update": copy.deepcopy(self.to_update),
            "to_insert": copy.deepcopy(self.to_insert),
        }

    def restore_state(self, dumped_state) -> None:
        self.storage.restore_state(dumped_state["storage"])
        self.to_delete = copy.copy(dumped_state["to_delete"])
        self.to_update = copy.copy(dumped_state["to_update"])
        self.to_insert = copy.copy(dumped_state["to_insert"])

    def project(
        self, id: e.EdgeID, tp: e.QualifiedName, prop: str
    ) -> e.MultiSetVal:
        if id in self.to_insert.dbdata.keys():
            raise ValueError(
                "Semantic Change: Insert should carry "
                "properties before storage coercion"
            )

        result = self.storage.project(id, tp, prop)
        assert isinstance(result, e.MultiSetVal)
        return result

    def delete(self, id: e.EdgeID, tp: e.QualifiedName) -> None:
        self.to_delete.append((tp, id))

    def insert(
        self, tp: e.QualifiedName, props: Dict[str, e.MultiSetVal]
    ) -> e.EdgeID:
        id = self.storage.next_id()
        self.to_insert.dbdata[id] = e.DBEntry(tp, props)
        return id

    def update(
        self,
        id: e.EdgeID,
        tp: e.QualifiedName,
        props: Dict[str, e.MultiSetVal],
    ) -> None:
        if id in self.to_insert.dbdata.keys():
            self.to_insert.dbdata[id] = e.DBEntry(
                tp=self.to_insert.dbdata[id].tp,
                data={**self.to_insert.dbdata[id].data, **props},
            )
        else:
            self.to_update[id] = (tp, props)

    def commit_dml(self) -> None:
        # updates must happen after insert because it may update inserted data
        for id, insert_obj in self.to_insert.dbdata.items():
            self.storage.insert(id, insert_obj.tp, insert_obj.data)
        for id, (tpname, obj) in self.to_update.items():
            self.storage.update(id, tpname, obj)
        # delete happens last, you may also delete an inserted object
        for tpname, id in self.to_delete:
            self.storage.delete(id, tpname)
        self.to_delete = []
        self.to_update = {}
        self.to_insert = e.DB({})
        self.storage.commit()

    def get_schema(self) -> e.DBSchema:
        return self.storage.get_schema()
