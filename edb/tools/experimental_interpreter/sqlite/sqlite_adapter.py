
import sqlite3
import pickle
from typing import List

from ..db_interface import EdgeID

from ..data.data_ops import DB, DBSchema, MultiSetVal, ResultTp, ObjectTp
from typing import *
from ..basis.built_ins import all_builtin_funcs
from ..db_interface import EdgeDatabaseInterface
from ..elab_schema import schema_from_sdl_file, schema_from_sdl_defs

import copy

# def sqlite_dbschema(sqlite_dbschema: Dict[str, ObjectTp]) -> DBSchema:
#     return DBSchema(sqlite_dbschema, all_builtin_funcs)



class SQLiteEdgeDatabase(EdgeDatabaseInterface):

    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn
        self.cursor = conn.cursor()
        # to_insert is set to zero usually, but it is set to one to inserted but not committed data
        self.cursor.execute("CREATE TABLE IF NOT EXISTS objects (id INTEGER PRIMARY KEY, tp TEXT, to_insert INTEGER)")
        # deletes and updates are set on the side until commit
        # inserts are directly written to the database
        self.to_delete : List[EdgeID] = []
        self.to_update : Dict[EdgeID, Dict[str, MultiSetVal]]= {}

        self.cursor.execute("CREATE TABLE IF NOT EXISTS next_id_to_return_gen (id INTEGER PRIMARY KEY)")
        self.cursor.execute("SELECT id FROM next_id_to_return_gen LIMIT 1")
        next_id_row = self.cursor.fetchone()
        if next_id_row is not None:
            self.next_id_to_return = next_id_row[0]
        else:
            self.cursor.execute("INSERT INTO next_id_to_return_gen (id) VALUES (?)", (1,))
            self.next_id_to_return = 1
        self.conn.commit() # I am not sure whether this is needed


    def dump_state(self) -> object:
        save_point_name = "savepoint_" + str(self.next_id())
        self.conn.execute("SAVEPOINT " + save_point_name)
        return {
            "savepoint": save_point_name,
            "to_delete": copy.copy(self.to_delete),
            "to_update": copy.copy(self.to_update),
        }

    def restore_state(self, dumped_state) -> None:
        self.to_delete = copy.copy(dumped_state["to_delete"])
        self.to_update = copy.copy(dumped_state["to_update"])
        self.conn.execute("ROLLBACK TO " + dumped_state["savepoint"])

    def query_ids_for_a_type(self, tp: str) -> List[EdgeID]:
        # to insert is set to 1 if it is to insert, set to zero usually
        # check if type exists
        self.cursor.execute("SELECT tp FROM types WHERE type=?", (tp,))
        tp_row = self.cursor.fetchone()
        if tp_row is None:
            self.cursor.execute("INSERT INTO types (tp) VALUES (?)", (tp,))
            # create table if not exists
            self.cursor.execute(f"CREATE TABLE IF NOT EXISTS {tp} (id INTEGER PRIMARY KEY)")
        self.cursor.execute(f"SELECT id FROM {tp} WHERE to_insert=0")
        return [row[0] for row in self.cursor.fetchall()]

    def get_props_for_id(self, id: EdgeID) -> Dict[str, MultiSetVal]:
        self.cursor.execute("SELECT tp FROM objects WHERE id=?", (id,))
        tp_row = self.cursor.fetchone()
        if tp_row is None:
            

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
    

    


    


def schema_and_db_from_sqlite(sdl_file_name, sqlite_file_name):
    # Connect to the SQLite database
    conn = sqlite3.connect(sqlite_file_name)
    c = conn.cursor()

    # Check if sdl_schema table exists
    c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='sdl_schema'")
    table_exists = c.fetchone()
    db_sdl = None
    if table_exists:
        # Retrieve the pickled object from the table
        c.execute("SELECT content FROM sdl_schema LIMIT 1")
        db_sdl_row = c.fetchone()
        if db_sdl_row:
            db_sdl = db_sdl_row[0]
    else:
        # Create sdl_schema table
        c.execute("CREATE TABLE sdl_schema (content TEXT)")

    if db_sdl is None:

        if sdl_file_name is None:
            content = "" #empty schema
        else:
            with open(sdl_file_name, "r") as file:
                content = file.read()

        # Read and store the content into sdl_schema table
        c.execute("INSERT INTO sdl_schema (content) VALUES (?)", (content,))
        dbschema = schema_from_sdl_defs(content)

        # Commit the changes
        conn.commit()
    else:

        if sdl_file_name is not None:
            # Read the content from sdl_file_name
            with open(sdl_file_name, "r") as file:
                sdl_content = file.read()

            # Compare content and abort if they differ
            if db_sdl != sdl_content:
                raise ValueError("Passed in SDL file differs from SQLite Schema.", sdl_content, db_sdl)

        dbschema = schema_from_sdl_defs(db_sdl)

    # Unpickle the objects
    # dbschema_val = pickle.loads(dbschema_bytes)
    # dbschema = sqlite_dbschema(dbschema_val)

    db = SQLiteEdgeDatabase(conn)
    return dbschema, db


