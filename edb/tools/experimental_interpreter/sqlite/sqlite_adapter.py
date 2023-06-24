
import sqlite3
import pickle
from typing import List

from ..db_interface import EdgeID

from ..data.data_ops import DB, DBSchema, MultiSetVal, ResultTp, ObjectTp, IntVal, StringVal, LinkPropVal
from ..data.data_ops import *
from typing import *
from ..basis.built_ins import all_builtin_funcs
from ..db_interface import EdgeDatabaseInterface
from ..elab_schema import schema_from_sdl_file, schema_from_sdl_defs

import copy

# def sqlite_dbschema(sqlite_dbschema: Dict[str, ObjectTp]) -> DBSchema:
#     return DBSchema(sqlite_dbschema, all_builtin_funcs)


def compute_projection(cursor: sqlite3.Cursor,
                       id : EdgeID,
                       tp : str,
                       prop: str
                        )  -> MultiSetVal:

    table_name = f"{tp}_{prop}"
    cursor.execute(f"SELECT property_type FROM property_types WHERE table_name=?", (table_name,))
    property_tp_row  = cursor.fetchone()
    if property_tp_row is None:
        raise ValueError(f"Table {table_name} not found in database")
    else:
        property_tp = property_tp_row[0]
        if property_tp == "INT":
            cursor.execute(f"SELECT int_value FROM {table_name} WHERE id=?", (id,))
            return MultiSetVal([IntVal(row[0]) for row in cursor.fetchall()])
        elif property_tp == "STRING":
            cursor.execute(f"SELECT string_value FROM {table_name} WHERE id=?", (id,))
            return MultiSetVal([StringVal(row[0]) for row in cursor.fetchall()])
        elif property_tp == "LINK":
            cursor.execute(f"SELECT link_value FROM {table_name} WHERE id=?", (id,))
            targets : List[RefVal]= []
            for link_target_id_row in cursor.fetchall():
                link_target_id = link_target_id_row[0]
                link_source_id = id
                link_props : Dict[Label, Tuple[Marker, MultiSetVal]]= {}

                # search all possible link properties

                prefix = tp + "_" + prop + "_%"
                cursor.execute("SELECT table_name FROM property_types WHERE table_name LIKE ?", (prefix,))

                for link_property_table_name in [row[0] for row in cursor.fetchall()]:
                    link_property_name = link_property_table_name.split("_")[2]

                    cursor.execute(f"SELECT property_type FROM property_types WHERE table_name=?", (link_property_table_name,))
                    link_property_tp_row  = cursor.fetchone()
                    if link_property_tp_row is None:
                        raise ValueError(f"Table {link_property_table_name} not found in property_types")
                    else:
                        link_property_tp = link_property_tp_row[0]
                        if link_property_tp == "INT":
                            cursor.execute(f"SELECT int_value FROM {link_property_table_name} WHERE source_id=? AND target_id=?", 
                                            (link_source_id, link_target_id))
                            link_props[StrLabel(link_property_name)] = (Visible(), MultiSetVal([IntVal(row[0]) for row in cursor.fetchall()]))
                        elif link_property_tp == "STRING":
                            cursor.execute(f"SELECT string_value FROM {link_property_table_name} WHERE source_id=? AND target_id=?",
                                                (link_source_id, link_target_id))
                            link_props[StrLabel(link_property_name)] = (Visible(), MultiSetVal([StrVal(row[0]) for row in cursor.fetchall()]))

                targets.append(RefVal(link_target_id, ObjectVal(link_props)))
            return MultiSetVal(targets)
        else:
            raise ValueError(f"Unknown property type {property_tp}")

class SQLiteEdgeDatabase(EdgeDatabaseInterface):

    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn
        self.cursor = conn.cursor()
        # to_insert is set to zero usually, but it is set to one to inserted but not committed data
        self.cursor.execute("CREATE TABLE IF NOT EXISTS objects (id INTEGER PRIMARY KEY, tp TEXT NOT NULL, to_insert INTEGER NOT NULL)")
        # property_type can be "INT", "STRING", "LINK"
        self.cursor.execute("CREATE TABLE IF NOT EXISTS property_types (table_name TEXT PRIMARY KEY, property_type TEXT)")
        
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
        self.cursor.execute(f"SELECT id FROM objects WHERE to_insert=0")
        return [row[0] for row in self.cursor.fetchall()]

    def get_props_for_id(self, id: EdgeID) -> Dict[str, MultiSetVal]:
        self.cursor.execute("SELECT tp, to_update FROM objects WHERE id=?", (id,))
        tp_row = self.cursor.fetchone()
        if tp_row is None:
            raise ValueError(f"ID {id} not found in database")
        else:
            tp, to_insert = tp_row
            if to_insert == 1 and id in self.to_update.keys():
                return self.to_update[id]
            else:
                # select all table names that are potential candidates
                prefix = tp + "_%"
                self.cursor.execute("SELECT table_name FROM property_types WHERE table_name LIKE ?", (prefix,))
                table_names = [row[0] for row in self.cursor.fetchall()]
                result = {}
                # properties are stored in table with tp_property
                # link properties are stored in table with tp_property_linkprop
                for table_name in table_names:
                    if table_name.count("_")>1:
                        continue
                    property_name = table_name.split("_")[1]
                    result[property_name] = self.project(id, property_name)
                return result


                
    def get_type_for_an_id(self, id: EdgeID) -> str:
        self.cursor.execute("SELECT tp FROM objects WHERE id=?", (id,))
        tp_row = self.cursor.fetchone()
        if tp_row is None:
            raise ValueError(f"ID {id} not found in database")
        else:
            return tp_row[0]
    
    def is_projectable(self, id: EdgeID, prop: str) -> bool:
        tp_name = self.get_type_for_an_id(id)
        self.cursor.execute("SELECT property_type FROM property_types WHERE table_name=?", 
                                (tp_name + "_" + prop,))
        property_type_row = self.cursor.fetchone()
        if property_type_row is None:
            return False
        else:
            return True
    
    def project(self, id: EdgeID, prop: str) -> MultiSetVal:
        tp_name = self.get_type_for_an_id(id)
        return compute_projection(self.cursor, id, tp_name, prop)

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
        ## XXX: This is not thread safe
        self.cursor.execute("SELECT id FROM next_id_to_return_gen LIMIT 1")
        id_row = self.cursor.fetchone()
        if id_row is None:
            raise ValueError("Cannot fetch next id, check initialization")
        id = id_row[0]
        self.cursor.execute("UPDATE next_id_to_return_gen SET id = id + 1")
        return id
    
    def close(self) -> None:
        self.conn.close()
    

    


    


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


