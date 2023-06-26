
import sqlite3
import pickle
from typing import List

from ..db_interface import EdgeID

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
            return MultiSetVal([StrVal(row[0]) for row in cursor.fetchall()])
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

    def __init__(self, conn: sqlite3.Connection, schema: DBSchema):
        self.conn = conn
        self.schema = schema
        self.cursor = conn.cursor()
        # to_insert is set to zero usually, but it is set to one to inserted but not committed data
        self.cursor.execute("CREATE TABLE IF NOT EXISTS objects (id INTEGER PRIMARY KEY, tp TEXT NOT NULL, to_insert INTEGER NOT NULL)")
        self.conn.commit()
        self.cursor.execute("CREATE INDEX IF NOT EXISTS objects_idx1 ON objects (id)")
        self.cursor.execute("CREATE INDEX IF NOT EXISTS objects_idx2 ON objects (tp)")
        # property_type can be "INT", "STRING", "LINK"
        self.cursor.execute("CREATE TABLE IF NOT EXISTS property_types (table_name TEXT PRIMARY KEY, property_type TEXT NOT NULL)")
        self.cursor.execute("CREATE INDEX IF NOT EXISTS property_types_idx1 ON property_types (table_name)")
        self.cursor.execute("CREATE INDEX IF NOT EXISTS property_types_idx2 ON property_types (property_type)")
        
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
            self.cursor.execute("INSERT INTO next_id_to_return_gen (id) VALUES (?)", (101,))
        self.conn.commit() # I am not sure whether this is needed


    def dump_state(self) -> object:
        self.conn.commit()

        # Create a drop and restore routine by first dropping all tables
        self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = self.cursor.fetchall()
        drop_statements = [f"DROP TABLE IF EXISTS {table[0]};" for table in tables]

        # Combine the DROP TABLE statements with the dump script
        dump_script = '\n'.join(drop_statements) + '\n\n' + '\n'.join(self.conn.iterdump())

        return {
            # "savepoint": save_point_name,
            "dump": dump_script,
            "to_delete": copy.copy(self.to_delete),
            "to_update": copy.copy(self.to_update),
        }

    def restore_state(self, dumped_state) -> None:
        self.to_delete = copy.copy(dumped_state["to_delete"])
        self.to_update = copy.copy(dumped_state["to_update"])
        self.conn.executescript(dumped_state["dump"])
        # self.conn.execute("ROLLBACK TO SAVEPOINT " + dumped_state["savepoint"])

    def query_ids_for_a_type(self, tp: str) -> List[EdgeID]:
        # to insert is set to 1 if it is to insert, set to zero usually
        # check if type exists
        self.cursor.execute(f"SELECT id FROM objects WHERE to_insert=0 AND tp=?", (tp,))
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

        # retrieve all link tables:
        self.cursor.execute("SELECT table_name FROM property_types WHERE property_type='LINK' AND table_name LIKE ?", (f"%_{prop}",))
        link_tables = [row[0] for row in self.cursor.fetchall() if row[0].count("_")==1] # do not allow link property table names

        result : List[Val] = []
        for link_table in link_tables:
            self.cursor.execute(f"SELECT id, link_value FROM {link_table} WHERE link_value IN ({','.join(['?']*len(subject_ids))})", subject_ids)
            # retrive link proerty for each subject_id
            for (source_id, target_id) in [row[0] for row in self.cursor.fetchall()]:
                # fetch the link property table names
                self.cursor.execute("SELECT table_name, property_type FROM property_types WHERE property_type='LINK' AND table_name LIKE ?", (f"{link_table}_%",))
                link_props : Dict[Label, Tuple[Marker, MultiSetVal]]= {}
                for (link_property_table, link_property_type) in [row for row in self.cursor.fetchall() if row[0].count("_")==2]:
                    property_name = link_property_table.split("_")[2]
                    match link_property_type:
                        case "LINK":
                            raise ValueError("Link property cannot be a link property")
                        case "INT":
                            self.cursor.execute(f"SELECT int_value FROM {link_property_table} WHERE source_id=? AND target_id=?", (source_id, target_id))
                            link_props[StrLabel(property_name)] = (Visible(), MultiSetVal([IntVal(row[0]) for row in self.cursor.fetchall()]))
                        case "STRING":
                            self.cursor.execute(f"SELECT string_value FROM {link_property_table} WHERE source_id=? AND target_id=?", (source_id, target_id))
                            link_props[StrLabel(property_name)] = (Visible(), MultiSetVal([StrVal(row[0]) for row in self.cursor.fetchall()]))
                        case _:
                            raise ValueError(f"Unknown property type {link_property_type}")
                result.append(LinkPropVal(refid=source_id, linkprop=ObjectVal(link_props)))
        return MultiSetVal(result)

    def delete(self, id: EdgeID) -> None:
        self.to_delete.append(id)
    
    # this is a smarter way to figure out a type of an expression
    # by first looking at the passed in value, if the val is empty,
    # it tryies to retrieve the type from the schema
    def get_type_for_proprty(self, tp : str, prop : str, lp_prop : Optional[str], val : MultiSetVal) -> str:
        # query the type from the database
        if lp_prop is None:
            self.cursor.execute("SELECT property_type FROM property_types WHERE table_name=?", (tp + "_" + prop,))
        else:
            self.cursor.execute("SELECT property_type FROM property_types WHERE table_name=?", (tp + "_" + prop + "_" + lp_prop,))
        property_type_row = self.cursor.fetchone()
        if property_type_row is not None:
            return property_type_row[0]

        # retrive the type from the values and tp, and create appropriate table
        result_tp = None
        if len(val.vals) == 0:
            schema = self.get_schema()
            base_tp = schema.val[tp].val[prop].tp
            if lp_prop is None:
                match base_tp:
                    case StrTp():
                        result_tp =  "STRING"
                    case IntTp():
                        result_tp =  "INT"
                    case VarTp(_):
                        result_tp =  "LINK"
                    case LinkPropTp(_):
                        result_tp =  "LINK"
                    case _:
                        raise ValueError(f"Unknown type {base_tp}")
            else:
                match base_tp:
                    case LinkPropTp(subject, link_props):
                        match link_props.val[lp_prop].tp:
                            case StrTp():
                                result_tp =  "STRING"
                            case IntTp():
                                result_tp =  "INT"
                            case _:
                                raise ValueError(f"Unknown type {link_props.val[lp_prop]}")
        else:
            if all(isinstance(v, StrVal) for v in val.vals):
                result_tp =  "STRING"
            elif all(isinstance(v, IntVal) for v in val.vals):
                result_tp =  "INT"
            elif all(isinstance(v, LinkPropVal) for v in val.vals):
                result_tp =  "LINK"
            else:
                raise ValueError(f"Unknown type for {val}")

        assert result_tp is not None, "should be assigned a type"

        # create the table
        if lp_prop is None:
            match result_tp:
                case "STRING":
                    self.cursor.execute(f"CREATE TABLE IF NOT EXISTS {tp}_{prop} (id INTEGER, string_value TEXT)") 
                    self.cursor.execute(f"CREATE INDEX IF NOT EXISTS {tp}_{prop}_idx1 ON {tp}_{prop} (id)")
                    # IF NOT EXISTS serves to rule out database anormalies (maybe previosu transaction was cutout in the middle)
                case "INT":
                    self.cursor.execute(f"CREATE TABLE IF NOT EXISTS {tp}_{prop} (id INTEGER, int_value INTEGER)")
                    self.cursor.execute(f"CREATE INDEX IF NOT EXISTS {tp}_{prop}_idx1 ON {tp}_{prop} (id)")
                case "LINK":
                    self.cursor.execute(f"CREATE TABLE IF NOT EXISTS {tp}_{prop} (id INTEGER, link_value INTEGER)")
                    self.cursor.execute(f"CREATE INDEX IF NOT EXISTS {tp}_{prop}_idx1 ON {tp}_{prop} (id)")
                case _:
                    raise ValueError(f"Unknown type {result_tp}")
        else:
            match result_tp:
                case "STRING":
                    self.cursor.execute(f"CREATE TABLE IF NOT EXISTS {tp}_{prop}_{lp_prop} (source_id INTEGER, target_id INTEGER, string_value TEXT)")
                    self.cursor.execute(f"CREATE INDEX IF NOT EXISTS {tp}_{prop}_{lp_prop}_idx1 ON {tp}_{prop}_{lp_prop} (source_id, target_id)")
                case "INT":
                    self.cursor.execute(f"CREATE TABLE IF NOT EXISTS {tp}_{prop}_{lp_prop} (source_id INTEGER, target_id INTEGER, int_value INTEGER)")
                    self.cursor.execute(f"CREATE INDEX IF NOT EXISTS {tp}_{prop}_{lp_prop}_idx1 ON {tp}_{prop}_{lp_prop} (source_id, target_id)")
                case _:
                    raise ValueError(f"Unknown type {result_tp}")

        # insert into the property_types table
        if lp_prop is None:
            self.cursor.execute("INSERT INTO property_types (table_name, property_type) VALUES (?, ?)", (tp + "_" + prop, result_tp))
        else:
            self.cursor.execute("INSERT INTO property_types (table_name, property_type) VALUES (?, ?)", (tp + "_" + prop + "_" + lp_prop, result_tp))

        return result_tp




    def insert(self, tp: str, props : Dict[str, MultiSetVal]) -> EdgeID:
        id = self.next_id()
        # insert the object into the objects table
        self.cursor.execute("INSERT INTO objects (id, tp, to_insert) VALUES (?, ?, ?)", (id, tp, 1))

        for (prop, val) in props.items():

            target_table_tp = self.get_type_for_proprty(tp, prop, None, val)
            
            # insert the property value
            for v in val.vals:
                match v:
                    case StrVal(s):
                        self.cursor.execute(f"INSERT INTO {tp}_{prop} (id, string_value) VALUES (?, ?)", (id, s))
                    case IntVal(i):
                        self.cursor.execute(f"INSERT INTO {tp}_{prop} (id, int_value) VALUES (?, ?)", (id, i))
                    case LinkPropVal(refid, linkprop):
                        # insert the link property table if it does not exist
                        for (lp_name, (_, val)) in linkprop.val.items():
                            lp_tp = self.get_type_for_proprty(tp, prop, lp_name.label, val)

                            # insert the link property value
                            for v in val.vals:
                                match v:
                                    case StrVal(s):
                                        self.cursor.execute(f"INSERT INTO {tp}_{prop}_{lp_name.label} (source_id, target_id, string_value) VALUES (?, ?, ?)", (id, refid, v))
                                    case IntVal(i):
                                        self.cursor.execute(f"INSERT INTO {tp}_{prop}_{lp_name.label} (source_id, target_id, int_value) VALUES (?, ?, ?)", (id, refid, v))
                                    case _:
                                        raise ValueError(f"Unknown value {v}")
                    case _:
                        raise ValueError(f"Unknown type {v}")






        
        return id

    def update(self, id: EdgeID, props : Dict[str, MultiSetVal]) -> None:
        self.to_update[id] = props
    
    def commit_dml(self) -> None:
        # make insert permanent
        self.cursor.execute("UPDATE objects SET to_insert=0 WHERE to_insert=1")

        # perform the updates
        for (id, props) in self.to_update.items():
            tp = self.get_type_for_an_id(id)
            for (prop_name, prop_val) in props.items():
                prop_tp_str = self.get_type_for_proprty(tp, prop_name, None, prop_val)
                match prop_tp_str:
                    case "STRING":
                        self.cursor.execute(f"DELETE FROM {tp}_{prop_name} WHERE id=?", (id,))
                        for v in prop_val.vals:
                            assert isinstance(v, StrVal), "type mismatch"
                            self.cursor.execute(f"INSERT INTO {tp}_{prop_name} VALUES (?, ?)", (id, v.val))
                    case "INT":
                        self.cursor.execute(f"DELETE FROM {tp}_{prop_name} WHERE id=?", (id,))
                        for v in prop_val.vals:
                            assert isinstance(v, IntVal), "type mismatch"
                            self.cursor.execute(f"INSERT INTO {tp}_{prop_name} VALUES (?, ?)", (id, v.val))
                    case "LINK":
                        self.cursor.execute(f"DELETE FROM {tp}_{prop_name} WHERE id=?", (id,))
                        for v in prop_val.vals:
                            assert isinstance(v, LinkPropVal), "type mismatch"
                            self.cursor.execute(f"INSERT INTO {tp}_{prop_name} VALUES (?, ?)", (id, v.refid))
                            # replace all link properties
                            for (lp_name, (_, lp_val)) in v.linkprop.val.items():
                                lp_prop_tp_str = self.get_type_for_proprty(tp, prop_name, lp_name.label, lp_val)
                                self.cursor.execute(f"DELETE FROM {tp}_{prop_name}_{lp_name.label} WHERE source_id=? AND target_id=?", (id, v.refid))
                                match lp_prop_tp_str:
                                    case "STRING":
                                        for lp_v in lp_val.vals:
                                            assert isinstance(lp_v, StrVal), "type mismatch"
                                            self.cursor.execute(f"INSERT INTO {tp}_{prop_name}_{lp_name.label} (source_id, target_id, string_value) VALUES (?, ?, ?)", (id, v.refid, lp_v.val))
                                    case "INT":
                                        for lp_v in lp_val.vals:
                                            assert isinstance(lp_v, IntVal), "type mismatch"
                                            self.cursor.execute(f"INSERT INTO {tp}_{prop_name}_{lp_name.label} (source_id, target_id, int_value) VALUES (?, ?, ?)", (id, v.refid, lp_v.val))
                                    case _:
                                        raise ValueError(f"Unknown type {lp_v}")

        # delete objects
        for id in self.to_delete:
            self.cursor.execute("DELETE FROM objects WHERE id=?", (id,))
            tp = self.get_type_for_an_id(id)
            # delete all properties and links
            self.cursor.execute(f"SELECT table_name FROM property_types")
            for (table_name, ) in self.cursor.fetchall():
                if table_name.count("_") == 1:
                    # it is a property table
                    self.cursor.execute(f"DELETE FROM {table_name} WHERE id=?", (id,))
                elif table_name.count("_") == 2:
                    # it is a link proeprty table
                    self.cursor.execute(f"DELETE FROM {table_name} WHERE source_id=? OR target_id=?", (id, id,))

            
        self.conn.commit() # do not commit as this may interfere with ROLLBACK, todo: figure out how to do proper checkpointing
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
    

    


    


def schema_and_db_from_sqlite(sdl_file_content, sqlite_file_name):
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

        if sdl_file_content is None:
            content = "" #empty schema
        else:
            content = sdl_file_content

        # Read and store the content into sdl_schema table
        c.execute("INSERT INTO sdl_schema (content) VALUES (?)", (content,))
        dbschema = schema_from_sdl_defs(content)

        # Commit the changes
        conn.commit()
    else:

        sdl_content = sdl_file_content

        # Compare content and abort if they differ
        if db_sdl != sdl_content:
            raise ValueError("Passed in SDL file differs from SQLite Schema.", sdl_content, db_sdl)

        dbschema = schema_from_sdl_defs(db_sdl)

    # Unpickle the objects
    # dbschema_val = pickle.loads(dbschema_bytes)
    # dbschema = sqlite_dbschema(dbschema_val)

    db = SQLiteEdgeDatabase(conn, dbschema)
    return dbschema, db


