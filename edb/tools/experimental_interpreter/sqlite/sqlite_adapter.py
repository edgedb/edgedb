
import sqlite3
import pickle

from ..data.data_ops import DB, DBSchema, MultiSetVal, ResultTp, ObjectTp
from typing import *
from ..basis.built_ins import all_builtin_funcs
from ..db_interface import EdgeDatabaseInterface
from ..elab_schema import schema_from_sdl_file, schema_from_sdl_defs

# def sqlite_dbschema(sqlite_dbschema: Dict[str, ObjectTp]) -> DBSchema:
#     return DBSchema(sqlite_dbschema, all_builtin_funcs)


class SQLiteEdgeDatabase(EdgeDatabaseInterface):

    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn
        self.cursor = conn.cursor()


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


