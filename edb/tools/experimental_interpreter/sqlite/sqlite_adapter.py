
import sqlite3
import pickle


# ChatGPT3.5
def pickle_to_sqlite(dbschema, db, sqlite_file_name):
    # Pickle the objects
    dbschema_bytes = pickle.dumps(dbschema)
    db_bytes = pickle.dumps(db)

    # Connect to the SQLite database
    conn = sqlite3.connect(sqlite_file_name)
    c = conn.cursor()

    # Create the table to store the pickled objects
    c.execute('''DROP TABLE IF EXISTS pickled_objects''')
    c.execute('''CREATE TABLE IF NOT EXISTS pickled_objects
                 (name TEXT PRIMARY KEY, obj BLOB)''')

    # Insert the pickled objects into the table
    c.execute("INSERT INTO pickled_objects VALUES (?, ?)",
              ("dbschema", sqlite3.Binary(dbschema_bytes)))
    c.execute("INSERT INTO pickled_objects VALUES (?, ?)",
              ("db", sqlite3.Binary(db_bytes)))

    # Commit and close the connection
    conn.commit()
    conn.close()


def unpickle_from_sqlite(sqlite_file_name):
    # Connect to the SQLite database
    conn = sqlite3.connect(sqlite_file_name)
    c = conn.cursor()

    # Retrieve the pickled objects from the table
    c.execute("SELECT obj FROM pickled_objects WHERE name = 'dbschema'")
    dbschema_bytes = c.fetchone()[0]
    c.execute("SELECT obj FROM pickled_objects WHERE name = 'db'")
    db_bytes = c.fetchone()[0]

    # Unpickle the objects
    dbschema = pickle.loads(dbschema_bytes)
    db = pickle.loads(db_bytes)

    # Close the connection and return the objects
    conn.close()
    return dbschema, db


