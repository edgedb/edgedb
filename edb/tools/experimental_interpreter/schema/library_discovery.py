from ..data import data_ops as e
from typing import List
from ..helper_funcs import parse_ddl
from .ddl_processing import process_ddls
import os


def process_edgeql_file(schema: e.DBSchema, path: str) -> None:
    """
    Process an edgeql file as ddl.
    """

    with open(path) as f:
        content = f.read()
        ddls = parse_ddl(content)
        process_ddls(schema, ddls)


def add_ddl_library(schema: e.DBSchema, libpaths: List[str]) -> None:
    """
    Add a library to the schema.

    Given a list of library paths,
    If library is a edgeql file, process the edgeql file as ddl.
    If library is a directory,
    process all edgeql files in the top level of the directory
    in a lexicographical order.
    """

    for libpath in libpaths:
        if os.path.isdir(libpath):
            for filename in sorted(os.listdir(libpath)):
                if filename.startswith("_"):
                    continue
                if filename.endswith(".edgeql"):
                    process_edgeql_file(
                        schema, os.path.join(libpath, filename)
                    )
        elif libpath.endswith(".edgeql"):
            process_edgeql_file(schema, libpath)
        else:
            raise ValueError(f"Invalid library path {libpath}")
