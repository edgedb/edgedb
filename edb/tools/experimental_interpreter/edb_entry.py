
import click
from edb.tools.edb import edbcommands

from .new_interpreter import repl


@edbcommands.command('exp-interp',
                     context_settings={
                         "help_option_names":
                         ["-h", "--help"]})
@click.option("--init-ql-file", type=str, required=False,
              help="run this edgeql file on startup")
@click.option("--init-sdl-file", type=str, required=False,
              help="initialize schema to be this file, schema"
              "should not be place")
@click.option("--library-ddl-files", '-l', multiple=True, type=str, 
              help="standard library files")
@click.option("--trace-to-file", type=str, required=False)
@click.option("--sqlite-file", type=str, required=False)
@click.option("--skip-type-checking", default=False, required=False, is_flag=True)
@click.option("-v", "--verbose", default=False, required=False, is_flag=True)
def interperter_entry(
        *, init_sdl_file=None, init_ql_file=None, verbose=False,
        trace_to_file=None, sqlite_file=None,
        library_ddl_files=None,
        skip_type_checking=False) -> None:
    """ Run the experimental interpreter for EdgeQL """
    repl(init_sdl_file=init_sdl_file,
         init_ql_file=init_ql_file,
         library_ddl_files=library_ddl_files,
         debug_print=verbose,
         trace_to_file_path=trace_to_file,
         sqlite_file=sqlite_file,
         skip_type_checking=skip_type_checking,
         )
