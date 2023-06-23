
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
@click.option("--trace-to-file", type=str, required=False)
@click.option("--sqlite-file", type=str, required=False)
@click.option("-v", "--verbose", default=False, required=False, is_flag=True)
def interperter_entry(
        *, init_sdl_file=None, init_ql_file=None, verbose=False,
        trace_to_file=None, sqlite_file=None) -> None:
    """ Run the experimental interpreter for EdgeQL """
    repl(init_sdl_file=init_sdl_file,
         init_ql_file=init_ql_file,
         debug_print=verbose,
         trace_to_file_path=trace_to_file,
         sqlite_file=sqlite_file,
         )
