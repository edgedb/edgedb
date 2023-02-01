
from edb.tools.edb import edbcommands
import click

from .new_interpreter import repl


@edbcommands.command('exp-interp', context_settings={"help_option_names": ["-h", "--help"]})
@click.option("--init-ql-file", type=str, required=False)
@click.option("-v", "--verbose", default=False, required=False, is_flag=True)
def interperter_entry(*, init_ql_file = None, verbose = False) -> None:
    """ Run the experimental interpreter for EdgeQL """
    repl(init_ql_file=init_ql_file, debug_print=verbose)