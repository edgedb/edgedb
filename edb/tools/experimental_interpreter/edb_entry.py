import click
from edb.tools.edb import edbcommands

from .new_interpreter import repl

import os


@edbcommands.command(
    'exp-interp', context_settings={"help_option_names": ["-h", "--help"]}
)
@click.option(
    "--init-ql-file",
    type=str,
    required=False,
    help="run this edgeql file on startup",
)
@click.option(
    "--next-ql-file",
    type=str,
    required=False,
    help="run this edgeql file after startup",
)
@click.option(
    "--init-sdl-file",
    type=str,
    required=False,
    help="initialize schema to be this file, schema" "should not be place",
)
@click.option(
    "--library-ddl-files",
    '-l',
    multiple=True,
    type=str,
    help="standard library files",
)
@click.option("--trace-to-file", type=str, required=False)
@click.option("--sqlite-file", type=str, required=False)
@click.option(
    "--test",
    type=str,
    required=False,
    help="""specify a single name.
Will search the test schema directory for esdl file containing the case
insensitive specified esdl file.
Will also load the corresponding ql file.
Will turn on trace-to-file to a default html file.
Will populate next ql file if it exists.
              """,
)
@click.option(
    "--no-setup",
    is_flag=True,
    default=False,
    required=False,
    help="""--test only, do not include a setup file. """,
)
@click.option(
    "-y", "--skip-test-confirm", default=False, required=False, is_flag=True
)
@click.option("-v", "--verbose", default=False, required=False, is_flag=True)
def interperter_entry(
    *,
    init_sdl_file=None,
    next_ql_file=None,
    init_ql_file=None,
    verbose=False,
    trace_to_file=None,
    sqlite_file=None,
    library_ddl_files=None,
    test=None,
    no_setup=False,
    skip_test_confirm=False,
) -> None:

    if test:
        schemas_dir = os.path.join(
            os.path.dirname(__file__), '..', '..', '..', 'tests', 'schemas'
        )
        print("Schemas are in ", schemas_dir)
        # search root of test schemas for esdl file
        candidate_files = []
        failed_files = []
        for root, _, files in os.walk(schemas_dir):
            for file in files:
                if file.endswith('.esdl'):
                    if test.lower() in file.lower():
                        candidate_files.append(os.path.join(root, file))
                    else:
                        failed_files.append(os.path.join(root, file))

        if len(candidate_files) == 0:
            print(f'Could not find any esdl files containing {test}')
            print('Found these files:', failed_files)
            return
        if len(candidate_files) > 1:
            print(f'Found multiple esdl files containing {test}:')
            for file in candidate_files:
                print(file)
            return
        if init_sdl_file is None:
            init_sdl_file = candidate_files[0]

        ql_file = candidate_files[0].replace('.esdl', '_setup.edgeql')
        if init_ql_file is None:
            init_ql_file = ql_file

        if next_ql_file is None and os.path.exists(
            "temp_current_testing.edgeql"
        ):
            next_ql_file = "temp_current_testing.edgeql"

        if no_setup:
            init_ql_file = None
            next_ql_file = None

        if trace_to_file is None:
            trace_to_file = "temp_debug.html"

        if verbose is False:
            verbose = True

        # print all options
        print(f'Running test {test} with options:')
        print(
            f'init_sdl_file: '
            + (
                init_sdl_file
                if not init_sdl_file.startswith(schemas_dir)
                else "<s_dir>" + init_sdl_file[len(schemas_dir) :]
            )
        )
        if init_ql_file:
            print(
                'init_ql_file: '
                + (
                    init_ql_file
                    if not init_ql_file.startswith(schemas_dir)
                    else "<s_dir>" + init_ql_file[len(schemas_dir) :]
                )
            )
        else:
            print(f'init_ql_file: None')
        print(f'next_ql_file: {next_ql_file}')
        print(f'trace_to_file: {trace_to_file}')
        print(f'verbose: {verbose}')

        if not skip_test_confirm:
            if input('Continue? (Y/n)') == 'n':
                return

    """ Run the experimental interpreter for EdgeQL """
    repl(
        init_sdl_file=init_sdl_file,
        init_ql_file=init_ql_file,
        next_ql_file=next_ql_file,
        library_ddl_files=library_ddl_files,
        debug_print=verbose,
        trace_to_file_path=trace_to_file,
        sqlite_file=sqlite_file,
    )
