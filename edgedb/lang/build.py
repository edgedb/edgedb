import os.path

from distutils.command import build


class build(build.build):
    def _compile_parsers(self):
        import parsing

        import edgedb
        import edgedb.server.main

        edgedb.server.main.init_import_system()

        import edgedb.lang.edgeql.parser.grammar.single as edgeql_spec
        import edgedb.lang.edgeql.parser.grammar.block as edgeql_spec2
        import edgedb.server.pgsql.parser.pgsql as pgsql_spec
        import edgedb.lang.schema.parser.grammar.declarations as schema_spec
        import edgedb.lang.graphql.parser.grammar.document as graphql_spec

        base_path = os.path.dirname(
            os.path.dirname(os.path.dirname(__file__)))

        for spec in (edgeql_spec, edgeql_spec2, pgsql_spec,
                     schema_spec, graphql_spec):
            subpath = os.path.dirname(spec.__file__)[len(base_path) + 1:]
            cache = os.path.join(self.build_lib,
                                 subpath,
                                 spec.__name__.rpartition('.')[2] + '.pickle')
            parsing.Spec(spec, pickleFile=cache, verbose=True)

    def run(self, *args, **kwargs):
        super().run(*args, **kwargs)
        self._compile_parsers()
