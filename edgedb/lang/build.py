import os.path

from distutils.command import build


class build(build.build):
    def _compile_parsers(self):
        import parsing

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
            cache_dir = os.path.join(self.build_lib, subpath)
            os.makedirs(cache_dir, exist_ok=True)
            cache = os.path.join(
                cache_dir, spec.__name__.rpartition('.')[2] + '.pickle')
            parsing.Spec(spec, pickleFile=cache, verbose=True)

    def run(self, *args, **kwargs):
        super().run(*args, **kwargs)
        self._compile_parsers()
