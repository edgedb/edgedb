from edb.schema.pygments import EdgeSchemaLexer

from sphinx import domains as s_domains
from sphinx.directives import code as s_code

from . import shared


class EschemaSynopsisDirective(s_code.CodeBlock):

    has_content = True
    optional_arguments = 0
    required_arguments = 0
    option_spec = {}

    def run(self):
        self.arguments = ['eschema-synopsis']
        return super().run()


class EschemaDomain(s_domains.Domain):

    name = "eschema"
    label = "EdgeDB Schema"

    directives = {
        'synopsis': EschemaSynopsisDirective,
    }


def setup_domain(app):
    app.add_lexer("eschema", EdgeSchemaLexer())
    app.add_lexer("eschema-synopsis", EdgeSchemaLexer())

    app.add_role(
        'eschema:synopsis',
        shared.InlineCodeRole('eschema-synopsis'))

    app.add_domain(EschemaDomain)
