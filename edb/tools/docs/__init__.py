from docutils import nodes as d_nodes
from sphinx import transforms as s_transforms

from . import eql
from . import eschema
from . import graphql
from . import shared


class ProhibitedNodeTransform(s_transforms.SphinxTransform):

    default_priority = 1  # before ReferencesResolver

    def apply(self):
        bqs = list(self.document.traverse(d_nodes.block_quote))
        if bqs:
            raise shared.EdgeSphinxExtensionError(
                f'blockquote found: {bqs[0].asdom().toxml()!r}')

        trs = list(self.document.traverse(d_nodes.title_reference))
        if trs:
            raise shared.EdgeSphinxExtensionError(
                f'title reference (single backticks quote) found: '
                f'{trs[0].asdom().toxml()!r}; perhaps you wanted to use '
                f'double backticks?')


def setup(app):
    eql.setup_domain(app)
    eschema.setup_domain(app)
    graphql.setup_domain(app)

    app.add_transform(ProhibitedNodeTransform)
