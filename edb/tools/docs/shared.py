from docutils import nodes as d_nodes
from docutils import utils as d_utils
from docutils.parsers.rst import roles as d_roles


class EdgeSphinxExtensionError(Exception):
    pass


class DirectiveParseError(EdgeSphinxExtensionError):

    def __init__(self, directive, msg, *, cause=None):
        fn, lineno = directive.state_machine.get_source_and_line()
        msg = f'{msg} in {fn}:{lineno}'
        if cause is not None:
            msg = f'{msg}\nCause: {type(cause).__name__}\n{cause}'
        super().__init__(msg)


class DomainError(EdgeSphinxExtensionError):
    pass


class InlineCodeRole:

    def __init__(self, lang):
        self.lang = lang

    def __call__(self, role, rawtext, text, lineno, inliner,
                 options={}, content=[]):
        d_roles.set_classes(options)
        node = d_nodes.literal(rawtext, d_utils.unescape(text), **options)
        node['eql-lang'] = self.lang
        return [node], []
