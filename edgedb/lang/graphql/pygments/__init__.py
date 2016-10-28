from pygments.lexer import RegexLexer, bygroups, include
from pygments.token import *


__all__ = ['GraphQLLexer']


class GraphQLLexer(RegexLexer):
    name = 'GraphQL'
    aliases = ['graphql']
    filenames = ['*.gql', '*.graphql']

    tokens = {
        'root': [
            include('comments'),
            (r'@\w+', Name.Decorator),
            (r'\$\w+', Name.Variable),
            include('keywords'),
            include('numbers'),
            include('strings'),
            (r'\b(true|false|null)\b', Keyword.Constant),
            (r'\s+', Text),
            (r'\w+', Text),
            (r'.', Text),
        ],
        'comments': [
            (r'#.*?\n', Comment.Singleline),
        ],
        'keywords': [
            (r'''(?x)
                \b(
                    query | mutation
                )\b
            ''', Keyword.Reserved),
        ],
        'strings': [
            (r'''(?x)
                " [^\n]*? (?<!\\)"
            ''', String.Double),
        ],
        'numbers': [
            (r'''(?x)
                (?: \d+ (?:\.\d*)?
                    |
                    \. \d+
                ) (?:[eE](?:[+\-])?[0-9]+)
            ''', Number.Float),
            (r'''(?x)
                (?: \d+\.(?!\.)\d*
                    |
                    \.\d+)
            ''', Number.Float),
            (r'\d+', Number.Integer),
        ],
    }
