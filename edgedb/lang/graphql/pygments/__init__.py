from pygments.lexer import RegexLexer, include
from pygments import token


__all__ = ['GraphQLLexer']


class GraphQLLexer(RegexLexer):
    name = 'GraphQL'
    aliases = ['graphql']
    filenames = ['*.gql', '*.graphql']

    tokens = {
        'root': [
            include('comments'),
            (r'@\w+', token.Name.Decorator),
            (r'\$\w+', token.Name.Variable),
            include('keywords'),
            include('numbers'),
            include('strings'),
            (r'\b(true|false|null)\b', token.Keyword.Constant),
            (r'\s+', token.Text),
            (r'\w+', token.Text),
            (r'.', token.Text),
        ],
        'comments': [
            (r'#.*?\n', token.Comment.Singleline),
        ],
        'keywords': [
            (r'''(?x)
                \b(
                    query | mutation
                )\b
            ''', token.Keyword.Reserved),
        ],
        'strings': [
            (r'''(?x)
                " [^\n]*? (?<!\\)"
            ''', token.String.Double),
        ],
        'numbers': [
            (r'''(?x)
                (?: \d+ (?:\.\d*)?
                    |
                    \. \d+
                ) (?:[eE](?:[+\-])?[0-9]+)
            ''', token.Number.Float),
            (r'''(?x)
                (?: \d+\.(?!\.)\d*
                    |
                    \.\d+)
            ''', token.Number.Float),
            (r'\d+', token.Number.Integer),
        ],
    }
