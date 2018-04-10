from pygments.lexer import RegexLexer, include
from pygments import token
from edgedb.lang.edgeql.parser.grammar import keywords


__all__ = ['EdgeQLLexer']


unreserved_keywords = keywords.unreserved_keywords - {'true', 'false'}
reserved_keywords = keywords.reserved_keywords - {
    '__self__', '__self__', '__type__'}


class EdgeQLLexer(RegexLexer):
    name = 'EdgeQL'
    aliases = ['eql', 'edgeql']
    filenames = ['*.eql', '*.edgeql']

    tokens = {
        'root': [
            include('comments'),
            include('keywords'),
            (r'@\w+', token.Name.Decorator),
            (r'\$[\w\d]+', token.Name.Variable),
            include('numbers'),
            include('strings'),
            (r'(?i)\b(true|false)\b', token.Keyword.Constant),
            (r'\s+', token.Text),
            (r'.', token.Text),
        ],
        'comments': [
            (r'#.*?\n', token.Comment.Singleline),
        ],
        'keywords': [
            (fr'''(?ix)
                \b(?<![:\.])(
                    {' | '.join(unreserved_keywords)}
                    |
                    {' | '.join(reserved_keywords)}
                )\b ''', token.Keyword.Reserved),

            (r'\b(?i)(?<![:\.])(__self__|__subject__)\b',
             token.Name.Builtin.Pseudo),

            (r'\b(__type__)\b', token.Name.Builtin.Pseudo),
        ],
        'strings': [
            (r'''(?x)
                (?P<Q>['"])
                (?:
                    (\\['"] | \n | .)*?
                )
                (?P=Q)
            ''', token.String),
            (r'''(?x)
                (?P<Q>
                    # capture the opening quote in group Q
                    (
                        \$([A-Za-z\200-\377_][0-9]*)*\$
                    )
                )
                (?:
                    (\\['"] | \n | .)*?
                )
                (?P=Q)
            ''', token.String.Other),
            (r'`.*?`', token.String.Backtick)
        ],
        'numbers': [
            (r'''(?x)
                (?<!\w)
                    (?: \d+ (?:\.\d+)?
                        (?:[eE](?:[+\-])?[0-9]+)
                    )
                    |
                    (?: \d+\.\d+)
            ''', token.Number.Float),
            (r'(?<!\w)\d+', token.Number.Integer),
        ],
    }
