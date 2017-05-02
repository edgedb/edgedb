from pygments.lexer import RegexLexer, include
from pygments import token


__all__ = ['EdgeQLLexer']


class EdgeQLLexer(RegexLexer):
    name = 'EdgeQL'
    aliases = ['eql', 'edgeql']
    filenames = ['*.eql', '*.edgeql']

    tokens = {
        'root': [
            include('comments'),
            include('keywords'),
            (r'@\w+', token.Name.Decorator),
            (r'\$\w+', token.Name.Variable),
            include('numbers'),
            include('strings'),
            (r'(?i)\b(true|false|empty)\b', token.Keyword.Constant),
            (r'\s+', token.Text),
            (r'.', token.Text),
        ],
        'comments': [
            (r'#.*?\n', token.Comment.Singleline),
        ],
        'keywords': [
            (r'''(?ix)
                \b(?<![:\.])(
                    abstract | action | after | aggregate | all |
                    alter | and | any | as | asc | atom | attribute |
                    before | by | commit | concept | constraint |
                    create | database | delete | desc | distinct |
                    drop | else | except | exists | event |
                    filter | final | first | for | from |
                    function | get | group | if | ilike | in | index |
                    inherit | inheriting | insert | intersect | is |
                    last | like | limit | link | migration | module |
                    not | offset | or | order | over | partition |
                    policy | property | required | rename | returning |
                    rollback | select | set | singleton | start |
                    target | then | to | transaction | update |
                    union | with
                )\b
            ''', token.Keyword.Reserved),
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
