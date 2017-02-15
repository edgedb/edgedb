from pygments.lexer import RegexLexer, bygroups, include
from pygments.token import *


__all__ = ['EdgeQLLexer']


class EdgeQLLexer(RegexLexer):
    name = 'EdgeQL'
    aliases = ['eql', 'edgeql']
    filenames = ['*.eql', '*.edgeql']

    tokens = {
        'root': [
            include('comments'),
            include('keywords'),
            (r'@\w+', Name.Decorator),
            (r'\$\w+', Name.Variable),
            include('numbers'),
            include('strings'),
            (r'(?i)\b(true|false|empty)\b', Keyword.Constant),
            (r'\s+', Text),
            (r'.', Text),
        ],
        'comments': [
            (r'#.*?\n', Comment.Singleline),
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
            ''', Keyword.Reserved),
        ],
        'strings': [
            (r'''(?x)
                (?P<Q>['"])
                (?:
                    (\\['"] | \n | .)*?
                )
                (?P=Q)
            ''', String),
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
            ''', String.Other),
            (r'`.*?`', String.Backtick)
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
