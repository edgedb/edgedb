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
            (r'(?i)\b(true|false|null)\b', Keyword.Constant),
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
                    create | database | delete | delta | desc |
                    distinct | drop | except | exists | event | filter |
                    final | first | for | from | function | group |
                    ilike | in | index | inherit | inheriting | inout |
                    insert | intersect | is | last | like | limit | link |
                    mod | module | no | not | nulls | of | offset |
                    operator | or | order | out | over | partition |
                    policy | property | required | rename | returning |
                    rollback | select | set | single | some | start | target |
                    then | to | transaction | union | update | using | where |
                    with
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
