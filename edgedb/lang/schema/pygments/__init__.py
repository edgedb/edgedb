from pygments.lexer import RegexLexer, include
from pygments.token import *


__all__ = ['EdgeSchemaLexer']


class EdgeSchemaLexer(RegexLexer):
    name = 'EdgeSchema'
    aliases = ['eschema']
    filenames = ['*.eschema']

    tokens = {
        'root': [
            include('comments'),
            include('keywords'),
            (r'\*1|\*\*|1\*', String),
            include('numbers'),
            include('strings'),
            (r'\b(true|false|null)\b', Keyword.Constant),
            (r'\s+', Text),
            (r'.', Text),
        ],
        'comments': [
            (r'#.*?\n', Comment.Singleline),
        ],
        'keywords': [
            (r'''(?x)
                \b(?<![:\.])(
                  action | atom | attribute | concept | constraint | event |
                  extends | index | link | linkproperty | properties
                )\b
            ''', Keyword.Reserved),
            (r'\b(?<![:\.])(abstract|final|required)\b', Keyword.Declaration),
            (r'\b(?<![:\.])(as|import|on|to)\b', Keyword.Namespace),
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
