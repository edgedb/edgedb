from pygments import lexer, token


__all__ = ['EdgeSchemaLexer']


class EdgeSchemaLexer(lexer.RegexLexer):
    name = 'EdgeSchema'
    aliases = ['eschema']
    filenames = ['*.eschema']

    tokens = {
        'root': [
            lexer.include('comments'),
            lexer.include('keywords'),
            (r'\*1|\*\*|1\*', token.String),
            lexer.include('numbers'),
            lexer.include('strings'),
            (r'\b(true|false|null)\b', token.Keyword.Constant),
            (r'\s+', token.Text),
            (r'.', token.Text),
        ],

        'comments': [
            (r'#.*?\n', token.Comment.Singleline),
        ],

        'keywords': [
            (r'''(?x)
                \b(?<![:\.])(
                  action | atom | attribute | concept | constraint |
                  event | extending | index | initial | link |
                  linkproperty | properties | value | view
                )\b
            ''', token.Keyword.Reserved),

            (r'\b(?<![:\.])(abstract|final|required)\b',
             token.Keyword.Declaration),

            (r'\b(?<![:\.])(as|import|on|to)\b', token.Keyword.Namespace),
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
                    (?: \d+ (?:\.\d*)?
                        |
                        \. \d+
                    ) (?:[eE](?:[+\-])?[0-9]+)
            ''', token.Number.Float),
            (r'''(?x)
                (?<!\w)
                    (?: \d+\.(?!\.)\d*
                        |
                        \.\d+)
            ''', token.Number.Float),
            (r'(?<!\w)\d+', token.Number.Integer),
        ],
    }
