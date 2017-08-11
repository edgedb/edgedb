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
            (r'@\w+', token.Name.Decorator),
            (r'\$[\w\d]+', token.Name.Variable),
            lexer.include('numbers'),
            lexer.include('strings'),
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
                  (?# schema-scpecific)
                  linkproperty |

                  (?# from EdgeQL)
                  action | after | asc | atom |
                  attribute | before | by | concept | constraint |
                  database | delegated | desc | event | extending |
                  first | for | from | index | initial | last | link |
                  migration | of | on | policy | property | rename |
                  target | then | transaction | value | view |

                  aggregate | all | alter | and | commit | create | delete |
                  distinct | drop | else | exists | filter |
                  function | get | group | if | ilike | in | insert | is |
                  like | limit | module | not | offset | or | order | over |
                  partition | rollback | select | set | singleton | start |
                  update | union | with

                )\b
            ''', token.Keyword.Reserved),

            (r'\b(?i)(?<![:\.])(abstract|final|required)\b',
             token.Keyword.Declaration),

            (r'\b(?i)(?<![:\.])(as|import|to)\b', token.Keyword.Namespace),

            (r'\b(?i)(?<![:\.])(self|__subject__)\b',
             token.Name.Builtin.Pseudo),

            (r'\b(?i)(__class__)\b', token.Name.Builtin.Pseudo),
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
