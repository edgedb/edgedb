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
            (r'\$[\w\d]+', token.Name.Variable),
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
                    abstract | action | after | any | array | as | asc |
                    atom | attribute | before | by | concept |
                    constraint | database | delegated | desc | event |
                    extending | final | first | for | from | index |
                    initial | last | link | map | migration | of | on |
                    policy | property | required | rename | target |
                    then | to | transaction | tuple | value | view |

                    aggregate | all | alter | and | commit | create |
                    delete | distinct | drop | else | exists | filter |
                    function | get | group | if | ilike | in |
                    insert | is | like | limit | module | not | offset |
                    or | order | over | partition | rollback |
                    select | set | singleton | start | update | union |
                    with
                )\b ''', token.Keyword.Reserved),

            (r'\b(?i)(?<![:\.])(self|__subject__)\b',
             token.Name.Builtin.Pseudo),

            (r'\b(__class__)\b', token.Name.Builtin.Pseudo),
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
