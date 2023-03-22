import re

# This matches spaces, minus or an empty string that comes before capital
# letter (and not at the start of the string).
# And is used to replace that word boundary for the underscore.
# It handles cases like this:
# * `Foo Bar` -- title case -- matches space
# * `FooBar` -- CamelCase -- matches empty string before `Bar`
# * `Some-word` -- words with dash -- matches dash
word_boundary_re = re.compile(r'(?<!^)(?<!\s|-)[\s-]*(?=[A-Z])')


def to_snake_case(name: str) -> str:
    # note this only covers cases we have not all possible cases of
    # case conversion
    return word_boundary_re.sub('_', name).lower()


def to_camel_case(name: str) -> str:
    # note this only covers cases we have not all possible cases of
    # case conversion
    return word_boundary_re.sub('', name)
