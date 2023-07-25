#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2011-present MagicStack Inc. and the EdgeDB authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""Library for building nicely indented output using f-strings.

textwrap.dedent allows removing extra indentation, but it performs
poorly when strings get interpolated in before dedenting, especially
if those strings were produced at a different level of indentation.

The `escape` function escapes a string for interpolation. Notionally,
the an interpolated escaped string has all of its leading indentation
stripped, and when it is interpolated in, lines after the first are
indented at the level the interpolated string appears in the output.

Interpolating an escaped `LINE_BLANK` deletes a newline that appears
directly before it. This can be useful when a branch might produce
nothing, but it is interpolated nonconditionally.

The `xdedent` function takes a string with interpolated escaped
strings and properly formats it.


The system uses escape delimeters for maintaining a nesting structure
in strings that the user produces. The `xdedent` function then parses apart
the nesting structure and interprets it.
Obviously, as with all schemes for
in-band signalling, all hell can break loose if the signals appear in
the input data unescaped.

Our signal sequences contain a null byte and both kinds of quote
character, so you should be fine as long as any untrusted data
either:
 * Has no null bytes
 * Has at least one kind of quote character escaped in it somehow

"""

from __future__ import annotations


import textwrap
from typing import Any

_LEFT_ESCAPE = "\0'\"<<{<[<[{{<!!!"
_RIGHT_ESCAPE = "\0'\"!!!>}}]>]>}>>"
_ESCAPE_LEN = len(_LEFT_ESCAPE)
assert len(_RIGHT_ESCAPE) == _ESCAPE_LEN

LINE_BLANK = _LEFT_ESCAPE[:-1] + "||||||" + _RIGHT_ESCAPE[1:]


def escape(s: str) -> str:
    return _LEFT_ESCAPE + s.strip('\n') + _RIGHT_ESCAPE


Rep = list[str | list[Any]]


def _parse(s: str, start: int) -> tuple[Rep, int]:
    frags: Rep = []
    while start < len(s):
        nleft = s.find(_LEFT_ESCAPE, start)
        nright = s.find(_RIGHT_ESCAPE, start)
        if nleft == nright == -1:
            frags.append(s[start:])
            start = len(s)
        elif nleft != -1 and nleft < nright:
            if nleft > start:
                frags.append(s[start:nleft])
            subfrag, start = _parse(s, nleft + _ESCAPE_LEN)
            # If it is the special magic line blanking fragment,
            # delete up through the last newline. Otherwise collect it.
            if subfrag == [LINE_BLANK] and frags and isinstance(frags[-1], str):
                frags[-1] = frags[-1].rsplit('\n', 1)[0]
            else:
                frags.append(subfrag)
        else:
            assert nright >= 0
            frags.append(s[start:nright])
            start = nright + _ESCAPE_LEN
            break

    return frags, start


def _format_rep(rep: Rep) -> str:
    # cpython does some really dubious things to make appending in place
    # to a string efficient, and we depend on them here
    out_str = ""

    # TODO: I think there ought to be a more complicated algorithm
    # that builds a list of lines + indentation metadata and then
    # fixes it all up in one go?

    for frag in rep:
        if isinstance(frag, str):
            out_str += frag
        else:
            fixed_frag = _format_rep(frag)
            # If there is a newline in the final result, we need to indent
            # it to our current position on the current line.
            if '\n' in fixed_frag:
                last_nl = out_str.rfind('\n')
                indent = (
                    len(out_str) if last_nl < 0
                    else len(out_str) - last_nl - 1
                )
                # Indent all the lines but the first (since that goes
                # onto our current line)
                fixed_frag = textwrap.indent(fixed_frag, ' ' * indent)[indent:]

            out_str += fixed_frag

    return textwrap.dedent(out_str).removesuffix('\n')


def xdedent(s: str) -> str:
    # unlike regular dedent, xdedent trims a leading newline
    s = s.removeprefix('\n')
    parsed, _ = _parse(s, 0)
    res = _format_rep(parsed)
    assert _LEFT_ESCAPE not in res
    return res
