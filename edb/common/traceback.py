# mypy: disable-error-code="attr-defined"

# Portions copyright 2019-present MagicStack Inc. and the EdgeDB authors.
# Portions copyright 2001-2019 Python Software Foundation.
# License: PSFL.

"""
Provides stack trace formatting that prints `{filename}:{line}`, instead of
`"{filename}", line {line}`.

Stolen from Python's traceback module.
"""

import traceback
import typing
from contextlib import suppress

StackSummaryLike = (
    traceback.StackSummary
    | typing.List[typing.Tuple[str, typing.Any, str, typing.Any]]
)


def format_exception(e: BaseException) -> str:
    exctype = type(e)
    value = e
    tb = e.__traceback__
    tb_e = traceback.TracebackException(
        exctype, value, tb, compact=True
    )
    tb_e.stack = StandardStackSummary(tb_e.stack)
    return '\n'.join(tb_e.format())


def format_stack_summary(stack: StackSummaryLike) -> typing.List[str]:
    return _format_stack_summary(_into_list_of_frames(stack))


class StandardStackSummary(traceback.StackSummary):
    def format(self) -> typing.List[str]:
        return format_stack_summary(self)


def _into_list_of_frames(a_list: StackSummaryLike):
    """
    Create a StackSummary object from a supplied list of
    FrameSummary objects or old-style list of tuples.
    """
    # While doing a fast-path check for isinstance(a_list, StackSummary) is
    # appealing, idlelib.run.cleanup_traceback and other similar code may
    # break this by making arbitrary frames plain tuples, so we need to
    # check on a frame by frame basis.
    result = []
    for frame in a_list:
        if isinstance(frame, traceback.FrameSummary):
            result.append(frame)
        else:
            filename, lineno, name, line = frame
            result.append(
                traceback.FrameSummary(filename, lineno, name, line=line)
            )
    return result


def _format_stack_summary(stack: typing.List[traceback.FrameSummary]):
    """Format the stack ready for printing.

    Returns a list of strings ready for printing.  Each string in the
    resulting list corresponds to a single frame from the stack.
    Each string ends in a newline; the strings may contain internal
    newlines as well, for those items with source text lines.

    For long sequences of the same frame and line, the first few
    repetitions are shown, followed by a summary line stating the exact
    number of further repetitions.
    """
    result = []
    last_file = None
    last_line = None
    last_name = None
    count = 0
    for frame_summary in stack:
        formatted_frame = _format_frame_summary(frame_summary)
        if formatted_frame is None:
            continue
        if (
            last_file is None
            or last_file != frame_summary.filename
            or last_line is None
            or last_line != frame_summary.lineno
            or last_name is None
            or last_name != frame_summary.name
        ):
            if count > traceback._RECURSIVE_CUTOFF:
                count -= traceback._RECURSIVE_CUTOFF
                result.append(
                    f'  [Previous line repeated {count} more '
                    f'time{"s" if count > 1 else ""}]\n'
                )
            last_file = frame_summary.filename
            last_line = frame_summary.lineno
            last_name = frame_summary.name
            count = 0
        count += 1
        if count > traceback._RECURSIVE_CUTOFF:
            continue
        result.append(formatted_frame)

    if count > traceback._RECURSIVE_CUTOFF:
        count -= traceback._RECURSIVE_CUTOFF
        result.append(
            f'  [Previous line repeated {count} more '
            f'time{"s" if count > 1 else ""}]\n'
        )
    return result


def _format_frame_summary(frame: traceback.FrameSummary):
    """Format the lines for a single FrameSummary.

    Returns a string representing one frame involved in the stack. This
    gets called for every frame to be printed in the stack summary.
    """
    row = [f'  {frame.filename}:{frame.lineno}, in {frame.name}\n']
    if frame.line:
        stripped_line = frame.line.strip()
        row.append('    {}\n'.format(stripped_line))

        orig_line_len = len(frame._original_line)
        frame_line_len = len(frame.line.lstrip())
        stripped_characters = orig_line_len - frame_line_len
        if frame.colno is not None and frame.end_colno is not None:
            start_offset = (
                traceback._byte_offset_to_character_offset(
                    frame._original_line, frame.colno
                )
                + 1
            )
            end_offset = (
                traceback._byte_offset_to_character_offset(
                    frame._original_line, frame.end_colno
                )
                + 1
            )

            anchors = None
            if frame.lineno == frame.end_lineno:
                with suppress(Exception):
                    anchors = (
                        traceback._extract_caret_anchors_from_line_segment(
                            frame._original_line[
                                start_offset - 1 : end_offset - 1
                            ]
                        )
                    )
            else:
                end_offset = stripped_characters + len(stripped_line)

            # show indicators if primary char doesn't span the frame line
            if end_offset - start_offset < len(stripped_line) or (
                anchors
                and anchors.right_start_offset - anchors.left_end_offset > 0
            ):
                row.append('    ')
                row.append(' ' * (start_offset - stripped_characters))

                if anchors:
                    row.append(anchors.primary_char * (anchors.left_end_offset))
                    row.append(
                        anchors.secondary_char
                        * (anchors.right_start_offset - anchors.left_end_offset)
                    )
                    row.append(
                        anchors.primary_char
                        * (
                            end_offset
                            - start_offset
                            - anchors.right_start_offset
                        )
                    )
                else:
                    row.append('^' * (end_offset - start_offset))

                row.append('\n')

    if frame.locals:
        for name, value in sorted(frame.locals.items()):
            row.append('    {name} = {value}\n'.format(name=name, value=value))

    return ''.join(row)
