# SHOW_WARNINGS = True
SHOW_WARNINGS = False


def print_warning(*args, **kwargs):
    if SHOW_WARNINGS:
        print(*args, **kwargs)
