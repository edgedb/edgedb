
SHOW_WARNINGS = True


def print_warning(*args, **kwargs):
    if SHOW_WARNINGS:
        print(*args, **kwargs)