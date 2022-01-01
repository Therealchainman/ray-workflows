import os

def _get_root():
    cwd = os.path.dirname(os.path.abspath(__file__))
    head = os.path.split(cwd)[0]
    return head