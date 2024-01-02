import os
import sys


def file_root():
    p = os.path.dirname(os.path.abspath(__file__))
    sys.path.insert(0, p)
    return p


print('a')
