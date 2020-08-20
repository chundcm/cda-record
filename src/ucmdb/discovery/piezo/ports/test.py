import sys
print sys.excepthook
import better_exceptions
print sys.excepthook


import os

foo = 52

def shallow(a, b):

    deep(a + b)





def deep(val):

    global foo

    assert val > 10 and foo == 60

bar = foo - 50
shallow(bar, 15)
shallow(bar, 2)
