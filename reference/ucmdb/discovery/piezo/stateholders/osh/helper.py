# coding=utf-8

"""@package piezo.stateholders.osh.helper
Helper functions to set specific attribute types and values.
"""

__author__ = "Kevin Woldt"
__copyright__ = "Copyright (C) 2017"
__license__ = "MIT License"

from appilog.common.system.defines.AppilogTypes import (
    INTEGER_DEF, LONG_DEF, DOUBLE_DEF, STRING_DEF, DATE_DEF, BOOLEAN_DEF,
    UNKNOWN_DEF)
from appilog.common.system.types.vectors import (
    IntegerVector, LongVector, DoubleVector, StringVector, DateVector,
    BooleanVector, ObjectStateHolderVector)
from appilog.common.system.types.attributecache import AttributeSHBean
from appilog.common.system.types import AttributeStateHolder

LIST_OP_SET = 0
LIST_OP_ADD = 51
LIST_OP_RM = 52


def listattr(iterable, operator=LIST_OP_SET, objtype=UNKNOWN_DEF):
    """Helper function to set a list attribute or add/remove items to/from the
    list.

        >>> osh = ObjStateHolder()
        >>> osh['spam'] = listattr(['foo', 'bar'])
        >>> str(osh)
        "ObjStateHolder(None, {u'spam': [u'foo', u'bar']})"

    @param iterable All value to set.
    @param operator int Defines if the given values should be appended to,
        removed from or should entirely replace the list in UCMDB. Defaults to
        replace the list.
    @param objtype str The attribute type defined for the CI type.
    """
    values = list(iterable)
    for value in values:
        if objtype == UNKNOWN_DEF:
            objtype = AttributeSHBean.getTypeFromValue(value, False)

    if objtype == INTEGER_DEF:
        vector = IntegerVector()
    elif objtype == LONG_DEF:
        vector = LongVector()
    elif objtype == DOUBLE_DEF:
        vector = DoubleVector()
    elif objtype == STRING_DEF:
        vector = StringVector()
    elif objtype == DATE_DEF:
        vector = DateVector()
    elif objtype == BOOLEAN_DEF:
        vector = BooleanVector()
    else:
        vector = ObjectStateHolderVector()

    for value in values:
        if value not in vector:
            vector.add(value)

    def wrapper(key):
        """Returns a AttributeStateHolder with the given key."""
        return AttributeStateHolder(key, vector, operator)
    return wrapper


def noneattr(objtype=UNKNOWN_DEF):
    """Helper function to set a attribute to None. If you do not specify
    objtype, UCMDB will silently ignore the request.

        >>> osh = ObjStateHolder()
        >>> osh['spam'] = noneattr(STRING_DEF)
        >>> str(osh)
        "ObjStateHolder(None, {u'spam': None})"
        >>> osh.getAttribute('spam').getType() == STRING_DEF
        True

    In case you are not sure if a value is None use a condition:

        >>> from random import choice
        >>> values = range(9) + [None]
        >>> osh = ObjStateHolder()
        >>> osh['spam'] = choice(values) or noneattr(INTEGER_DEF)

    @param objtype str The optional attribute type defined for the CI type. If
        not specified, UCMDB will silently ignore the request.
    @returns A function creating the attribute with the given name.
    """
    def wrapper(key):
        """Returns a AttributeStateHolder woth the given key."""
        return AttributeStateHolder(key, None, 1, objtype)
    return wrapper

def ornoneattr(value, objtype=UNKNOWN_DEF):
    """Helper function to set a attribute to given value. Handle None values
    by utilizing noneattr().
    @param value The value to set
    @param objtype str The optional attribute type defined for the CI type. If
        not specified, UCMDB will silently ignore the request.
    @returns The given value or a function creating the attribute with the given name.
    """
    return value or noneattr(objtype)
