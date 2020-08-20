"""@package piezo.validators
Collection of validator especially for ObjectStateHolder and
ObjectStateHolderVector.
"""

__author__ = "Kevin Woldt"
__copyright__ = "Copyright (C) 2017"
__license__ = "MIT License"

from itertools import chain


def validate_all(*funcs):
    """Validate that all given functions returns True.
    @param *funcs validation functions
    @return function The validation function is returned
    """
    def validate(item):
        """Return True if all given functions are True (or if no function is
        given).
        @param item ObjectStateHolder
        @return bool True if all functions returned True, False otherwise
        """
        for func in funcs:
            if not func(item):
                return False
        return True

    return validate


def validate_any(*funcs):
    """Validate that one given function returns True.
    @param *funcs validation functions
    @return function The validation function is returned
    """
    def validate(item):
        """Return True if any given function is True. If no function is given,
        return False.
        @param item ObjectStateHolder
        @return bool True if any given function is True
        """
        for func in funcs:
            if func(item):
                return True
        return False

    return validate


def validate_osh_class(*classes):
    """Validate that given item is an object state holder of any given class.
    @param classes iterable of class names as str of object state holder as
        defined in UCMDB to validate agains
    @return function The validator function is returned
    """
    def validate(item):
        """Validate if item is any of object class in classes.
        @param item ObjectStateHolder
        @return bool True if object class of item equals to any of classes,
            False otherwise
        """
        try:
            cls = item.getObjectClass()
        except AttributeError:
            msg = 'argument must be a ObjectStateHolder, but is of type "%s"' % (
                type(item))
            raise TypeError(msg)
        if cls in classes:
            return True
        return False

    return validate


def validate_osh_hasattr(*args, **kwargs):
    """Validate that given objects state holder has given attributes. Accepts
    attributes as arguments and key/value pairs as keyword arguments.
    If you want to check if a list attribute contain one or more specific
    values, the value of the keyword argument needs to be a list.
    @param *args Attributes to check existens
    @param **kwargs Attributes to check values
    @return function The validator function is returned
    """
    def validate(item):
        """Validate if item has all given attributes defined.
        @param item ObjectStateHolder
        @return bool True if item has all given attributes defined, False
            otherwise
        """
        try:
            item_attrs = item.getAttributeAll()
        except AttributeError:
            msg = 'argument must be a ObjectStateHolder, but is of type "%s"' % (
                type(item))
            raise TypeError(msg)

        attrs = dict((attr.getName(), attr) for attr in item_attrs)
        for attr in chain(args, kwargs):
            if attr not in attrs:
                return False

        for name, value in kwargs.iteritems():
            attr = attrs[name]
            attr_value = attr.getValue()
            if attr.isList():
                for item in value:
                    if item not in attr_value:
                        return False
            else:
                if value != attr_value:
                    return False

        return True

    return validate


def validate_rel_ends(end1=None, end2=None):
    """Validate that ends of given relation equals the given ends.
    validate_rel_ends() accepts either a validation function for that end or
    None if validation for that end should be skipped.
    @param end1 function
    @param end2 function
    @return function the validator function is returned
    """
    ends = {'link_end1': end1, 'link_end2': end2}
    for end, func in list(ends.iteritems()):
        if func is None:
            del ends[end]
    if not ends:
        return lambda: False

    def validate(item):
        """Validate if item ends are matching accordingly.
        @param item ObjectStateHolder
        @return bool True if item ends are matching, False otherwise
        """
        if not hasattr(item, 'getAttributeValue'):
            msg = 'argument must be a ObjectStateHolder, but is of type "%s"' % (
                type(item))
            raise TypeError(msg)
        for end, func in ends.iteritems():
            if func(item.getAttributeValue(end)) is False:
                return False

        return True

    return validate
