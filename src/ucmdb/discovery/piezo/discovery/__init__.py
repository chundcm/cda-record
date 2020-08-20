# coding=utf-8

"""@package piezo.discovery
Useful collection of helper functions and classes for discovery and
integration.
"""

# python builtins
import traceback
from operator import xor

# JAVA API
from java.lang import Exception as JavaException

# UCMDB JAVA API
import logger

from ..exceptions import ClearException
from ..stateholders.oshv import ObjStateHolderVector
from ..services import get_framework

__author__ = "Kevin Woldt"
__copyright__ = "Copyright (C) 2017"
__license__ = "MIT License"


class Structure(object):

    """Abstract class. All deriving classes need to implement the class
    variable _attributes, a list of attributes required for initializing
    an instance.

        >>> class Example(Structure):
            ...     _attributes = ('attr1', 'attr2')
        >>> obj1 = Example(attr1='foo', attr2='bar')
        >>> obj2 = Example(attr1='foo')
        Traceback (most recent call last):
            ...
        TypeError: missing argument "attr2"

    Use the attributes as usual, but keep in mind that they are immutable.

        >>> class Example(Structure):
            ...   _attributes = ('attr',)
        >>> obj = Example(attr='foo')
        >>> obj.attr
        'foo'
        >>> obj.attr = 'bar'
        Traceback (most recent call last):
            ...
        RuntimeError: attribute "attr" is immutable
    """

    _attributes = ()

    def __init__(self, **kwargs):
        """Class constructor.
        @param[in] **kwargs The attributes defined as mandatory in _attributes
        """

        for attr in self._attributes:
            if attr not in kwargs:
                msg = 'missing argument "%s"' % (attr)
                raise TypeError(msg)

            # writing directly to instance dict
            # see self.__setattr__() for details
            self.__dict__[attr] = kwargs[attr]

    def __str__(self):
        """Converts the object state into a string. The state of the object
        are all attributes defined in _attributes with there values.
        """

        str_dict = dict((attr, getattr(self, attr))
                        for attr in self._attributes)
        return '(%s %s)' % (self.__class__.__name__, str(str_dict))

    def __hash__(self):
        """The instance hash is a mix of all hashes of all values of all
        attributes in _attributes.
        """

        attr_values = (getattr(self, attr) for attr in self._attributes)
        attr_hashes = (hash(value) for value in attr_values)
        return reduce(xor, attr_hashes, hash(False))

    def __eq__(self, other):
        """Two instances of Structure are equal if there hashes are equal.
        @param other instance to compare to
        @return boolean True if the instances are equal
        """

        eqres = hash(self) == hash(other)
        return eqres

    def __ne__(self, other):
        """Opposite of __eq__().
        @see __eq__()
        @param other instance to compare to
        @return boolean True if the instances are unequal

        """

        return not self.__eq__(other)

    def __setattr__(self, name, value):
        """Set a class attribute to the given value. The hash of the instance
        must not change during lifetime to use the instance in sets or as dict
        keys. So we forbid to set the attributes defined in _attributes.
        @param name  the attribute name
        @param value the value the attribute should be set
        @exception RuntimeError Raised while trying to change an attribute
            declared in _attributes.
        """

        if name in self._attributes:
            msg = 'attribute "%s" is immutable' % (name)
            raise RuntimeError(msg)
        return super(Structure, self).__setattr__(name, value)

    def __delattr__(self, name):
        """Delete a class attribute. The hash of the instance must not change
        during lifetime to use the instance in sets or as dict keys. So we
        forbid to delete the attributes defined in _attributes.
        @param name the attribute name
        @exception RuntimeError Raised while trying to delete an attribute
            declared in _attributes.
        """

        if name in self._attributes:
            msg = 'attribute "%s" is immutable' % (name)
            raise RuntimeError(msg)
        return super(Structure, self).__delattr__(name)


class DefaultStructure(Structure):

    """Abstract class. All deriving classes need to implement the class
    variable _default_factory. _default_factory is called for each attribute in
    _attributes to initialize it with the return value.

    >>> class TestCache(DefaultStructure):
    ...     _attributes = ('foo', 'bar')
    ...     _default_factory = dict

    >>> test = TestCache()
    >>> test.foo
    {}
    >>> test.foo['spam'] = 'egg'
    >>> test.foo
    {'spam': 'egg'}
    """

    _default_factory = TypeError

    def __init__(self):
        attributes = dict((attr, self._default_factory())
                          for attr in self._attributes)
        super(DefaultStructure, self).__init__(**attributes)


class Discovery(object):

    """Generalized error handling and framework reporting. Replaces
    DiscoveryMain with a generalized error handling and framework reporting.

        >>> from piezo import Discovery
        >>> def discover_some_topology(addv, delv):
                ...
        >>> DiscoveryMain = Discovery(discover_some_topology)
    """

    def __init__(self, discovery_func, clear_oshv_on_error=True, oshv_class=ObjStateHolderVector, oshv_convert_func=lambda v: v.oshv):
        """Class constructor. Takes the discovery function to execute.
        @param discovery_func discovery function to execute
        @param clear_oshv_on_error defines if the vector is cleared if a exception is caugth
        @param oshv_class The class to use to create add/update and deletion
            vectors. Defaults to ObjStateHolderVector, a wrapper around
            ObjectStateHolderVector.
        @param oshv_convert_func A function to convert the oshv_class into a
            ObjectStateHolderVector class for sending to UCMDB.
        """
        self._discovery_func = discovery_func
        self._clear_osh = clear_oshv_on_error
        self._oshv_class = oshv_class
        self._oshv_convert_func = oshv_convert_func

    def __call__(self, *args, **kwargs):
        """Called when the instance is called as a function. This method
        defines `x(arg1, arg2, ...)` as shorthand for
        `x.__call__(arg1, arg2, ...)`, which in turn is an alias for
        `x.start()`.
        """
        self.start(*args, **kwargs)

    def start(self, *args, **kwargs):
        """Executes discovery entry function. Catches all python and java
        exceptions and reports an error to the framework. The communication log
        gets additionally a stack trace.
        """
        framework = get_framework()
        add_oshv = self._oshv_class()
        del_oshv = self._oshv_class()

        try:
            self._discovery_func(add_oshv, del_oshv)
        except (Exception, JavaException), err:
            if isinstance(err, ClearException):
                if err.clear_add_oshv:
                    add_oshv.clear()
                if err.clear_del_oshv:
                    del_oshv.clear()
                err = err.error
            elif self._clear_osh:
                add_oshv.clear()
                del_oshv.clear()

            err_msg = '%s: %s' % (err.__class__.__name__, err)
            framework.reportError(err_msg)
            logger.error(err_msg)
            logger.error(traceback.format_exc())
        finally:
            framework.sendObjects(self._oshv_convert_func(add_oshv))
            framework.deleteObjects(self._oshv_convert_func(del_oshv))
