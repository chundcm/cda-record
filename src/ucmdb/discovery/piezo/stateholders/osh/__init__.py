# coding=utf-8

"""@package piezo.stateholders.osh
Wrapper around the ObjectStateHolder class of UCMDB API.
"""

__author__ = "Kevin Woldt"
__copyright__ = "Copyright (C) 2017"
__license__ = "MIT License"

# python API
from collections import MutableMapping
from collections import Iterable
from collections import Callable

# JAVA API
from java.lang import (
    Boolean,
    Double, Float,
    Enum,
    Integer, Long, Short,
    Character, String, StringBuffer, StringBuilder)
from java.lang import Iterable as JIterable
from java.util import Date

# UCMDB JAVA API
from appilog.common.system.types import AttributeStateHolder
from appilog.common.system.types import ObjectStateHolder
from appilog.common.system.types.vectors import ObjectStateHolderVector


class ObjStateHolder(MutableMapping):

    """Wrapper around ObjectStateHolder. Implements all ABS abstract and mixin
    methods to use the ObjectStateHolder like python dict.

        >>> osh = ObjStateHolder('ipaddress')
        >>> osh['name'] = '1.1.1.1'
        >>> osh.update(routing_domain='mydomain')
        >>> str(osh)
        "ObjStateHolder(ipaddress, {u'name': u'1.1.1.1', u'routing_domain': u'mydomain'})"
        >>> osh['routing_domain'].upper()
        u'MYDOMAIN'
        >>> len(osh)
        2

    The dataflow probe serializes the ObjectStateHolderVector and all
    containing elements to send it to the UCMDB. Subclasses of
    ObjectStateHolder are unknown there, especially Jython subclasses. So we
    implement a wrapper around ObjectStateHolder and ObjectStateHolderVector.
    The wrapper ObjStateHolderVector implicitly converts arguments and return
    values of get and set request to the pythonized methods. The consequence
    is that you have to do it manually, if you want to use ObjStateHolder, but
    not ObjStateHolderVector.

        >>> osh = ObjStateHolder('spam')
        >>> oshv = ObjectStateHolderVector()
        >>> oshv.add(osh.get_osh())
    """

    def __init__(self, cls=None, attrs=None):
        """Contructor.

            >>> # empty OSH
            >>> osh = ObjStateHolder()
            >>> # CI type name
            >>> cls = 'ipaddress'
            >>> # OSH of type cls
            >>> osh = ObjStateHolder(cls)
            >>> # attribute mapping
            >>> attrs = {'name': '1.1.1.1', 'routing_domain': 'mydomain'}
            >>> # OSH of type cls with attributes attrs
            >>> osh = ObjStateHolder(cls, attrs)
            >>> # wrapped OSH from the UCMDB JAVA API
            >>> osh = ObjStateHolder(ObjectStateHolder())

        @param cls An ObjectStateHolder to copy from or the CI type of the
            object
        @param attrs A mapping of attributes and there values or a reference to
            an ObjectStateHolder.
        """
        if isinstance(cls, ObjectStateHolder):
            self.osh = cls
        else:
            self.osh = ObjectStateHolder()
            self.osh.setObjectClass(cls)
            if attrs is not None:
                self.update(attrs)

    def get_osh(self):
        """Returning the wrapped ObjectStateHolder.
        @return ObjectStateHolder The wrapped ObjectStateHolder
        """
        return self.osh

    def __getattr__(self, name):
        """Redirecting attribute lookup to the wrapped ObjectStateHolder."""
        return getattr(self.osh, name)

    def __len__(self):
        """Called to implement the built-in function len(). Returns the number
        of attributes defined, an integer >= 0.

            >>> osh = ObjStateHolder()
            >>> len(osh)
            0
            >>> osh['name'] = '1.1.1.1'
            >>> len(osh)
            1

        @return integer Number of atributes defined
        """
        return self.osh.getAttributeAll().size()

    def iterkeys(self):
        """Return an iterator over the dictionary's keys.

            >>> osh = ObjStateHolder('ipaddress', {
            ...     'routing_domain': 'mydomain',
            ...     'name': '1.1.1.1',
            ... })
            >>> sorted(osh.iterkeys())
            [u'name', u'routing_domain']

        @return Generator
        """
        return (elem.getName() for elem in self.osh.getAttributeAll())

    def __iter__(self):
        """Return an iterator over the keys of the object state holder. This
        is a shortcut for iterkeys().

            >>> osh = ObjStateHolder('ipaddress', {
            ...     'routing_domain': 'mydomain',
            ...     'name': '1.1.1.1',
            ... })
            >>> sorted(osh.iterkeys())
            [u'name', u'routing_domain']

        @return Iterator
        """
        return self.iterkeys()

    def __contains__(self, key):
        """Called to implement membership test operators. Return True if key
        is in self, False otherwise.

            >>> osh = ObjStateHolder('ipaddress', {'name': '1.1.1.1'})
            >>> 'name' in osh
            True
            >>> 'foo' in osh
            False

        @param key the attribute name to test membership for
        @return bool Return True if key is a member of the object state holder,
            else False.
        """
        return self.osh.findAttr(key) is not None

    def __getitem__(self, key):
        """Implements evaluation of self[key]. If key is missing (not in the
        container), KeyError is raised.

            >>> osh = ObjStateHolder('ipaddress', {'name': '1.1.1.1'})
            >>> osh['name']
            u'1.1.1.1'

        @param key the attribute name as defined in UCMDB
        @return The value of found
            appilog.common.system.types.AttributeStateHolder
        @exception KeyError Raises a KeyError if key is not in the dictionary.
        """
        item = self.osh.getAttribute(key)
        if item is None:
            raise KeyError("'%s'" % (key))
        value = item.getValue()
        if isinstance(value, ObjectStateHolder):
            return ObjStateHolder(value)
        return value

    def setattr(self, key, value):
        """Sets the attribute key to value. If you want to set the value None
        or an empty list, you should use one of the helper functions, otherwise
        the UCMDB will fail silently to update the attribute.

            >>> osh = ObjStateHolder()
            >>> osh.setattr('spam', 'spam')
            >>> str(osh)
            "ObjStateHolder(None, {u'spam': u'spam'})"
            >>> osh.getAttribute('spam').getType()
            u'String'
            >>> osh.setattr('foo', None)
            >>> str(osh)
            "ObjStateHolder(None, {u'foo': None, u'spam': u'spam'})"
            >>> osh.getAttribute('foo').getType()
            u'Unknown'
            >>> osh.setattr('foo', noneattr(STRING_DEF))
            >>> str(osh)
            "ObjStateHolder(None, {u'foo': None, u'spam': u'spam'})"
            >>> osh.getAttribute('foo').getType()
            u'String'

        If the attribute was already set before, the object type will be reused.

            >>> osh = ObjStateHolder()
            >>> osh.setattr('spam', 'spam')
            >>> osh.getAttribute('spam').getType()
            u'String'
            >>> osh.setattr('spam', None)
            >>> osh.getAttribute('spam').getType()
            u'String'

        @param key the attribute name as defined in UCMDB
        @param value the value to set
        """
        if isinstance(value, AttributeStateHolder):
            self.osh.setAttribute(value)
        elif isinstance(value,
                        (basestring, Character, String, StringBuffer,
                         StringBuilder)):
            self.osh.setStringAttribute(key, value)
        elif isinstance(value, (bool, Boolean)):
            self.osh.setBoolAttribute(key, value)
        elif isinstance(value, Date):
            self.osh.setDateAttribute(key, value)
        elif isinstance(value, Double):
            self.osh.setDoubleAttribute(key, value)
        elif isinstance(value, Enum):
            self.osh.setEnumAttribute(key, value)
        elif isinstance(value, (float, Float)):
            self.osh.setFloatAttribute(key, value)
        elif isinstance(value, (int, Integer, Short)):
            self.osh.setIntegerAttribute(key, value)
        elif isinstance(value, (long, Long)):
            self.osh.setLongAttribute(key, value)
        elif isinstance(value, ObjStateHolder):
            self.osh.setOSHAttribute(key, value.get_osh())
        elif isinstance(value, ObjectStateHolder):
            self.osh.setOSHAttribute(key, value)
        elif isinstance(value, (Iterable, JIterable)):
            self.osh.setListAttribute(key, value)
        elif isinstance(value, Callable):
            self.osh.setAttribute(value(key))
        else:
            attr = self.osh.getAttribute(key)
            if attr is not None:
                attr.setValue(value)
            else:
                self.osh.setAttribute(key, value)

    def __setitem__(self, key, value):
        """Called to implement assignment to self[key]. Same note as for
        __getitem__() and setattr(). The same exceptions are raised for
        improper key values as for the __getitem__() method.

            >>> osh = ObjStateHolder('ipaddress', {'name': '1.1.1.1'})
            >>> osh['name'] = '2.2.2.2'
            >>> osh['name']
            u'2.2.2.2'

        @param key   the attribute name as defined in UCMDB
        @param value the attribute value
        """
        self.setattr(key, value)

    def __delitem__(self, key):
        """Called to implement deletion of self[key]. Same note as for
        __getitem__(). The same exceptions are raised for improper key values
        as for the __getitem__() method.

            >>> osh = ObjStateHolder('ipaddress', {'name': '1.1.1.1'})
            >>> del osh['name']
            >>> 'name' in osh
            False

        @param key the attribute name as defined in UCMDB
        @exception KeyError Raises a KeyError if key is not in the dictionary.
        """
        if key in self:
            self.osh.removeAttribute(key)
        else:
            raise KeyError("'%s'" % (key))

    def keys(self):
        """Return a copy of the object state holder's list of keys.

            >>> osh = ObjStateHolder('ipaddress', {
            ...     'routing_domain': 'mydomain',
            ...     'name': '1.1.1.1',
            ... })
            >>> sorted(osh.keys())
            [u'name', u'routing_domain']

        @return list List of keys
        """
        return list(self.iterkeys())

    def iteritems(self):
        """Return an iterator over the object state holder's (key, value)
        pairs.

            >>> attrs = {
            ...     'routing_domain': 'mydomain',
            ...     'name': '1.1.1.1',
            ... }
            >>> osh = ObjStateHolder('ipaddress', attrs)
            >>> sorted(osh.iteritems()) == sorted(attrs.iteritems())
            True

        @return iterator
        """
        return ((elem.getName(), elem.getValue())
                for elem in self.osh.getAttributeAll())

    def items(self):
        """Return a copy of the object state holder's list of (key, value)
        pairs.

            >>> attrs = {
            ...     'routing_domain': 'mydomain',
            ...     'name': '1.1.1.1',
            ... }
            >>> osh = ObjStateHolder('ipaddress', attrs)
            >>> osh.items().sort() == attrs.items().sort()
            True

        @return list List of (key, value) pairs
        """
        return list(self.iteritems())

    def itervalues(self):
        """Return an iterator over the object state holder's values.

            >>> attrs = {
            ...     'routing_domain': 'mydomain',
            ...     'name': '1.1.1.1',
            ... }
            >>> osh = ObjStateHolder('ipaddress', attrs)
            >>> list(osh.itervalues()).sort() == attrs.values().sort()
            True

        @return iterator
        """
        return (elem.getValue() for elem in self.osh.getAttributeAll())

    def values(self):
        """Return a copy of the object state holder's list of values.

            >>> attrs = {
            ...     'routing_domain': 'mydomain',
            ...     'name': '1.1.1.1',
            ... }
            >>> osh = ObjStateHolder('ipaddress', attrs)
            >>> sorted(osh.itervalues()) == sorted(attrs.values())
            True

        @return list List of values
        """
        return list(self.itervalues())

    def get(self, key, default=None):
        """Return the value for key if key is in the object state holder, else
        default.  If default is not given, it defaults to None, so that this
        method never raises a KeyError.

            >>> osh = ObjStateHolder('ipaddress', {'name': '1.1.1.1'})
            >>> osh.get('name')
            u'1.1.1.1'
            >>> osh.get('foo') is None
            True

        @param key the attribute name as defined in UCMDB
        @param default the default value to return if key is not in the
            object state holder
        @return java.lang.Object
        """
        try:
            return self[key]
        except KeyError:
            return default

    def __eq__(self, other):
        """Called by comparison operators in preference to __cmp__().

            >>> osh1 = ObjStateHolder('ipaddress', {'name': '1.1.1.1'})
            >>> osh2 = ObjStateHolder('ipaddress', {'name': '1.1.1.1'})
            >>> osh1 == osh2
            True
            >>> osh2['name'] = '2.2.2.2'
            >>> osh1 == osh2
            False

        @return bool True if the ObjectStateHolders are equal, otherwise
            False.
        """
        if self.osh.getObjectClass() != other.osh.getObjectClass():
            return False

        sattrs = set((elem.getName(), elem.getValue(), elem.getType())
                     for elem in self.osh.getAttributeAll())
        oattrs = set((elem.getName(), elem.getValue(), elem.getType())
                     for elem in other.getAttributeAll())
        return sattrs == oattrs

    def __cmp__(self, other):
        """Called by comparison operations if rich comparison is not
        implemented.
        @param other the object to be compared
        @exception java.lang.ClassCastException Raised if other is not a string
        @exception java.lang.NullPointerException Raised if other is None
        @return integer Return a negative integer if self < other, zero if
            self == other, a positive integer if self > other.
        """
        if hasattr(other, 'get_osh'):
            other = other.get_osh()
        return self.osh.compareTo(other)

    def __ne__(self, other):
        """Called by comparison operators !=.

            >>> osh1 = ObjStateHolder('ipaddress', {'name': '1.1.1.1'})
            >>> osh2 = ObjStateHolder('ipaddress', {'name': '1.1.1.1'})
            >>> osh1 != osh2
            False
            >>> osh2['name'] = '2.2.2.2'
            >>> osh1 != osh2
            True

        @return bool False if the ObjectStateHolders are equal, otherwise
            True.
        """
        return not self.__eq__(other)

    def pop(self, key, *default):
        """If key is in the object state holder, remove it and return its value,
        else return default. If default is not given and key is not in the
        object state holder, a KeyError is raised.

            >>> osh = ObjStateHolder('ipaddress', {'name': '1.1.1.1'})
            >>> osh.pop('name')
            u'1.1.1.1'
            >>> osh.pop('name', 'foo')
            'foo'
            >>> osh.pop('name', None) is None
            True
            >>> osh.pop('name')
            Traceback (most recent call last):
                ...
            KeyError: "'name'"
            >>> osh.pop('name', 'foo', 'bar')
            Traceback (most recent call last):
                ...
            TypeError: pop expected at most 2 arguments, got 3

        @param key the attribute name
        @param default the default value to return if key is not in the
            dictionary
        @return java.lang.Object
        """
        # default is defined as *args that we can count the number of
        # arguments rather then defining default=None, because the
        # default is allowed to be None.
        try:
            value = self[key]
        except KeyError as err:
            length = len(default)
            # no default defined
            if length == 0:
                raise err
            # too many arguments
            elif length > 1:
                msg = 'pop expected at most {} arguments, got {}'
                raise TypeError(msg.format(2, length + 1))
            # using the given default
            else:
                value = default[0]
        else:
            del self[key]
        return value

    def popitem(self):
        """Remove and return an arbitrary (key, value) pair from the object
        state holder. popitem() is useful to destructively iterate over a
        object state holder, as often used in set algorithms. If the object
        state holder is empty, calling popitem() raises a KeyError.

            >>> osh = ObjStateHolder('ipaddress', {'name': '1.1.1.1'})
            >>> osh.popitem()
            (u'name', u'1.1.1.1')
            >>> osh.popitem()
            Traceback (most recent call last):
                ...
            KeyError: 'popitem(): dictionary is empty'

        @return tuple (key, value) pair
        @exception KeyError Raised if object state holder is empty
        """
        try:
            elem = next(self.iteritems())
        except StopIteration:
            msg = 'popitem(): dictionary is empty'
            raise KeyError(msg)

        del self[elem[0]]
        return elem

    def clear(self):
        """Remove all attributes from the object state holder.

            >>> osh = ObjStateHolder('ipaddress', {'name': '1.1.1.1'})
            >>> bool(osh)
            True
            >>> osh.clear()
            >>> bool(osh)
            False

        """
        self.osh.removeAttributeAll()

    def update(self, *args, **kwargs):
        """Update the object state holder with key/value pairs, overwriting
        existing keys. update() accepts either another ObjectStateHolder or an
        iterable of key/value pairs (as tuples or other iterables of length
        two). If keyword arguments are specified, the dictionary is then
        updated with those key/value pairs.

            >>> osh1 = ObjStateHolder('ipaddress', {'name': '1.1.1.1'})
            >>> osh1['name']
            u'1.1.1.1'
            >>> osh2 = ObjStateHolder('ipaddress', {'name': '2.2.2.2'})
            >>> osh1.update(osh2)
            >>> osh1['name']
            u'2.2.2.2'
            >>> osh1.update(name='3.3.3.3', routing_domain='foo')
            >>> osh1['name']
            u'3.3.3.3'
            >>> osh1.update([('name', '4.4.4.4'), ('routing_domain', 'bar')])
            >>> osh1['name']
            u'4.4.4.4'
            >>> osh1['routing_domain']
            u'bar'
            >>> osh1.update([('name', noneattr(STRING_DEF))])
            >>> osh1['name'] is None
            True
            >>> osh1.update(routing_domain=noneattr(STRING_DEF))
            >>> osh1['routing_domain'] is None
            True

        value can be a tuple or iterable of length two to provide the value
        type.

        @param *args One argument as iterable of length two or an
            ObjectStateHolder
        @param **kwargs key/value pairs as keyword arguments
        @exception TypeError Raised if *args[0] is not iterable or if any
            element of *args[0] is not a sequence
        @exception ValueError Raised if length of an element of *args[0] is not
            two
        """
        argc = len(args)
        if argc > 1:
            msg = 'update expected at most 1 arguments, got %d' % (argc)
            raise TypeError(msg)
        elif argc == 1:
            arg = args[0]
            if isinstance(arg, ObjectStateHolder):
                iterable = ((elem.getName(), elem.getValue())
                            for elem in arg.getAttributeAll())
            else:
                iterable = arg
                for method in ('iteritems', 'items'):
                    try:
                        iterable = getattr(arg, method)()
                        break
                    except AttributeError:
                        continue

            for i, elem in enumerate(iterable):
                length = len(elem)
                if length != 2:
                    msg = 'object state holder update sequence element #{} has length {}; 2 is required'
                    raise ValueError(msg.format(i, length))
                key, value = elem
                self.setattr(key, value)

        for key, value in kwargs.iteritems():
            self.setattr(key, value)

    def setdefault(self, key, default=None):
        """If key is in the object state holder, return its value. If not,
        insert key with a value of default and return default. default defaults
        to None.

            >>> osh = ObjStateHolder('ipaddress', {'name': '1.1.1.1'})
            >>> osh.setdefault('name', '2.2.2.2')
            u'1.1.1.1'
            >>> osh.setdefault('routing_domain', 'foo')
            'foo'
            >>> osh.setdefault('routing_domain', 'bar')
            u'foo'

        @param key the attribute name as defined in UCMDB
        @param default value to set and return if key is not in the object
            state holder
        @return java.lang.Object
        """
        try:
            return self[key]
        except KeyError:
            self.setattr(key, default)
            return default

    def __nonzero__(self):
        """Implements truth value testing and the built-in operation bool().

            >>> osh = ObjStateHolder()
            >>> bool(osh)
            False
            >>> osh['name'] = '1.1.1.1'
            >>> bool(osh)
            True

        @return bool True if object state holder has no attribute defined,
            False otherwise.
        """
        return not self.osh.getAttributeAll().isEmpty()

    def __hash__(self):
        """Called by built-in function hash() and for operations of members of
        hashed collections.
        @return integer Returns a hash code to identify this object. If not
            available, returns a hash code for the parent object.
        """
        return self.osh.hashCode()

    def copy(self):
        """Return a shallow copy of the instance.
        @return java.lang.Object A clone of the instance
        @exception java.lang.CloneNotSupportedException
        """
        osh = self.osh.clone()
        return self.__class__(osh)

    @classmethod
    def fromkeys(cls, seq, value=None):
        """Create a new ObjStateHolder with keys from seq and values set to
        value. fromkeys() is a class method that returns a new ObjStateHolder.
        value defaults to None.
        @param seq   Sequence of attributes
        @param value Attribute value, defaults to None
        @return ObjStateHolder
        """
        osh = cls()
        for key in seq:
            osh.setattr(key, value)
        return osh

    def has_key(self, key):
        """Test for the presence of key in the map.
        @param key the attribute name
        @return bool
        @deprecated has_key() is deprecated in favor of `key in self`.
        """
        return key in self

    def __str__(self):
        """String representation of this instance."""
        attrs = []
        for attr in self.osh.getAttributeAll():
            key = attr.getName()
            val = attr.getValue()
            if attr.isList():
                val = list(val)
            attrs.append(': '.join((repr(key), repr(val))))
        attrs.sort()

        ret = 'ObjStateHolder({clsname}, {{{attrs}}})'
        return ret.format(
            clsname=self.osh.getObjectClass(),
            attrs=', '.join(attrs))

    def __repr__(self):
        """Representation of this instance."""
        ret = "<{clsmod}.{clsname} '{clstype}' at {pos:x}>"
        return ret.format(
            clsmod=self.__class__.__module__,
            clsname=self.__class__.__name__,
            clstype=self.osh.getObjectClass(),
            pos=id(self))

    def issubstate(self, other):
        """Tests whether every attribute of self is in other and has the same
        value.

            >>> osh1 = ObjStateHolder('ipaddress', {'name': '1.1.1.1'})
            >>> osh2 = ObjStateHolder('ipaddress', {
            ...     'name': '1.1.1.1',
            ...     'routing_domain': 'foo'
            ... })
            >>> osh3 = ObjStateHolder('node')
            >>> osh2.issubstate(osh1)
            False
            >>> osh1.issubstate(osh2)
            True
            >>> osh2['name'] = '2.2.2.2'
            >>> osh1.issubstate(osh2)
            False
            >>> osh3.issubstate(osh1)
            False

        @param other An ObjectStateHolder to compare to
        @return bool True if every attribute of self is also defined in other
            and has the same value, False otherwise
        """
        if self.osh.getObjectClass() != other.getObjectClass():
            return False
        elif not other.getAttributeAll().containsAll(self.osh.getAttributeAll()):
            return False
        return True

    def issuperstate(self, other):
        """Test whether every attribute of other is in self and has the same
        value. Opposite of issubstate().

        @param other An ObjectStateHolder to compare to
        @return bool True if every attribute of other is also defined in self
            and has the same value, False otherwise
        """
        if self.osh.getObjectClass() != other.getObjectClass():
            return False
        elif not self.osh.getAttributeAll().containsAll(other.getAttributeAll()):
            return False
        return True
