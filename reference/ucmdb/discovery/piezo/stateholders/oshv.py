# coding=utf-8

"""@package piezo.stateholders.oshv
Wrapper around the ObjectStateHolderVector class of UCMDB API.
"""

__author__ = "Kevin Woldt"
__copyright__ = "Copyright (C) 2017"
__license__ = "MIT License"

# python API
from collections import MutableSequence
from collections import Iterable

# UCMDB JAVA API
from appilog.common.system.types import ObjectStateHolder
from appilog.common.system.types.vectors import ObjectStateHolderVector

# piezo lib (relative import)
from .osh import ObjStateHolder


class ObjStateHolderVector(MutableSequence):

    """Wrapper around ObjectStateHolderVector. Implements all ABC abstract and
    mixin methods to use the ObjectStateHolderVector like a python list.

        >>> osh = ObjStateHolder('ipaddress')
        >>> oshv = ObjStateHolderVector()
        >>> oshv.append(osh)
        >>> oshv[0] == osh
        True
        >>> len(oshv)
        1

    The dataflow probe serializes the ObjectStateHolderVector and all
    containing elements to send it to the UCMDB. Subclasses of
    ObjectStateHolder are unknown there, especially Jython subclasses. So we
    use a wrapper around ObjectStateHolder and
    ObjectStateHolderVector. The wrapper ObjStateHolderVector implicitly
    converts arguments and return values of get and set request to thei
    pythonized methods.
    """

    default_osh_class = ObjStateHolder

    def __init__(self, iterable=None, osh_class=default_osh_class):
        """Constructor.
        @param iterable A sequence of elements or a reference to an
            ObjectStateHolderVector.
        @param osh_class Get methods defined in ObjStateHolderVector will
            convert ObjectStateHolder to osh_class. Exceptions are
            attribute lookup redirections via __getattr__(). Set methods will
            extract the ObjectStateHolder if not given. Therefore the supplied
            class needs to implement a get_osh() method.
        """
        self.osh_class = osh_class
        if isinstance(iterable, ObjectStateHolderVector):
            self.oshv = iterable
        else:
            self.oshv = ObjectStateHolderVector()
            if iterable is not None:
                self.extend(iterable)

    def _convert_osh(self, osh):
        """Convert ObjectStateHolder to requested class.
        @param osh ObjectStateHolder
        @return Create a new instance of requested class and pass the
            ObjectStateHolder as first argument.
        """
        return self.osh_class(osh)

    @staticmethod
    def _extract_osh(item):
        """Extracts the ObjectStateHolder of given object.
        @param Ant kind of object implementing a get_osh() method.
        @return ObjectStateHolder The extracted ObjectStateHolder
        """
        if not isinstance(item, ObjectStateHolder):
            item = item.get_osh()
        return item

    def _create_instance(self):
        """Creates a new instance with same osh_class defined.
        @return ObjStateHolderVector new instance
        """
        return self.__class__(osh_class=self.osh_class)

    def __getattr__(self, name):
        """Redirecting all failed attribute lookups to the ObjectStateHolderVector.
        @param name attribute to lookup
        @exception AttributeError Raised if does not exists in
            ObjectStateHolderVector
        """
        return getattr(self.oshv, name)

    def __len__(self):
        """Called to implement the built-in function len(). Returns the length
        of the vector, an integer >= 0. Use with cation, can be inefficient.

            >>> oshv = ObjStateHolderVector()
            >>> len(oshv)
            0

        @return integer Number of elements in vector.
        """
        return self.oshv.size()

    def __iter__(self):
        """Return the iterator object itself.
        @return An iterator object.
        """
        return (self._convert_osh(osh) for osh in self.oshv)

    def __contains__(self, item):
        """Called to implement membership test operators. Return true if item
        is in self, false otherwise.

            >>> osh = ObjStateHolder('ipaddress')
            >>> oshv = ObjStateHolderVector()
            >>> oshv.append(osh)
            >>> osh in oshv
            True

        @param item The item to test membership for
        @return True if item is a member of the vector, false otherwise.
        """
        osh = self._extract_osh(item)
        return self.oshv.contains(osh)

    def _getidx(self, key, slicing=False, bounds=True):
        """Calculates the index based on the given key.
        @param key A positive or negative integer or a slice object.
        @param slicing Indicates if key is allowed to be a slice object.
        @param bounds Indicates whether IndexError should be raised if index is
            out of bounds or key should be truncated to 0 or len(self)
        @return key itself if it is positive or a calculated positive
            integer if key is negative or a iterator of a list of positive
            integers if key is a slice object.
        @exception IndexError if key is out of range.
        @exception TypeError if key is not an integer or slice object.
        """
        if isinstance(key, slice) and slicing is False:
            msg = 'vector indices must be integers, not %s' % (type(key))
            raise TypeError(msg)

        if isinstance(key, slice):
            return xrange(*key.indices(len(self)))
        elif isinstance(key, int):
            length = len(self)
            if key < 0:
                idx = length + key
            else:
                idx = key

            if bounds and (idx < 0 or idx >= length):
                msg = 'vector index out of range'
                raise IndexError(msg)
            elif idx < 0:
                idx = 0
            elif idx >= length:
                idx = max(0, length - 1)
            return idx
        else:
            msg = 'vector indices must be integers, not %s' % (type(key))
            raise TypeError(msg)

    def __getitem__(self, key):
        """Called to implement evaluation of self[key]. The accepted keys are
        integers and slice objects. If key is of an inappropriate type,
        TypeError is raised; if of a value outside the set of indexes for the
        sequence (after interpretation of negative values), IndexError is
        raised.

            >>> ip_osh = ObjStateHolder('ipaddress')
            >>> node_osh = ObjStateHolder('node')
            >>> oshv = ObjStateHolderVector((ip_osh, node_osh))
            >>> oshv[0] == ip_osh
            True
            >>> oshv[0:2][0] == ip_osh
            True
            >>> oshv[2]
            Traceback (most recent call last):
                ...
            IndexError: vector index out of range
            >>> oshv['ipaddress']
            Traceback (most recent call last):
                ...
            TypeError: vector indices must be integers, not <type 'str'>

        @param key A positive or negative integer or a slice object.
        @return The element at given index if key is an integer; an vector if
            key is a slice object.
        @exception IndexError if key is out of range.
        @exception TypeError if key is not an integer or slice object.
        """
        idx = self._getidx(key, slicing=True)
        if isinstance(idx, Iterable):
            new_oshv = self._create_instance()
            for i in idx:
                new_oshv.oshv.add(self.oshv.get(i))
            return new_oshv
        else:
            return self._convert_osh(self.oshv.get(idx))

    def __setitem__(self, key, value):
        """Called to implement assignment to self[key]. Same note as for
        __getitem__(). The same exceptions are raised for improper key values
        as for the __getitem__() method.

            >>> ip_osh = ObjStateHolder('ipaddress')
            >>> node_osh = ObjStateHolder('node')
            >>> oshv = ObjStateHolderVector((ip_osh, node_osh))
            >>> oshv[0] == ip_osh and oshv[1] == node_osh
            True
            >>> oshv[0] = node_osh
            >>> oshv[1] = ip_osh
            >>> oshv[0] == node_osh and oshv[1] == ip_osh
            True
            >>> oshv[0:2] = (ip_osh, node_osh)
            >>> oshv[0] == ip_osh and oshv[1] == node_osh
            True

        @param key A positive or negative integer or a slice object.
        @param value The value to set.
        @return The previously stored element at given index if key is an
            integer; an vector with the previously stored elements if key is a
            slice object.
        @exception IndexError if key is out of range.
        @exception TypeError if key is not an integer or slice object.
        """
        idx = self._getidx(key, slicing=True)
        if isinstance(idx, Iterable):
            new_oshv = self._create_instance()
            for i, val in zip(idx, value):
                old = self.oshv.set(i, self._extract_osh(val))
                new_oshv.oshv.add(old)
            return new_oshv
        else:
            old = self.oshv.set(key, self._extract_osh(value))
            return self._convert_osh(old)

    def __delitem__(self, key):
        """Called to implement deletion of self[key]. Same note as for
        __getitem__(). The same exceptions are raised for improper key values
        as for the __getitem__() method.

            >>> osh = ObjStateHolder('ipaddress')
            >>> oshv = ObjStateHolderVector()
            >>> oshv.append(osh)
            >>> osh in oshv
            True
            >>> del oshv[0]
            >>> osh in oshv
            False

        @param key A positive or negative integer or a slice object.
        @return The previously stored element at given index if key is an
            integer; an vector with the previously stored elements if key is a
            slice object.
        @exception IndexError if key is out of range.
        @exception TypeError if key is not an integer or slice object.
        """
        idx = self._getidx(key, slicing=True)
        if isinstance(idx, Iterable):
            new_oshv = self._create_instance()
            for i in reversed(idx):
                old = self.oshv.remove(i)
                new_oshv.oshv.add(old)
            return new_oshv
        else:
            # TODO: remove(int) is broken; create ticket
            # old = self.oshv.remove(idx)
            old = self.oshv.get(idx)
            self.oshv.remove(old)
            return self._convert_osh(old)

    def append(self, item):
        """Add an item to the end of the vector; equivalent to
        self[len(self):] = [item]. Alias of parents add() method.

            >>> osh = ObjStateHolder('ipaddress')
            >>> oshv = ObjStateHolderVector()
            >>> oshv.append(osh)
            >>> osh in oshv
            True

        @param item Element to append.
        """
        osh = self._extract_osh(item)
        self.oshv.add(osh)

    def extend(self, iterable):
        """Extend the vector by appending all the items in the given iterable;
        equivalent to self[len(self):] = iterable. Alias of parents addAll()
        method with extended behaviour.

            >>> ip_osh = ObjStateHolder('ipaddress')
            >>> node_osh = ObjStateHolder('node')
            >>> oshv = ObjStateHolderVector()
            >>> oshv.extend([ip_osh, node_osh])
            >>> ip_osh in oshv and node_osh in oshv
            True
            >>> oshv.extend(5)
            Traceback (most recent call last):
                ...
            TypeError: 'int' object is not iterable

        @param iterable Iterable to get elements from to append.
        @exception TypeError Raised if iterable is not iterable.
        """
        if isinstance(iterable, ObjectStateHolderVector):
            self.oshv.addAll(iterable)
        else:
            for item in iterable:
                osh = self._extract_osh(item)
                self.oshv.add(osh)

    def insert(self, key, item):
        """Insert an item at a given position (same as self[key:key] = [item]).
        Alias of add(key, item) of parent class with extended behaviour.

            >>> ip_osh = ObjStateHolder('ipaddress')
            >>> node_osh = ObjStateHolder('node')
            >>> oshv = ObjStateHolderVector()
            >>> oshv.append(ip_osh)
            >>> oshv.insert(0, node_osh)
            >>> oshv[0] == node_osh
            True
            >>> oshv.insert(-1, ip_osh)
            >>> oshv[-2] == ip_osh
            True

        @param key Index of the element before which to insert, so
            self.insert(0, item) inserts at the front of the vector, and
            self.insert(len(self), item) is equivalent to self.append(item).
        @param item The item to insert.
        @exception TypeError Raised if key is not an integer.
        """
        idx = self._getidx(key, bounds=False)
        osh = self._extract_osh(item)
        self.oshv.add(idx, osh)

    def remove(self, item):
        """Remove the first item from the vector whose value is item. It is an
        error if there is no such item. This methods hides the parent remove
        method with different behaviour.

            >>> ip_osh = ObjStateHolder('ipaddress')
            >>> node_osh = ObjStateHolder('node')
            >>> oshv = ObjStateHolderVector([ip_osh, node_osh])
            >>> oshv.remove(ip_osh)
            >>> oshv[0] == node_osh
            True
            >>> oshv.remove(ip_osh)
            Traceback (most recent call last):
                ...
            ValueError: item not in vector

        @param item The item to remove.
        @exception ValueError Raised if item is not in the vector.
        """
        osh = self._extract_osh(item)
        contained = self.oshv.remove(osh)
        if contained is False:
            msg = 'item not in vector'
            raise ValueError(msg)

    def pop(self, key=-1):
        """Remove the item at the given position in the vector, and return it.
        If no index is specified, self.pop() removes and returns the last item
        in the vector. Alias of parents remove(key) method with extended
        behaviour.

            >>> ip_osh = ObjStateHolder('ipaddress')
            >>> node_osh = ObjStateHolder('node')
            >>> oshv = ObjStateHolderVector([ip_osh, node_osh])
            >>> osh = oshv.pop(0)
            >>> osh == ip_osh and oshv[0] == node_osh
            True
            >>> osh = oshv.pop()
            >>> osh == node_osh
            True
            >>> bool(oshv) is False
            True

        @param key Index of the element to remove.
        @return The removed element.
        """
        idx = self._getidx(key)
        # TODO: remove(int) in ObjectStateHolderVector is broken; create ticket
        # osh = self.oshv.remove(idx)
        osh = self.oshv.get(idx)
        self.oshv.remove(osh)
        return self._convert_osh(osh)

    def index(self, item):
        """Return the index in the vector of the first item whose value is
        item. It is an error if there is no such item. Alias of parents
        indexOf(item) method with extended behaviour.

            >>> osh = ObjStateHolder('ipaddress')
            >>> oshv = ObjStateHolderVector()
            >>> oshv.index(osh)
            Traceback (most recent call last):
                ...
            ValueError: item not in vector
            >>> oshv.append(osh)
            >>> oshv.index(osh)
            0

        @param item The element searched for in vector.
        @return integer The position of the element in the vector.
        @exception ValueError Raised if item is not in the vector.
        """
        osh = self._extract_osh(item)
        idx = self.oshv.indexOf(osh)
        if idx == -1:
            msg = 'item not in vector'
            raise ValueError(msg)
        return idx

    def count(self, item):
        """Return the number of times item appears in the vector.

            >>> osh = ObjStateHolder('ipaddress')
            >>> oshv = ObjStateHolderVector()
            >>> oshv.count(osh)
            0
            >>> oshv.append(osh)
            >>> oshv.count(osh)
            1

        @param item The element to count in vector.
        @return The number of times item appears in the vector.
        """
        cnt = 0
        for elem in self:
            if elem == item:
                cnt += 1
        return cnt

    def reverse(self):
        """Reverse the elements of the vector, in place.

            >>> ip_osh = ObjStateHolder('ipaddress')
            >>> node_osh = ObjStateHolder('node')
            >>> oshv = ObjStateHolderVector([ip_osh, node_osh])
            >>> oshv[0] == ip_osh
            True
            >>> oshv.reverse()
            >>> oshv[0] == node_osh
            True
        """
        oshv = self.oshv
        length = len(self)
        for i in xrange(length / 2):
            tmp = oshv.get(i)
            oshv.set(i, oshv.get(length - i - 1))
            oshv.set(length - i - 1, tmp)

    def copy(self):
        """Creates a shallow copy of self (same as self[:]).
        @return ObjStateHolderVector A shallow copy
        """
        new_oshv = self._create_instance()
        new_oshv.oshv.addAll(self.oshv)
        return new_oshv

    def __add__(self, other):
        """The concatenation of self and other.

            >>> ip_osh = ObjStateHolder('ipaddress')
            >>> ip_oshv = ObjStateHolderVector([ip_osh])
            >>> node_osh = ObjStateHolder('node')
            >>> node_oshv = ObjStateHolderVector([node_osh])
            >>> oshv = ip_oshv + node_oshv
            >>> oshv[0] == ip_osh and oshv[1] == node_osh
            True

        @param other ObjStateHolderVector
        @return A new ObjStateHolderVector containing all elements of self and
            other.
        """
        new_oshv = self._create_instance()
        new_oshv.oshv.addAll(self.oshv)
        new_oshv.extend(other)
        return new_oshv

    def __iadd__(self, other):
        """Extends self with the contents of other (same as
        self[len(self):len(self)] = other). These method is called to implement
        the augmented arithmetic assignment +=. These method does the operation
        in-place (modifying self) and return the result.

            >>> ip_osh = ObjStateHolder('ipaddress')
            >>> node_osh = ObjStateHolder('node')
            >>> node_oshv = ObjStateHolderVector([node_osh])
            >>> oshv = ObjStateHolderVector([ip_osh])
            >>> oshv += node_oshv
            >>> oshv[0] == ip_osh and oshv[1] == node_osh
            True

        @param other ObjStateHolderVector
        @return self
        """
        self.extend(other)
        return self

    def __mul__(self, other):
        """Equivalent to adding self to itself other times. These method is
        called to implement the binary arithmetic operations *.

            >>> ip_osh = ObjStateHolder('ipaddress')
            >>> node_osh = ObjStateHolder('node')
            >>> oshv = ObjStateHolderVector([ip_osh, node_osh])
            >>> mul_oshv = oshv * 2
            >>> mul_oshv[0] == mul_oshv[2] == ip_osh
            True
            >>> mul_oshv[1] == mul_oshv[3] == node_osh
            True

        @param other An integer or object implementing the __index__() method.
        @return A new ObjStateHolderVector contaning the elements of self
            multiple times.
        """
        new_oshv = self._create_instance()
        for _ in xrange(other.__index__()):
            new_oshv.oshv.addAll(self.oshv)
        return new_oshv

    def __rmul__(self, other):
        """Equivalent to adding self to itself other times. These method is
        called to implement the binary arithmetic operations * with reflected
        (swapped) operands. These function is only called if the left operand
        does not support the corresponding operation and the operands are of
        different types.

            >>> ip_osh = ObjStateHolder('ipaddress')
            >>> node_osh = ObjStateHolder('node')
            >>> oshv = ObjStateHolderVector([ip_osh, node_osh])
            >>> mul_oshv = 2 * oshv
            >>> mul_oshv[0] == mul_oshv[2] == ip_osh
            True
            >>> mul_oshv[1] == mul_oshv[3] == node_osh
            True

        @param other An integer or object implementing the __index__() method.
        @return A new ObjStateHolderVector contaning the elements of self
            multiple times.
        """
        return self.__mul__(other)

    def __imul(self, other):
        """Updates self with its contents repeated other times. These method is
        called to implement the augmented arithmetic assignments *=. These
        method does the operation in-place (modifying self) and return the
        result.

            >>> ip_osh = ObjStateHolder('ipaddress')
            >>> node_osh = ObjStateHolder('node')
            >>> oshv = ObjStateHolderVector([ip_osh, node_osh])
            >>> oshv *= 2
            >>> oshv[0] == oshv[2] == ip_osh
            True
            >>> oshv[1] == oshv[3] == node_osh
            True

        @param other An integer or object implementing the __index__() method.
        @return A new ObjStateHolderVector contaning the elements of self
            multiple times.
        """
        if other == 0:
            self.oshv.clear()
        elif other > 1:
            self.extends(self.__mul__(other - 1))
        return self

    def filter(self, func):
        """Scan through vector looking for locations where func returns True
        and return a dictionary with index and value of matches.
        @param func Filter function invoked for each element in vector
        @return dict Returns elements of vector for which func returns True
        """
        result = {}
        for idx, elem in enumerate(self):
            if func(elem) is True:
                result[idx] = elem
        return result

    def filterfalse(self, func):
        """Complementary function of filter.
        @param func Filter function invoked for each element in vector
        @return dict Returns elements of vector for which func returns False
        """
        result = {}
        for idx, elem in enumerate(self):
            if func(elem) is False:
                result[idx] = elem
        return result

    def __str__(self):
        """Return string representation like a list."""
        items = (str(item) for item in self)
        return '[{}]'.format(', '.join(items))
