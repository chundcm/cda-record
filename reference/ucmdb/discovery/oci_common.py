# coding=utf-8
__author__ = 'Kane'


def new_vector():
    from appilog.common.system.types.vectors import ObjectStateHolderVector
    return ObjectStateHolderVector()


def new_osh(osh_type):
    from appilog.common.system.types import ObjectStateHolder
    return ObjectStateHolder(osh_type)


def new_osh_attribute(key, value, valueType):
    from appilog.common.system.types import AttributeStateHolder
    return AttributeStateHolder(key, value, valueType)
