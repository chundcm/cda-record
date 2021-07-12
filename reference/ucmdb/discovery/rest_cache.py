# coding=utf-8
from __future__ import with_statement

import hashlib
import os
import pickle


class Hash(object):
    @staticmethod
    def dict_hash(data):
        new_data = {}
        for key, value in data.iteritems():
            new_data[key] = Hash.common_hash(value)
        return frozenset(new_data.items())

    @staticmethod
    def seq_hash(data):
        return tuple(map(Hash.common_hash, data))

    @staticmethod
    def common_hash(data):
        if isinstance(data, dict):
            return Hash.dict_hash(data)
        elif isinstance(data, list) or isinstance(data, tuple):
            return Hash.seq_hash(data)
        return data


class FolderCache(object):
    def __init__(self, folder):
        super(FolderCache, self).__init__()
        self.folder = folder
        if not os.path.exists(self.folder):
            os.mkdir(self.folder)

    def __call__(self, f):
        def wrapper(*args, **kwargs):
            input = pickle.dumps((args, kwargs))
            name = hashlib.md5(input).hexdigest()
            fpath = os.path.join(self.folder, name)
            if os.path.exists(fpath):
                with open(fpath, 'rb') as cf:
                    result = pickle.load(cf)
            else:
                result = f(*args, **kwargs)
                with open(fpath, 'wb') as cf:
                    pickle.dump(result, cf)
            return result

        return wrapper


class ZipCache(object):
    def __init__(self, path, call_now=True):
        super(ZipCache, self).__init__()
        self.path = path
        self.call_now = call_now
        self.cache = None

    def _hash_key(self, data):
        return Hash.common_hash(data)

    def read_entries(self):
        from  zipfile import ZipFile
        if not os.path.exists(self.path):
            return None
        zf = None
        try:
            zf = ZipFile(self.path)
            files = zf.NameToInfo
            if 'cache' not in files:
                return None
            else:
                content = zf.read('cache')
                return pickle.loads(content)
        finally:
            if zf:
                zf.close()

    def read_entry(self, input):
        if self.cache is None:
            self.cache = self.read_entries() or {}
        input = (input[0], self._hash_key(input[1]))
        return self.cache.get(input)

    def write_entry(self, input, output):
        input = (input[0], self._hash_key(input[1]))
        self.cache[input] = output
        from  zipfile import ZipFile, ZIP_DEFLATED
        zf = None
        try:
            zf = ZipFile(self.path, 'w', ZIP_DEFLATED)
            content = pickle.dumps(self.cache)
            zf.writestr('cache', content)
        finally:
            if zf:
                zf.close()

    def __call__(self, f):
        def wrapper(*args, **kwargs):
            input = (args, kwargs)
            result = self.read_entry(input)
            if result is None and self.call_now is True:
                result = f(*args, **kwargs)
                self.write_entry(input, result)

            return result

        return wrapper


def empty(f):
    def wrapper(*args, **kwargs):
        return f(*args, **kwargs)

    return wrapper


_cache_path = None
_call_now = True


def enable_cache(cache_path, call_now=True):
    global _cache_path
    global _call_now
    _cache_path = cache_path
    _call_now = call_now


def default_cache():
    if not _cache_path:
        return empty
    if _cache_path.endswith('.zip'):
        return ZipCache(_cache_path, _call_now)
    return FolderCache(_cache_path)
