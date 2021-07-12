# coding=utf-8
try:
    import json
except ImportError:
    from org.json import JSONTokener
    from org.json import JSONObject
    from org.json import JSONArray


    class json:
        @classmethod
        def loads(cls, content):
            return cls.toMap(JSONTokener(content).nextValue())

        @classmethod
        def dumps(cls, obj):
            jsonObj = cls.fromMap(obj)
            return str(jsonObj)

        @classmethod
        def toMap(cls, jo):
            if isinstance(jo, type(JSONObject.NULL)):
                return None
            elif isinstance(jo, JSONObject):
                m = {}
                y = [(name, cls.toMap(jo.get(name))) for name in jo.keys()]
                m.update(y)
                return m
            elif isinstance(jo, JSONArray):
                return [cls.toMap(jo.get(i)) for i in range(jo.length())]
            else:
                return jo

        @classmethod
        def fromMap(cls, obj):
            if isinstance(obj, dict):
                jsonObject = JSONObject()
                for k in obj:
                    jsonObject.put(k, cls.fromMap(obj[k]))
                return jsonObject
            elif isinstance(obj, list):
                jsonArray = JSONArray()
                for k in obj:
                    jsonArray.put(k)
                return jsonArray
            else:
                return obj


def loads(content):
    return json.loads(content)


def dumps(obj):
    jsonObj = json.dumps(obj)
    return str(jsonObj)
