# coding=utf-8

class JsonStreamWrapper:
    def __init__(self, iter):
        self.iter = iter

    def __iter__(self):
        # Get each byte of json from stream and process the data by counting the {}
        cnt = 0
        strBuffer = ''
        started = False
        headLegal = False
        lastItem = ''
        for item in self.iter:
            if not started and item == '{':
                headLegal = True
            started = True
            if not headLegal and lastItem == '}' and item == '{':
                headLegal = True
                cnt = 0
                strBuffer = ''
            if headLegal:
                strBuffer += item
                if item == '{':
                    cnt += 1
                if item == '}':
                    cnt -= 1
                if cnt == 0:
                    print strBuffer
                    import rest_json as json
                    jsonBuf = json.loads(strBuffer)
                    yield jsonBuf
                    strBuffer = ''
            lastItem = item
