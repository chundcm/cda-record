# -------------------- 快排 -------------------------------
def quicksort(array):
    less = []
    greater = []
    if len(array) <= 1:
        return array
    pivot = array.pop()
    for x in array:
        if x <= pivot:
            less.append(x)
        else:
            greater.append(x)
    return quicksort(less) + [pivot] + quicksort(greater)


sorted = quicksort([9, 8, 4, 5, 32, 64, 2, 1, 0, 10, 19, 27])
print(sorted)

# -------------------- 字符串格式化 -------------------------------
# Bad:
print('Hello %s!' % ('Tom',))
print('Hello %(name)s' % {'name': 'Tom'})
value = {'greet': "Hello World", 'language': "Python"}
print('%(greet)s from %(language)s.' % value)
# Good:
print('{greet} from {language}.'.format(greet="Hello World", language='Python'))

# -------------------- flask库，比较 pythonic 写法 -------------------------------
# 比较 Pythonic 的库的写法，需要安装 flask
"""运行输出：
 * Serving Flask app "rule1" (lazy loading)
 * Environment: production
   WARNING: This is a development server. Do not use it in a production deployment.
   Use a production WSGI server instead.
 * Debug mode: off
 * Running on http://127.0.0.1:5000/ (Press CTRL+C to quit)
"""
from flask import Flask

app = Flask(__name__)


@app.route('/')
def hello():
    return "Hello World!"


if __name__ == "__main__":
    app.run()
