# ---------------- 避免劣化代码 ----------------
"""
1）避免只用大小写来区分不同的对象
2）避免使用容易引起混淆的名称，如 element, list, dict
3）不要害怕过长的变量名（不追求过分缩写）
"""


# Bad:
# 不推荐使用 list, element 这种变量名
def funA(list, num):
    for element in list:
        if num == element:
            return True
        else:
            pass


# Good:
def find_num(search_list, num):
    for listValue in search_list:
        if num == listValue:
            return True
        else:
            pass


# ---------------- 深入认识Python ----------------
"""
不好的风格：
if foo == 'blah': do_blah_thing()
do_one(); do_two(); do_three()

Pythonic风格：
if foo == 'blah':
    do_blah_thing()
do_one()
do_two()
do_three()
"""
