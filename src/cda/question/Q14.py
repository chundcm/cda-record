List = [
    '列表',
    ['Dell', 'Apple', 'HUAWEI'],
    ('Java', 'Python', 'Go'),
    {'土豆': '一块三', '番茄': "两块四"},
    True]
# 索引出 HUAWEI
i = -1
j = -1
for i, ch in enumerate(List):
    if isinstance(ch, list):
        if 0 < ch.count("HUAWEI"):
            j = ch.index("HUAWEI")
            break
print("位置：(i=%d,j=%d)" % (i, j))
value = List[i][j]
print("值：%s" % value)
# 提取出 Java
found = None
for ch in List:
    if isinstance(ch, tuple):
        if 0 < ch.count("Java"):
            index = ch.index("Java")
            found = ch[index]
            break
print("找到：%s" % found)
# 提取出 Java
price = None
for ch in List:
    if isinstance(ch, dict):
        price = ch['番茄']
        if price is not None:
            break
print("番茄价格：%s" % price)
