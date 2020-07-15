s = 'sadINDUionaREwSS'
result = ""
for index, ch in enumerate(s):
    if 0 == index % 2:
        # 奇数位大写
        result += ch.upper()
    else:
        # 偶数位小写
        result += ch.lower()
print("最终结果：" + result)
