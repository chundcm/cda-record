s = "sadINDUionaREwSS"

length = len(s)
middle = 0
if 0 == length % 2:
    # 偶数
    middle = length // 2
else:
    # 奇数
    middle = length // 2 + 1

result = s[:middle].upper() + s[middle:length].lower()
print("最终结果：" + result)
