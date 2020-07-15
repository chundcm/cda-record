s = "12345678"
sList = list(s)
sList.reverse()

L = list(filter(lambda item: 0 == int(item) % 2, sList))
result = "".join(L)
print("最终结果：%s" % result)
