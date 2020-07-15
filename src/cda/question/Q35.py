a = [[3, 5, 2, 4], [7, 6, 8, 8], [1, 6, 7, 7]]

result = list(map(
    lambda item: item[0:3],
    a
))
print("最终结果：%s" % result)
