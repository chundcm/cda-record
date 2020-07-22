a = [[1, 4, 5], [6, 7, 6]]
b = [[4, 6, 2], [8, 9, 7]]

result = list(map(
    lambda item: list(map(
        lambda each: each[0] * each[1],
        zip(item[0], item[1])
    )),
    zip(a, b)
))
print("最终结果：%s" % result)
