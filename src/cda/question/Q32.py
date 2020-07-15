a = [[1, 4, 5], [6, 7, 6]]
b = [[4, 6, 2], [8, 9, 7]]

aProc = sum(a, [])
bProc = sum(b, [])
result = list(map(lambda zipped: zipped[0] * zipped[1], zip(aProc, bProc)))
print("最终结果：%s" % result)
