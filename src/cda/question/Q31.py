from itertools import permutations

s = [1, 2, 3, 4]
pending = list(s)
pending.extend(s)
pending.extend(s)

selected = list(permutations(pending, 3))

result = set()
for item in selected:
    result.add("".join(map(lambda ele: str(ele), item)))
print("最终结果（数字可重复使用）：%s, \n 数量：%d" % (result, len(result)))

another = list(permutations(s, 3))
result1 = set()
for item in another:
    result1.add("".join(map(lambda ele: str(ele), item)))
print("最终结果（数字不可重复使用）：%s, \n 数量：%d" % (result1, len(result1)))
