a = [1, 4, 5, 6, 7]
b = [4, 6, 2, 8, 9]


# 乘积
def multiply(left, right): return left * right


result = list(map(multiply, a, b))
print("最终结果：%s" % result)

# 笛卡尔积
result2 = []
for aV in a:
    for bV in b:
        result2.append(multiply(aV, bV))

print("最终结果：%s" % result2)
