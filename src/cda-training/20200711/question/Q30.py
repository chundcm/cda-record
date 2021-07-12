# 生成数列
num = list(range(0, 10))
numStr = list(map(lambda ele: str(ele), num))
print(" ".join(numStr))

# 追加 over
overStr = list(numStr)
overStr.append("over")
print(" ".join(overStr))

# 间隔 over
divStr = list(numStr)
divStr = list(map(lambda ele: str(ele) + " over", divStr))
print(" ".join(divStr))
