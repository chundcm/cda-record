L = [2, 5, 3, 8, 10, 1]
# 翻转输出
r1 = list(L)
r1.reverse()
print(r1)

# 升序降序
r1.sort()
print(r1)

# 降序排序
r1.sort(reverse=True)
print(r1)

# 弹出最后一个元素
last_one = r1.pop()
print("元素：%d" % last_one)
print(r1)

# 清空列表
r1.clear()
print(r1)
