list = ["a", 1, True, [33, 5], 6, 8]
# 直接截取
result1 = list[4:6]
print(result1)

# 逆向截取
result2 = list[5:3:-1]
result2.reverse()  # 一定要逆序
print(result2)

# 范围计算
result3 = []
for i in range(4, 6):
    result3.append(list[i])
print(result3)
