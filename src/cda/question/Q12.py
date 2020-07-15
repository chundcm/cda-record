aa = [1, 'a', [2, 5], 88, 99, '倒数第二', '倒数第一']
# 1. 判断"a"
exist = aa.count('a')
if 0 == exist:
    print("a 不存在！")
else:
    print("a 存在于列表中！")

# 2.删除
copy = list(aa)
copy.remove(copy[3])
print(copy)

# 3.删除aa元素中的[2,5]
copy = list(aa)
ref = copy[2:3][0]
ref.remove(5)
print(copy)
