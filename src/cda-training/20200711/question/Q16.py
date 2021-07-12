d = {'键1': '值1',
     '键2': [{'键2.1': ["值2.1"]}],
     '键3': '值3',
     '键4': '值4'}
# 列表
itemArray = d["键2"]
# 列表第一个元素（字典）
item = itemArray[0]
# 字典中取值
result = item["键2.1"]
print(result)
