motorcycles = ['honda','yamaha', 'suzuki']
print(motorcycles)

motorcycles[0] = 'ducati'
print(motorcycles)

motorcycles = ['honda','yamaha', 'suzuki']
motorcycles.append("ducati")
print(motorcycles)

motorcycles = ['honda','yamaha', 'suzuki']
motorcycles.insert(0, 'A ducati')
print(motorcycles)

motorcycles = ['honda','yamaha', 'suzuki']
print(motorcycles)
del motorcycles[0]
print(motorcycles)

motorcycles = ['honda','yamaha', 'suzuki']
print(motorcycles)
del motorcycles[1]
print(motorcycles)

motorcycles = ['honda','yamaha', 'suzuki']
print(motorcycles)

popped_motorcycle = motorcycles.pop()
print(motorcycles)
print(popped_motorcycle)

motorcycles=['honda','yamaha','suzuki','ducati']
print(motorcycles)
motorcycles.remove('ducati')
print(motorcycles)

# 测试下删除返回
motorcycles = ['A','B','C']
removed = "B"
result = motorcycles.remove(removed)
print(motorcycles)
print(result)
print("\nA " + removed.title() + " is too")
