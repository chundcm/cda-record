cars = ['bmw', 'audi', 'toyota', 'subaru']
# 会修改原始引用
cars.sort()
print(cars)

cars = ['bmw', 'audi', 'toyota', 'subaru']
cars.sort(reverse=True)
print(cars)

cars = ['bmw', 'audi', 'toyota', 'subaru']
print(cars)

print(sorted(cars, reverse=True))

print(cars)

cars = ['bmw', 'audi', 'toyota', 'subaru']
cars.reverse()
print(cars)
print(len(cars))