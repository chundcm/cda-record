import math


def distribute(x):
    return (1 / math.sqrt(2 * math.pi)) * math.exp(- (x ** 2) / 2)


print(distribute(1))
print(distribute(2))
print(distribute(3))
