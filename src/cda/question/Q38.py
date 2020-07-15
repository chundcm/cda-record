import math as np


def sigmoid(x):
    return 1 / (1 + np.exp(-x))


print(sigmoid(1))
print(sigmoid(2))
print(sigmoid(3))
