# 记得打开运行中的 run console 选项，可查看变量结构
# 直接使用 numpy 创建数组
import numpy as np

L = [[1, 2], [3, 4]]
A = np.array(L)

# 1.先预定义列表d1,元组d2,嵌套列表d3、d4和嵌套元组d5
d1 = [1, 2, 3, 4, 0.1, 7]  # 列表
d2 = (1, 2, 3, 4, 2.3)  # 元组
d3 = [[1, 2, 3, 4], [5, 6, 7, 8]]  # 嵌套列表,元素为列表
d4 = [(1, 2, 3, 4), (5, 6, 7, 8)]  # 嵌套列表,元素为元组
d5 = ((1, 2, 3, 4), (5, 6, 7, 8))  # 嵌套元组
# 2.导入Numpy,并调用其中的array函数,创建数组
d11 = np.array(d1)
d21 = np.array(d2)
d31 = np.array(d3)
d41 = np.array(d4)
d51 = np.array(d5)
# 3.删除d1、d2、d3、d4、d5变量
del d1, d2, d3, d4, d5

z1 = np.ones((3, 3))  # 创建3行3列元素全为1的数组
z2 = np.zeros((3, 4))  # 创建3行4列元素全为0的数组
z3 = np.arange(10)  # 创建默认初始值为0,默认步长为1,末值为9的一维数组
z4 = np.arange(2, 10)  # 创建默认初始值为2,默认步长为1,末值为9的一维数组
z5 = np.arange(2, 10, 2)  # 创建默认初始值为2,步长为2,末值为9的一维数组

d1 = [1, 2, 3, 4, 0.1, 7]  # 列表
d3 = [[1, 2, 3, 4], [5, 6, 7, 8]]  # 嵌套列表,元素为列表

d12 = np.array(d1)  # 将d1列表转换为一维数组,结果赋值给变量d12
d32 = np.array(d3)  # 将d3嵌套列表转换为二维数组,结果赋值给变量d32
del d1, d3  # 删除d1、d3
s11 = d12.shape  # 返回一维数组d11的尺寸,结果赋值给变量s11
s31 = d32.shape  # 返回二维数组d31的尺寸,结果赋值给变量s31

r = np.array(range(9))
r1 = r.reshape((3, 3))
