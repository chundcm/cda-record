import matplotlib.pyplot as plt

x_values = list(range(1, 1001))
y_values = [x ** 2 for x in x_values]

"""
错误信息：
'c' argument looks like a single numeric RGB or RGBA sequence, 
which should be avoided as value-mapping will have precedence in case its length matches with 'x' & 'y'.  
Please use a 2-D array with a single row if you really want to specify the same RGB or RGBA value for all points.
应该传入  [[0,0,0.8]]，替换原始的 (0,0,0.8)
"""
plt.scatter(x_values, y_values, c=y_values, s=40, cmap=plt.cm.Blues)

# 设置图标标题并给坐标轴加上标签
plt.title("Square Numbers", fontsize=24)
plt.xlabel("Value", fontsize=14)
plt.ylabel("Sequre of Value", fontsize=14)

# 设置可读标记的大小
plt.tick_params(axis="both", which="major", labelsize=14)
# 设置每个坐标轴的取值范围
plt.axis([0, 1100, 0, 1100000])
plt.savefig('squares_plot.png', bbox_inches='tight')
