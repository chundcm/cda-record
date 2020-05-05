import matplotlib.pyplot as plt

from random_walk import RandowWalk

while True:
    # 创建一个 RandowWalk 实例，并将包含的点都绘制出来
    rw = RandowWalk(50000)
    rw.fill_walk()

    plt.figure(dpi=128, figsize=(10, 6))

    point_numbers = list(range(rw.num_points))
    plt.scatter(rw.x_values, rw.y_values, s=1, c=point_numbers, cmap=plt.cm.Blues)

    plt.scatter(0, 0, c='green', s=100)
    plt.scatter(rw.x_values[-1], rw.y_values[-1], c='red', s=100)
    """
    警告：
    MatplotlibDeprecationWarning: 
    Adding an axes using the same arguments as a previous axes currently 
    reuses the earlier instance.  In a future version, 
    a new instance will always be created and returned.  Meanwhile, 
    this warning can be suppressed, and the future behavior ensured, 
    by passing a unique label to each axes instance.plt.axes().get_xaxis().set_visible(False)
    """
    plt.axes().get_xaxis().set_visible(False)
    plt.axes().get_yaxis().set_visible(False)
    plt.show()

    keep_running = input("Make another walk ? (y/n): ")
    if keep_running == 'n':
        break
