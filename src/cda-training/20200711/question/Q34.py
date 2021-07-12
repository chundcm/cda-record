for i in range(1, 10):
    for j in range(1, i + 1):
        d = i * j
        print('%dx%d=%-2d' % (i, j, d), end=' ')
    print()
