def printLine(i):
    line = []
    for star in range(0, i):
        line.append("*")
    return " ".join(line)


for i in range(1, 10):
    line = ""
    if i <= 5:
        line = printLine(i)
    else:
        line = printLine(10 - i)
    print(line)
