s = ["a", "a+", "c", "a+", "d", "c", "d", "a+", "e", "f", "d", "f", "a+", "f", "d"]

for index, item in enumerate(s):
    s[index] = item.replace("a+", "a")
print(s)
