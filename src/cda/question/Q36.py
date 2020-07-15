exclude = {2, 5, 7, 8}
for i in range(1, 11):
    found = exclude.__contains__(i)
    if not found:
        print(i)
