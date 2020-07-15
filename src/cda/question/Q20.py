isWrong = True
"""
开启循环输入模式，直到输入了合法的数值为止
只能输入整数，不可是浮点数
"""
while isWrong:
    age = input("--> 请输入您的年龄\n").strip()
    # 不取浮点数
    if age.isdigit():
        age = int(age)
        if age >= 18:
            print("成年了，可以去网吧了")
        else:
            print("未成年，不能入内")
        isWrong = False
    else:
        # 如果输入非数字，跳过
        continue
print("Done, 成功退出")
