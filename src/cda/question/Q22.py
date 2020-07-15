def inputInt(flag):
    isWrong = True
    inputValue = -1
    while isWrong:
        inputValue = input("请输入" + flag + "\n").strip()
        if inputValue.isdigit():
            inputValue = int(inputValue)
            isWrong = False
    return inputValue


"""
总共4中情况
          2     3
情况1：    v     v
情况2：    v     x
情况3：    x     v
情况4：    x     x

题目分析：
题目一：情况1
题目二：情况1、情况2
题目三：情况1、情况3
题目四：情况2、情况3、情况4
"""
intV = inputInt("整数")
if 0 == intV % 2:
    print("该数可以被 2 整除")
if 0 == intV % 3:
    print("该数可以被 3 整除")

if 0 == intV % 2 and 0 == intV % 3:
    print("该数既可以被 2 整除，也可以被 3 整除")
else:
    print("该数不可以被 2 或 3 整除")
print("执行完成")
