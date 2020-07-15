from math import pow


def inputNumber(flag):
    isWrong = True
    inputValue = -1
    while isWrong:
        inputValue = input("请输入您的" + flag + "\n").strip()
        if inputValue.replace(".", '').isdigit():
            # 判断输入是整数还是浮点
            if inputValue.count(".") == 0:
                # 整数
                inputValue = int(inputValue)
            elif inputValue.count(".") == 1:
                # 浮点数
                inputValue = float(inputValue)
            isWrong = False
    return inputValue


height = inputNumber("身高（m）")
weight = inputNumber("体重（kg）")
bmi = weight / pow(height, 2)

if bmi <= 18.5:
    print("过轻")
elif 18.5 < bmi <= 25:
    print("正常")
elif 25 < bmi <= 28:
    print("过重")
elif 28 < bmi <= 32:
    print("肥胖")
else:
    print("严重肥胖")
