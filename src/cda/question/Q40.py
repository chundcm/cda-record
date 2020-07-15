def inputFloat(flag):
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


def calcRate(range):
    if range <= 10:
        return 0.1
    elif 10 < range <= 20:
        return 0.075
    elif 20 < range <= 40:
        return 0.05
    elif 40 < range <= 60:
        return 0.03
    elif 60 < range <= 100:
        return 0.015
    else:
        return 0.01


def calcResult(input):
    if input <= 10:
        return input * calcRate(input)
    elif 10 < input <= 20:
        return calcResult(10) + (input - 10) * calcRate(input)
    elif 20 < input <= 40:
        return calcResult(20) + (input - 20) * calcRate(input)
    elif 40 < input <= 60:
        return calcResult(40) + (input - 40) * calcRate(input)
    elif 60 < input <= 100:
        return calcResult(60) + (input - 60) * calcRate(input)
    elif 100 < input:
        return calcResult(100) + (input - 100) * calcRate(input)


money = inputFloat("利润（万元）")
print("最终结果：%f 万元" % calcResult(money))
