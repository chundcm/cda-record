def inputInt(flag):
    isWrong = True
    inputValue = -1
    while isWrong:
        inputValue = input("请输入" + flag + "\n").strip()
        if inputValue.isdigit():
            inputValue = int(inputValue)
            isWrong = False
    return inputValue


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


money = inputFloat("身上的钱")
site = inputInt("公车空位")

if 2 <= money and 0 < site:
    print("您可以乘坐公交车！")
else:
    if 0 == site:
        print("公车上已没有位置")
    if 2 > money:
        print("对不起，您的余额不足")
print("执行完成")
