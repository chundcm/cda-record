def inputInt(flag):
    isWrong = True
    inputValue = -1
    while isWrong:
        inputValue = input("请输入" + flag + "\n").strip()
        if inputValue.isdigit():
            inputValue = int(inputValue)
            isWrong = False
    return inputValue


while True:
    guess = inputInt("猜测值")
    if 8 < guess:
        print("%d 比目标数值大" % guess)
    elif 8 > guess:
        print("%d 比目标数值小" % guess)
    else:
        print("恭喜，您猜中了")
        break
print("执行完成")
