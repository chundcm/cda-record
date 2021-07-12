isWrong = True
"""
开启循环输入模式，直到输入了合法的数值为止
1）字母和中文非法
2）带有 . 两次以上的字符串非法（非法浮点数）
*：浮点数和整数都为合法
"""
while isWrong:
    data = input("--> 请输入数据\n").strip()
    # 去小数点判断是否合法数值
    if data.replace(".", '').isdigit():
        # 判断输入是整数还是浮点
        result = -1
        if data.count(".") == 0:
            # 整数
            result = int(data) * 5 + 8
        elif data.count(".") == 1:
            # 浮点数
            result = float(data) * 5 + 8

        # 还有一种例外：0.444.56 属于非法，重新输入
        if -1 == result:
            continue
        else:
            print("<-- 您的结果是 %d" % result)
            isWrong = False
    else:
        # 如果输入非数字，跳过
        continue
print("Done, 成功退出")
