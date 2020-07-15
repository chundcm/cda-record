required = "pl,okmijn123"
"""
基本版本
"""
password0 = input("输入密码\n")
if required == password0:
    print("密码正确，恭喜")
else:
    print("密码错误，退出")
print("------------- 分割线 --------------")
"""
循环输入版本
"""
exit = False
while not exit:
    password = input("输入密码\n")
    if required == password:
        print("密码正确，恭喜")
        exit = True
    else:
        print("对不起，密码错误！")
print("执行完成")
