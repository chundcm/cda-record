# ----------- 理解 Python 和 C语言的不同 -----------------
# 1）缩进和 "{}"，Python严格要求缩进，空白和Tab不要混用
# 2）单引号 ' 和双引号 "
# 3）三元操作符
# 4）switch 的几种写法

# Bad:
string1 = "He said, \"Hello\""
# Good:
string2 = 'He said, "Hello"'

# Python 中的三元
X = 0
Y = -2
print(X if X < Y else Y)

# Python 中的 switch...case 等价语法
# if...elif...else
n = 0
if n == 0:
    print("You typed zero.\n")
elif n == 1:
    print("You typed in top.\n")
elif n == 2:
    print("n is an even number\n")
else:
    print("Only single-digit numbers are allowed\n")


# 另外一种等价写法
def f(x):
    return {
        0: "You typed zero.\n",
        1: "You are in top.\n",
        2: "n is an even number\n"
    }.get(x, "Only single-digit numbers are allowed\n")
