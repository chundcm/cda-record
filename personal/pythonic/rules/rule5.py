# ----------- 通过添加空行让代码布局更优雅 -----------------
# 优雅布局的代码可读性更好
# 1）在一组代码表达完一个完整的思路之后，应该用空白行进行间隔
# 2）尽量保持上下文语义的易理解性
# 3）避免过长的代码行，每行最好不要超过80个字符
# 4）不要为了保持水平对齐而使用多余的空格，其实使阅读者尽可能容易地理解代码所要表达的意义更重要。
#     X =                   5
#     Year =             2013
#     name =            "Jam"
#     d2 = {'spam': 2, 'eggs': 3}
# 5）空格的使用要能够在需要强调的时候警告读者，在疏松关系的实体间起到分隔作用，而在具有紧密关系的时候不要使用空格。
#     推荐：if x == 4: print x, y; x, y = y, x
#   不推荐：if x == 4 : print x, y ; x , y = y , x
#

# Good:
import random

guesses_made = 0
name = input("Hello ! What is your name?\n")
number = random.randint(1, 20)
print("Well, {0}, I am thinking of a number between 1 and 20.".format(name))

guess = -1
while guesses_made < 6:
    guess = int(input('Take a guess: '))
    guesses_made += 1
    if guess < number:
        print("Your guess is too low.")
    if guess > number:
        print("Your guess is too high.")
    if guess == number:
        break
if guess == number:
    print("Good job, {0}! You guessed my number in {1} guesses!".format(name, guesses_made))
else:
    print("Nope. The number I was thinking of was {0}".format(number))

# 不优雅的代码如下
# Bad:
"""
# Python 2.7
import random
guesses_made = 0
name = raw_input("Hello! What is your name?\n")
number = random.randint(1, 20)
print 'Well, {0}, I am thinking of a number between 1 and 20.'.format(name)
while guesses_mode < 6:
    guess = int(raw_input('Take a guess: '))
    guesses_mode += 1
    if guess < number:print 'Your guess is too low.'
    if guess > number:print 'Your guess is too high.'
    if guess == number:break
if guess == number:print 'Good job, {0}! You guessed my number in {1} guesses!'
    .format(name, guesses_made)
else:print 'Nope. The number I was thinking of was {0}'.format(number)
"""
