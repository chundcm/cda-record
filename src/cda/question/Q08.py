s = "2020/07/10 14:15:45"
year = s[0:4]  # 提取年
month = s[5:7]  # 提取月
day = s[8:10]  # 提取天
hour = s[11:13]  # 提取小时
min = s[14:16]  # 提取分钟
sec = s[17:19]  # 提取秒
print("年：%s,月：%s,日：%s,时：%s，分：%s，秒：%s"
      % (year, month, day, hour, min, sec))
