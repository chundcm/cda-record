import json
import math

import pygal

# 将数据加载到一个列表中
filename = 'btc_close_2017.json'
with open(filename) as f:
    btc_data = json.load(f)
dates = []
months = []
weeks = []
weekdays = []
close = []
# 打印每一天的信息
for btc_dict in btc_data:
    dates.append(btc_dict['date'])
    months.append(int(btc_dict['month']))
    weeks.append(int(btc_dict['week']))
    weekdays.append(btc_dict['weekday'])
    close.append(int(float(btc_dict['close'])))

line_chart = pygal.Line(x_label_ratation=20, show_minor_x_labels=False)
line_chart.title = "收盘价（￥）"
line_chart.x_labels = dates
N = 20  # x轴坐标每隔20天显示一次
line_chart.x_labels_major = dates[::N]
# 半对数变换：semi-logarithmic
close_log = [math.log10(_) for _ in close]
line_chart.add('收盘价', close_log)
line_chart.render_to_file("收盘价折现图（￥）.svg")
