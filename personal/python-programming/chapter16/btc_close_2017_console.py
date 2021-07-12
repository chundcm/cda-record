import requests

# serve -s personal/pythonic/pythonic-programming/chapter16/
json_url = 'http://localhost:5000/btc_close_2017.json'
req = requests.get(json_url)
# 将数据写入文件
with open('btc_close_2017_request.json', 'w') as f:
    f.write(req.text)

file_request = req.json()
print(file_request)
