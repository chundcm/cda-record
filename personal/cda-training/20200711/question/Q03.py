s = "上海E网_E网论坛_上海房产汽车装修亲子培训生活网"
# 直接切片
result1 = s[5:9]
# 逆序切片
result2 = s[19:15:-1]
# 分段
result3 = s[7:9] + s[20:22] + s[len(s) - 1]
print(result1 + "," + result2 + "," + result3)
