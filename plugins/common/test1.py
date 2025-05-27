from datetime import datetime
from dateutil import relativedelta

now = datetime(year=2023, month=3, day=30)

print('현재시간:' + str(now))
print('------------월 연산------------')
print(now + relativedelta.relativedelta(month=1))
print(now.replace(month=1))
print(now + relativedelta.relativedelta(months=-1))

print('------------일 연산------------')
print(now + relativedelta.relativedelta(day=1))
print(now.replace(day=1))
print(now + relativedelta.relativedelta(days=-1))

print('------------연산 여러개-----------')
print(now + relativedelta.relativedelta(months=-1) + relativedelta.relativedelta(days=-1))