import pandas as pd

s2 = {}
int1 = ['0', '1', '2', '3', '4', '5', '6']
str1 = [0, 1, 2, 3, 4, 5, 6]
data = []
for j in range(0, 5):
    for i in range(len(str1)):
        print(int1[i])
        print(str1[i])
        s2[int1[i]] = str1[i]
    data.append(s2)
print(data)
# import numpy as np
#
# mystr = "0"
# print(np.array(list(mystr)))
# print(np.array(mystr).tostring())
