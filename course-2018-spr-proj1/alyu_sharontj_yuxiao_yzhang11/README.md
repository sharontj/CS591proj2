# Project 2



# The purpose of the project

建房子/投资者角度，挑选



# Authors

1. Ailing Yu(alyu@bu.edu)
2. Jin Tang(sharontj@bu.edu)
3. Yuxiao Wang(yuxiao@bu.edu)
4. YunZhang(yzhang11@bu.edu)



# Datasets in use 

1. rental: zipcode 【赟】
2. University: coorodinates/city(district)  【sharon】
3. fire: zipcode/ 【已经count好了每个zipcode内fire #】
4. garden: zipcode 【sharon】
5. hospital: zipcode/coorodinates/ 【sharon】
6. Hubway: coorodinates/city(district) 【sharon】

所有数据都按某一年来算。



# Analysis

【Theo】

Hubway: coorodinates/city(district)

University: coorodinates/zipcode/city(district)

constraint1: 每个University 3km内的 # of hubway spot 每一个spot 加一分



【Erin】

Hospital: zipcode/coorodinates/

Fire: zipcode/

constraint2: 每个zipcode内  fire / # of hospital <= ？ 的加一分



【赟】

rental: city/zipcode/rental

garden: zipcode

constraint3:  每个zipcode内 



【All】

结合，评分，选择





# Coefficient Table

| Correlation                 | Correlation Coefficient |
| --------------------------- | ----------------------- |
| Rent vs education           | 0.22430                 |
| Rent vs Garden              | 0.05451                 |
| Education vs transportation | 0.04157                 |
|                             |                         |
|                             |                         |







