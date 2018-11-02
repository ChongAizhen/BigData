###需求：
订单数据表t_order：

id | date | pid | amount
---|---|---|---
1001 | 20180410 | P0001 | 2
1002 | 20180410 | P0001 | 3
1002 | 20180410 | P0002 | 3

商品信息表t_product

id | name | category_id | price
---|---|---|---
P0001 | 小米5 | C01 | 2
P0002 | 锤子T1 | C01 | 3

假设数据量巨大，两表的数据是以文件的形式存储在HDFS中，需要用mapreduce程序来实现一下SQL查询运算：
```$xslt
select a.id,a.date,b.name,b.category_id,b.price from t_order a join t_product b on a.pid = b.id
```

### 解决方法：

#### 1.reduce端join

通过将关联的条件作为map输出的key，将两表满足join条件的数据并携带数据所来源的文件信息，发往同一个reduce task，在reduce中进行数据的串联

缺点：这种方式中，join的操作是在reduce阶段完成，reduce端的处理压力太大，map节点的运算负载则很低，资源利用率不高，且在reduce阶段极易产生数据倾斜

#### 2.map端join

1、原理阐述

适用于关联表中有小表的情形；

可以将小表分发到所有的map节点，这样，map节点就可以在本地对自己所读到的大表数据进行join并输出最终结果，可以大大提高join操作的并发度，加快处理速度

2、实现示例

--先在mapper类中预先定义好小表，进行join

--引入实际场景中的解决方案：一次加载数据库或者用distributedcache