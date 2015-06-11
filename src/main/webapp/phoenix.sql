-- -- 创建表
CREATE TABLE mysql_hbase_test.scm_order (docno varchar(45) not null primary key,supplierid bigint,supplierstoreid bigint,supplieruserid bigint,customerid bigint, customerstoreid bigint,customername varchar(45),customeruserid bigint,consignee varchar(45),address varchar(45),mobile varchar(45),postcode varchar(45),addressname varchar(45),date DATE,totalnum decimal(10,0),totalmny decimal(10,2),status INTEGER,tag varchar(100),receivedate DATE) SALT_BUCKETS=16
CREATE TABLE mysql_hbase_test.scm_order_b (docno varchar(45) not null,productid bigint not null,price decimal(10,2),num decimal(10,0),mny decimal(10,2), CONSTRAINT pk PRIMARY KEY (docno, productid)) SALT_BUCKETS=16

-- -- 删除表
DROP TABLE IF EXISTS mysql_hbase_test.scm_order_b
DROP TABLE IF EXISTS mysql_hbase_test.scm_order

-- -- 创建索引
CREATE INDEX IF NOT EXISTS idx_scm_order_supplierstoreid ON mysql_hbase_test.scm_order(supplierstoreid, status, date) -- SALT_BUCKETS=16
CREATE INDEX IF NOT EXISTS idx_scm_order_customerstoreid ON mysql_hbase_test.scm_order(customerstoreid, status, date) -- SALT_BUCKETS=16
CREATE INDEX IF NOT EXISTS idx_scm_order_date ON mysql_hbase_test.scm_order(date DESC) -- SALT_BUCKETS=16

CREATE INDEX IF NOT EXISTS idx_scm_order_b_productid ON mysql_hbase_test.scm_order_b(productid) -- SALT_BUCKETS=16
CREATE INDEX IF NOT EXISTS idx_scm_order_b_docno ON mysql_hbase_test.scm_order_b(docno)
-- -- 修改索引
-- 重建索引
ALTER INDEX IF EXISTS idx_scm_order_b_productid ON mysql_hbase_test.scm_order_b REBUILD
-- -- 删除索引
DROP INDEX IF EXISTS idx_scm_order_supplierstoreid ON mysql_hbase_test.scm_order
DROP INDEX IF EXISTS idx_scm_order_customerstoreid ON mysql_hbase_test.scm_order
DROP INDEX IF EXISTS idx_scm_order_date ON mysql_hbase_test.scm_order
DROP INDEX IF EXISTS idx_scm_order_b_productid ON mysql_hbase_test.scm_order_b

-- -- 修改表
-- ALTER TABLE mysql_hbase_test.scm_order SET SALT_BUCKETS=16 --SALT_BUCKETS只在创建期间可用

-- -- 查询(有索引)
SELECT * from mysql_hbase_test.scm_order WHERE supplierstoreid <> 82 and status <> 0 LIMIT 1000
SELECT * from mysql_hbase_test.scm_order WHERE supplierstoreid = 82 LIMIT 1000
SELECT * from mysql_hbase_test.scm_order WHERE supplierstoreid <> 82 or status <> 0 LIMIT 100
SELECT * FROM mysql_hbase_test.scm_order_b WHERE mysql_hbase_test.scm_order_b.docno>'0000000000000004' LIMIT 100
SELECT DOCNO, count(PRODUCTID) from mysql_hbase_test.scm_order_b GROUP BY DOCNO LIMIT 1000

--有问题的查询
SELECT * FROM mysql_hbase_test.scm_order_b WHERE mysql_hbase_test.scm_order_b.productid=0000000000000004 LIMIT 100
-- -- 查询(无索引)
SELECT DOCNO, count(PRODUCTID) from mysql_hbase_test.scm_order_b GROUP BY DOCNO LIMIT 1000

-- -- 联合查询
SELECT A.supplieruserid, B.productid , B.price, B.num, B.mny 
FROM mysql_hbase_test.scm_order AS A
JOIN mysql_hbase_test.scm_order_b AS B
ON B.docno=A.docno 
WHERE A.docno='0000000000000004' 
LIMIT 100


SELECT A.supplieruserid, B.productid , B.price, B.num, B.mny 
FROM mysql_hbase_test.scm_order AS A
JOIN (SELECT docno, productid, price, num, mny FROM mysql_hbase_test.scm_order_b WHERE mysql_hbase_test.scm_order_b.docno='0000000000000004') AS B 
ON B.docno=A.docno 
LIMIT 100

-- 假设登录用户所属商家id为6，查询待确认、订单日期范围为 2014-10-1至2014-11-7期间、供应商商家id为4的采购订单
select * from mysql_hbase_test.scm_order where status in (0) and date>=to_date('2014-10-01 00:00:00.000') and date<=to_date('2014-11-07 23:59:59.999') and supplierstoreid=4 and customerstoreid=6 order by date desc

-- 假设登录用户所属商家id为4，查询待收款的销售订单
select * from mysql_hbase_test.scm_order where status in (2 , 18 , 6 , 14 , 22 , 30) and and supplierstoreid='4' order by date desc

--订单统计
select B.productid, sum(B.num), sum(B.mny) 
from mysql_hbase_test.scm_order as A 
inner join mysql_hbase_test.scm_order_b as B 
on A.docno=B.docno 
where A.date>=to_date('2014-11-07 00:00:00.000') and A.date<=to_date('2014-11-07 23:59:59.999') and A.supplierstoreid=84 and ( A.status in  (0 , 1)) 
group by B.productid

select B.productid, sum(B.num), sum(B.mny) 
from mysql_hbase_test.scm_order_b as B 
join mysql_hbase_test.scm_order as A 
on A.docno=B.docno 
where A.date>=to_date('2014-11-07 00:00:00.000') and A.date<=to_date('2014-11-07 23:59:59.999') and A.supplierstoreid=84 and ( A.status not in  (0 , 1)) 
group by B.productid

select B.productid, sum(B.num), sum(B.mny) 
from mysql_hbase_test.scm_order_b as B 
join (select docno from mysql_hbase_test.scm_order where date>=to_date('2014-11-07 00:00:00.000') and date<=to_date('2014-11-07 23:59:59.999') and supplierstoreid=84 and ( status in  (0 , 1)) ) as A 
on A.docno=B.docno 
group by B.productid

-- -- 插入数据
UPSERT INTO mysql_hbase_test.scm_order(docno, supplierid, supplierstoreid, supplieruserid, customerid, customerstoreid, customername, customeruserid, consignee, address, mobile, postcode, addressname, date, totalnum, totalmny, status, tag, receivedate) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)