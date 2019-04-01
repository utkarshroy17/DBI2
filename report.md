# COP 6726 - Assignment 3
Relational Operation

## Group Info
  - Anand Chinnappan Mani,  UFID: 7399-9125
  - Utkarsh Roy,            UFID: 9109-6657

## Instructions
```
make clean 
make test.out
./test.out [query_number 1-6]
```
## Test cases
All 6 test cases passed. Output not attached as it is too big.


## Instructions for GTest

```
make clean 
make gtest.out
./gtest.out
```

## GTest
All 6 query tests passes.

## Queries 
Following are the outputs to the queries in output.log

1. select * from partsupp where ps_supplycost < 1.03 ;

![N|Solid](https://i.imgur.com/hjPkel3.png)

2. select p_partkey(0), p_name(1), p_retailprice(7) from part where (p_retailprice > 931.01) AND (p_retailprice < 931.3);

![N|Solid](https://i.imgur.com/j2Wz9yr.png)

3. select sum (s_acctbal + (s_acctbal * 1.05)) from supplier;

![N|Solid](https://i.imgur.com/x10fkGn.png)

4. select sum (ps_supplycost) from supplier, partsupp where s_suppkey = ps_suppkey;

![N|Solid](https://i.imgur.com/vHHrGdq.png)

5. select distinct ps_suppkey from partsupp where ps_supplycost < 100.11;

![N|Solid]( https://i.imgur.com/pKMGXke.png)

6. select sum (ps_supplycost) from supplier, partsupp where s_suppkey = ps_suppkey groupby s_nationkey;

![N|Solid](https://i.imgur.com/p5T09BU.png)