# Questions_J

## Seller_Dynamic_Rate

To be done in Spark/MapReduce
An Ecom website calculates price of any item dynamically. Dynamic price of any product is computed when user clicks on that product and it is based on prices other competitors are offering at that moment
Calculate the price of item subject to following requirements
PC => Procuring cost, Q => Cheapest price from other retailer (A product can be sold by multiple competitors)
1. If PC + MaxMargin < Q then PC + MaxMargin
2. If PC + minMargin < Q then Q
3. If PC < Q, And competitor with cheapest price is selling it in “Special” sale category then final price is Q.
4. If Q < PC && competitor with cheapest price is selling is in “Special” sale category && seller classification == “VeryHigh” then 0.9PC
5. If none of the conditions is satisfied then PC

## To be done in Hive
Write hive DDLs/DMLs to load competitor’s data into partitioned hive table, where partitions are created per competitor after replacing “.” by “_” in name.
Sample table should look like productId price saleEvent fetchTS rivalName(Partitioned Column)

Please assume, file “ecom_competitor_data” in ecom data folder as your input data file.
