# Questions_J

To be done in Spark/MapReduce
An Ecom website calculates price of any item dynamically. Dynamic price of any product is computed when user clicks on that product and it is based on prices other competitors are offering at that moment
Calculate the price of item subject to following requirements
PC => Procuring cost, Q => Cheapest price from other retailer (A product can be sold by multiple competitors)
1. If PC + MaxMargin < Q then PC + MaxMargin
2. If PC + minMargin < Q then Q
3. If PC < Q, And competitor with cheapest price is selling it in “Special” sale category then final price is Q.
4. If Q < PC && competitor with cheapest price is selling is in “Special” sale category && seller classification == “VeryHigh” then 0.9PC
5. If none of the conditions is satisfied then PC
Desired output:
ProductId
final-Price
TimeStamp
Cheapest Price amongst all Rivals
Rival name
To be done in Hive
Write hive DDLs/DMLs to load competitor’s data into partitioned hive table, where partitions are created per competitor after replacing “.” by “_” in name.
Sample table should look like productId price saleEvent fetchTS rivalName(Partitioned Column)
82545658
399.73
Special
2017-05-11 15:39:30
VistaCart_com
82545658
388.52
Regular
2017-05-11 16:09:43
ShopYourWay_com
Please assume, file “ecom_competitor_data” in ecom data folder as your input data file.
