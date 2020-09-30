# Hiraishin
This is a REST-based service for a given `Amazon Product` dataset. This ervice will accept a query in the form of the template provided below.

### Query Template
```sql
Group By Template
SELECT<COLUMNS>,FUNC(COLUMN1)
WHERE <COLUMN1> = X
FROM<TABLE>
GROUP BY <COLUMNS>
HAVING FUNC(COLUMN1) > X
```

The service translates the query into **MapReduce jobs** and also into **Spark job**. It should run these two jobs separately and return the following in a **JSON object**:
- [x] Time taken for Hadoop MapReduce execution.
- [x] Time taken for Spark execution.
- [x] Input and output of map and reduce tasks in a chain as they are applied on the data.
- [x] Spark transformations and actions in the order they are applied.
- [x] Result of the query.


## Desccription of Input Dataset
There are four tables named as 
- product
- category
- similar
- reviews
### product

|id| ASIN | title | productGroup | salesrank | similarCount | categoriesCount | reviewCount | downloads | averageRating |
|--|------|-------|--------------|-----------|--------------|-----------------|-------------|-----------|---------------|
- **id** : The primary Key of the product table 
- **ASIN** : It is the [Amazon Standard Identification Number](https://en.wikipedia.org/wiki/Amazon_Standard_Identification_Number)
- **title** : Name/title of the product
- **productGroup** : Product group (Book, DVD, Video or Music)
- **salesrank** : Amazon [Salesrank](https://www.amazon.com/gp/help/customer/display.html?nodeId=525376)
- **similarCount** : Number of similar products 
- **categoriesCount** : Number of categories
- **reviewCount** : Number of reviews on the product
- **downloads** : Number of downloads
- **averageRating** : Average rating of a product

 ### category
 
 | id | productId | categoryName | categoryId |
 |----|-----------|--------------|------------|
- **id** : Primary key of the category table
- **productId** : Product ASIN id from product table
- **categoryName** : Name of the cateogry
- **categoryId** : Category unique identification in amazon

### similar

| id | productId | similarId |
|----|-----------|-----------|
- **id** : Primary key of the similar table
- **productId** : Product ASIN id from the product table
- **similarId** : Similar Product ASIN id from the product table

### reviews

| id | date | productId | customerId | rating | votes | helpful |
|----|------|-----------|------------|--------|-------|---------|
- **id** : Primary key of the review table
- **date** : Date of review
- **productId** : Product ASIN number from product table
- **customerId** : Customer id of the reviewer
- **rating** : Rating for the product from the reviewer
- **votes** : Number of votes to the review
- **helpful** : Number of people found the review helpful

## System Design

![System Design](https://drive.google.com/uc?export=view&id=1Rgogjy-hPas12Q9VNHoGmCMn8LQPMv2m)

### Description
1. User submits query.
2. `initiator` sends the string query to the `Driver`.
3. The `Driver` sends the query to the `Parser` and receives the parsed query.
4. `Parser` creates a file `elements.json` which stores the breakdown elements of the query.
5. `Driver` sends the parsed query to the `MRSession` and `Sparkler` to receive the query result.
6. `MRSession` initiates the `mapper` & `reducer`.
7. `MRResult` fetches the output of the MapReduce job from `hdfs` to local and sends the JSON string to `Driver`.
8. `Sparkler` executes the query and sends the result to `Driver`.
9. JSON String from `MRResult` and `Sparkler` is passed to the `Driver`.
10. `Driver` sends the JSON String to the `initiator` and it sends the response to client.

## Module Specification

1. **initiator.py :**
Links the user to the Driver for the execution of the query.
2. **Driver :**
Sends the input query to the Parser and upon receiving the parsed query sends it to the MapRed and Spark modules to get the result.
3. **Elements/MapRed :**
Executes the MapReduce job the query and sends the result to the Driver.
4. **Elements/Spark :**
Executes the Spark job the query and sends the result to the Driver.
5. **Dependencies :**
Stores the `Database schema`, `Column DataType`, `Query Elements` and `Config` files.

## Implementation Logic

### Query Parsing

For the given query  
`Select Column1, Column2, count(Column3) from Table where Column2 = X group by Column1 having count(Column3) > Y`
the [Parser] breaks down into elements as shown below and stores it into `elements.json`.

```yaml
selectColumns: 
- 'Column1'
- 'Column2'
selectFunc: 'count'
selectFuncColumn: 'Column3'
fromTable: 'Table'
whereColumn: 'Column2'
whereOperator: '='
whereValue: 'X'
groupByColumn: 'Column1'
havingThreshold: 'Y'
havingOperation: '>'
```
### MapReduce

The `MapRed` module takes in the STDIN input from the CSV file and uses the `elements.json` to get the parsed query. It goes through the input and selects the rows based on the **where** condition. A <key, value> pair is generated with keys being groupBy column values joined by space and value being aggregation column values. The key, values are separated by the tab delimiter. Reducer does the group by and excecutes the having condition to generate the output.

### Spark

The `Spark` module takes in the CSV file to create a Dataframe. It performs the transformations as per the `where` condition and `aggregate` function and generated the output.
