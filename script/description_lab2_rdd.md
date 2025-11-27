
# Architecture du lab
```
project-root/
│
├── ecommerce/
│   ├── customers.csv
│   ├── products.csv
│   ├── orders.csv
│   
│
├── scripts/
│   └── lab2_filter.py
│   └── lab2_flatmap.py
    └── lab2_keyvalue.py
└── decription.md
```



# A.1: Basic and Advanced Filtering
## Output from lab2_filter.py 

### FILTER 1: Condition Simple
```
[FILTER 1] High-value orders (>$5000)

 Order #1: $7599.96
 Order #2: $14473.56
 Order #3: $12501.77
 Order #5: $14072.95
 Order #6: $9698.19

```
Simple filtering with orders_rdd.filter(lambda o: o['amount'] > 5000) (/script/lab2_filter.py)
### FILTER 2: Conditions Multiples (ET)

```
[FILTER 2] Shipped orders over $2000
 Order #4: Shipped - $4849.80
 Order #7: Shipped - $2170.89
 Order #12: Shipped - $6285.31

```
This filter selects shipped orders with amounts over $2000, combining two conditions using the AND operator.

### FILTER 3: Logique Complexe avec Fonction

```
[FILTER 3] Problem orders (On Hold or Cancelled, >$1000)

 #8: Cancelled - $2929.36
 #18: Cancelled - $2713.06
 #33: Cancelled - $3104.29
 #38: Cancelled - $12618.08
 #52: Cancelled - $12385.21

```
Here, we use the filter() function with a named function is_problem_order instead of a lambda expression, which improves code readability and allows clearer documentation of the complex business logic for detecting problematic orders.


````
[FILTER 4] Orders from November 2024

November 2024 orders: 218

````
Here, we use the filter() function with a named function in_november_2024 that includes error handling, allowing robust filtering the number of November 2024 orders while preventing crashes due to invalid date formats.

````
[FILTER 5] Sample 10% of orders

Sample: 483 orders (~10%)

````
Unlike filter() which selects elements based on specific business conditions, sample() performs probabilistic random sampling, useful for statistical analysis or performance testing on a representative data subset


````
[FILTER 6] Chaining multiple filters

After all filters: 170 orders
Criteria: Amount > $1000, Status = Shipped, Payment = Credit Card

````
chaining multiple filters where each condition is applied sequentially, optimized by Spark for efficient execution, enabling progressive and readable construction of complex selection criteria.


```
[FILTER 7] Exclude cancelled orders
Non-cancelled orders: 3995

```
This filter uses negation to exclude all cancelled orders, demonstrating the use of the 'not equal' operator (!=) to inversely select elements that don't match a specific criterion.



# **PART B: FLATMAP OPERATIONS 

* Objective
Understand the difference between map() and flatMap(), and when to use each.

````
======================================================================
FLATMAP vs MAP
======================================================================

[EXAMPLE 1] Understanding the difference

Using map():

[['Hello', 'World'], ['Spark', 'is', 'awesome'], ['RDD', 'operations']]
Result: Nested lists

Using flatMap():
['Hello', 'World', 'Spark', 'is', 'awesome', 'RDD', 'operations']
Result: Flat list

````
The map() function applies the transformation to each element and maintains the nested structure, producing a list of lists, while flatMap() applies the transformation and then flattens the result into a single simple list, removing one level of nesting.

````
[EXAMPLE 2] Word Count - Classic flatMap use case

 spark          : 3
 a              : 2
 analytics      : 1
 engine         : 1
 provides       : 1
 high-level     : 1
 in             : 1
 java,          : 1
 python         : 1
 r              : 1

````
 this is an example of classic flatMap() use case for word counting: it first breaks down each line into individual words using flatMap(), then transforms each word into a key-value pair using map(), aggregates the occurrences with reduceByKey(), and finally sorts the results by descending frequency.


````
[EXAMPLE 3] Generate multiple records per input

Using map() - nested:
[[0], [0, 1], [0, 1, 2], [0, 1, 2, 3]]

Using flatMap() - flat:
[0, 0, 1, 0, 1, 2, 0, 1, 2, 3]

````
numbers.map(lambda x: list(range(x))) generates for each number a list of elements, thus maintaining the nested structure with 4 distinct lists, while numbers.flatMap(lambda x: range(x)) generates the same elements but flattens them into a single linear sequence, removing the boundaries between the original lists.

````
[EXAMPLE 4] Explode orders with multiple products

Exploded order items:
 Order 1: CustomerA bought P001
 Order 1: CustomerA bought P002
 Order 1: CustomerA bought P003
 Order 2: CustomerB bought P001
 Order 2: CustomerB bought P004
 Order 3: CustomerC bought P002
 Order 3: CustomerC bought P005
 Order 3: CustomerC bought P006
 Order 3: CustomerC bought P007

 Total items across all orders: 9

````
This example demonstrates using flatMap() to "explode" structured data: each order line containing multiple products separated by semicolons is transformed into multiple individual records, creating a separate entry for each product purchased in each order.

````
[EXAMPLE 5] Extract all unique characters
Unique characters: HSWadeklopr

````

````
[EXAMPLE 6] Extract email domains
 - company.com
 - test.com
 - example.org

````

````
[EXAMPLE 7] When you NEED flatMap 

Original nested structure:
 data.count() = 3 # Only 3 elements (lists)

After flatMap:
 all_numbers.count() = 9 # 9 numbers

Numbers: [1, 2, 3, 4, 5, 6, 7, 8, 9]

````
This example illustrates a situation where flatMap() is essential: when data is structured in nested lists and we want to process them as individual elements. Without flatMap(), Spark only sees 3 lists, but with flatMap(), it can access the 9 individual numbers inside these lists.

# PART C: KEY-VALUE RDD OPERATIONS

```
======================================================================
KEY-VALUE RDD OPERATIONS
======================================================================

[OPERATION 1] Creating key-value pairs

Sample pairs (customer_id, amount):
 Customer 622: $7599.96
 Customer 558: $14473.56
 Customer 678: $12501.77
 Customer 948: $4849.80
 Customer 352: $14072.95

```
orders_rdd.map(lambda o: (o['customer_id'], o['amount'])) 
converts the RDD into key-value pairs by extracting the customer ID as key and order amount as value, creating a Pair RDD essential for subsequent aggregations


```
[OPERATION 2] Extract keys and values

Sample values (amounts):
 $7599.96
 $14473.56
 $12501.77
 $4849.80
 $14072.95

```


```
[OPERATION 3] mapValues() - Add 10% tax

Sample with 10% tax:

 Customer 622: $8359.96
 Customer 558: $15920.92
 Customer 678: $13751.95
 Customer 948: $5334.78
 Customer 352: $15480.25

```
The mapValues() function applies a transformation only to the values of the Pair RDD by adding 10% tax to each amount, while keeping the keys (customer_id) unchanged.


```
[OPERATION 4] reduceByKey() - Total per customer

Top 10 customers by total spending:

 Customer   38: $118,414.80
 Customer  112: $106,713.16
 Customer  647: $ 98,245.70
 Customer  212: $ 90,366.54
 Customer  695: $ 78,325.07
 Customer  970: $ 78,305.59
 Customer  352: $ 75,723.21
 Customer  329: $ 74,456.45
 Customer  772: $ 73,480.07
 Customer  993: $ 72,272.47

```
reduceByKey() function aggregates order amounts by customer by summing them, then sorts the results to identify the top 10 customers with the highest spending.


```
[OPERATION 5] groupByKey() - All orders per customer

Sample grouped data:

 Customer 622: 3 orders
 Amounts: ['$7599.96', '$3232.92', '$1711.36']
 Customer 558: 7 orders
 Amounts: ['$14473.56', '$6617.62', '$6125.88', '$6194.92', '$3825.18']
 Customer 678: 6 orders
 Amounts: ['$12501.77', '$2223.93', '$4852.20', '$7378.01', '$3324.96']

⚠️ WARNING: groupByKey() shuffles ALL data!
Better alternatives: reduceByKey(), aggregateByKey(), combineByKey()

```
The groupByKey() function groups all values associated with each key, enabling viewing of all orders per customer, but it's performance-intensive as it requires complete data transfer across the network.


```
[OPERATION 6] countByKey() - Orders per customer

Total unique customers: 995

Sample counts:
 Customer  622: 3 orders
 Customer  558: 7 orders
 Customer  678: 6 orders
 Customer  948: 6 orders
 Customer  352: 10 orders
 Customer  771: 8 orders
 Customer  524: 8 orders
 Customer  969: 12 orders
 Customer  597: 8 orders
 Customer  631: 6 orders

```
The countByKey() function counts the number of values associated with each key, quickly providing the number of orders per customer without performing expensive shuffle operations.

```
[OPERATION 7] sortByKey() - Sort by customer ID

 Customer    1: $9033.52
 Customer    1: $6198.64
 Customer    1: $4111.85
 Customer    1: $5795.14
 Customer    1: $13251.43
 Customer    1: $9824.47
 Customer    1: $11006.64
 Customer    2: $2584.48
 Customer    2: $1473.98
 Customer    2: $3107.71

```


```
[OPERATION 8] aggregateByKey() - Stats per customer

Customer statistics (top 5 by total spending):

 Customer 38:
 Orders: 15, Total: $118,414.80, Avg: $7894.32
 Min: $2987.78, Max: $14246.40

 Customer 112:
 Orders: 12, Total: $106,713.16, Avg: $8892.76
 Min: $1087.33, Max: $17347.53

 Customer 647:
 Orders: 11, Total: $98,245.70, Avg: $8931.43
 Min: $3278.62, Max: $14624.04

 Customer 212:
 Orders: 11, Total: $90,366.54, Avg: $8215.14
 Min: $1399.80, Max: $18237.77

 Customer 695:
 Orders: 10, Total: $78,325.07, Avg: $7832.51
 Min: $2555.67, Max: $13182.96

```


```
[OPERATION 9] combineByKey() - Calculate average

 Customer  258: $16,795.12 average
 Customer  397: $13,838.78 average
 Customer  996: $13,560.97 average
 Customer  610: $13,472.44 average
 Customer  707: $13,277.08 average
 Customer  200: $13,115.72 average
 Customer  106: $12,840.49 average
 Customer  234: $12,822.05 average
 Customer  832: $11,822.67 average
 Customer  331: $11,660.36 average

```


```
[OPERATION 10] Analysis by order status

Revenue by status:
                : $  303,768.74
 Cancelled      : $5,960,530.70
 Delivered      : $6,075,925.90
 Pending        : $6,361,514.20
 Processing     : $6,297,148.56
 Shipped        : $6,307,114.11

Detailed statistics by status:

               :   57 orders, $  303,768.74 total, $5,329.28 average
 Cancelled      :  912 orders, $5,960,530.70 total, $6,535.67 average
 Delivered      :  960 orders, $6,075,925.90 total, $6,329.09 average
 Pending        :  991 orders, $6,361,514.20 total, $6,419.29 average
 Processing     : 1015 orders, $6,297,148.56 total, $6,204.09 average
 Shipped        :  972 orders, $6,307,114.11 total, $6,488.80 average

```

## Questions to Answer:

```
1. Why is reduceByKey() faster than groupByKey()?
Answer: reduceByKey() combines data locally before shuffling, while groupByKey() shuffles all data.

2. When would you use aggregateByKey() instead of reduceByKey()?
Answer: When you need complex aggregations with different input/output types or multiple statistics.

3. What does mapValues() preserve that map() doesn't?
Answer: mapValues() preserves the keys and only transforms values, while map() can change both keys and values.

```