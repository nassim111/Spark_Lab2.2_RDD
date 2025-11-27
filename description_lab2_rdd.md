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