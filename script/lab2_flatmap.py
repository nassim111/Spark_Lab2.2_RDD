from pyspark import SparkContext, SparkConf
import logging

conf = SparkConf().setAppName("Day2-FlatMap").setMaster("spark://spark-master:7077")
sc = SparkContext(conf=conf)

print("=" * 70)
print("FLATMAP vs MAP")
print("=" * 70)

# =====================================================
# EXAMPLE 1: The Fundamental Difference
# =====================================================
print("\n[EXAMPLE 1] Understanding the difference\n")
sentences = sc.parallelize([
    "Hello World",
    "Spark is awesome",
    "RDD operations"
])

# Using map - returns list of lists (nested structure)
words_map = sentences.map(lambda s: s.split(" "))
print("Using map():")
print(words_map.collect())
print("Result: Nested lists")
# [['Hello', 'World'], ['Spark', 'is', 'awesome'], ['RDD', 'operations']]

# Using flatMap - flattens to single list
words_flatmap = sentences.flatMap(lambda s: s.split(" "))
print("\nUsing flatMap():")
print(words_flatmap.collect())
print("Result: Flat list")
# ['Hello', 'World', 'Spark', 'is', 'awesome', 'RDD', 'operations']

# =====================================================
# EXAMPLE 2: Word Count (Classic Use Case)
# =====================================================
print("\n[EXAMPLE 2] Word Count - Classic flatMap use case\n")
# Create sample text
text_data = [
    "Apache Spark is a unified analytics engine",
    "Spark provides high-level APIs in Java, Scala, Python and R",
    "Spark also supports a rich set of higher-level tools"
]
text_rdd = sc.parallelize(text_data)

# Word count pipeline
word_count = text_rdd \
    .flatMap(lambda line: line.split(" ")) \
    .map(lambda word: word.lower()) \
    .map(lambda word: (word, 1)) \
    .reduceByKey(lambda a, b: a + b) \
    .sortBy(lambda x: x[1], ascending=False)

print("Top 10 most common words:")
for word, count in word_count.take(10):
    print(f" {word:15s}: {count}")

# =====================================================
# EXAMPLE 3: Generating Multiple Records
# =====================================================
print("\n[EXAMPLE 3] Generate multiple records per input\n")
numbers = sc.parallelize([1, 2, 3, 4])

# map returns nested lists
nested = numbers.map(lambda x: list(range(x)))
print("Using map() - nested:")
print(nested.collect())
# [[0], [0, 1], [0, 1, 2], [0, 1, 2, 3]]

# flatMap flattens
flat = numbers.flatMap(lambda x: range(x))
print("\nUsing flatMap() - flat:")
print(flat.collect())
# [0, 0, 1, 0, 1, 2, 0, 1, 2, 3]

# =====================================================
# EXAMPLE 4: Exploding CSV with Multiple Items
# =====================================================
print("\n[EXAMPLE 4] Explode orders with multiple products\n")
# Simulate orders with multiple products
order_data = sc.parallelize([
    "1,CustomerA,P001;P002;P003",
    "2,CustomerB,P001;P004",
    "3,CustomerC,P002;P005;P006;P007"
])

def explode_products(line):
    """Explode order line to multiple product records"""
    fields = line.split(',')
    order_id = fields[0]
    customer = fields[1]
    products = fields[2].split(';')
    # Generate one record per product
    for product in products:
        yield (order_id, customer, product)

exploded = order_data.flatMap(explode_products)
print("Exploded order items:")
for item in exploded.collect():
    print(f" Order {item[0]}: {item[1]} bought {item[2]}")

# Count total items
print(f"\nTotal items across all orders: {exploded.count()}")

# =====================================================
# EXAMPLE 5: Text Analysis - Extract Characters
# =====================================================
print("\n[EXAMPLE 5] Extract all unique characters\n")
words = sc.parallelize(["Hello", "World", "Spark"])

# Get all characters (flattened)
chars = words.flatMap(lambda word: list(word))
unique_chars = chars.distinct().sortBy(lambda x: x)
print("Unique characters:", ''.join(unique_chars.collect()))

# =====================================================
# EXAMPLE 6: Email Domain Extraction
# =====================================================
print("\n[EXAMPLE 6] Extract email domains\n")
import re

emails = sc.parallelize([
    "Contact us at: support@company.com, sales@company.com",
    "Email: info@example.org",
    "Reach john@test.com or jane@test.com"
])

def extract_domains(text):
    """Extract all email domains from text"""
    email_pattern = r'[\w\.-]+@([\w\.-]+)'
    matches = re.findall(email_pattern, text)
    return matches

domains = emails.flatMap(extract_domains).distinct()
print("Email domains found:")
for domain in domains.collect():
    print(f" - {domain}")

# =====================================================
# EXAMPLE 7: When map() Would Fail
# =====================================================
print("\n[EXAMPLE 7] When you NEED flatMap\n")
data = sc.parallelize([
    [1, 2, 3],
    [4, 5],
    [6, 7, 8, 9]
])

# Try to count all numbers
print("Original nested structure:")
print(f" data.count() = {data.count()} # Only 3 elements (lists)")

# Use flatMap to flatten
all_numbers = data.flatMap(lambda x: x)
print("\nAfter flatMap:")
print(f" all_numbers.count() = {all_numbers.count()} # 9 numbers")
print(f" Numbers: {all_numbers.collect()}")

# =====================================================
# PRACTICE EXERCISES
# =====================================================
print("\n" + "=" * 70)
print("PRACTICE EXERCISES")
print("=" * 70)
print("""
Complete these exercises:
1. Load customers.csv and extract all words from customer names
2. Count how many customers have each word in their name
3. Create an RDD that generates numbers 0 to N-1 for each N in [3, 5, 2, 4]
4. Split customer addresses by spaces and count word frequencies
5. Extract all unique digits from all phone numbers
""")
logger = sc._jvm.org.apache.log4j
logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

# Ou désactiver tous les logs Python
logging.getLogger("py4j").setLevel(logging.ERROR)

sc.stop()