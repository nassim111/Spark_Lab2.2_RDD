from pyspark import SparkContext, SparkConf

# Configuration Spark
conf = SparkConf().setAppName("Day2-Filter").setMaster("spark://spark-master:7077")
sc = SparkContext(conf=conf)

print("=" * 70)
print("FILTER OPERATIONS")
print("=" * 70)

# =====================================================
# CHARGEMENT DES DONNÉES AVEC GESTION D'ERREURS
# =====================================================
# Charge le fichier orders.csv dans un RDD
orders = sc.textFile("/opt/spark/data/ecommerce/orders.csv")
header = orders.first()  # Récupère l'en-tête
orders_data = orders.filter(lambda line: line != header)  # Filtre l'en-tête

def parse_order(line):
    """Convertit une ligne CSV en dictionnaire avec gestion d'erreurs"""
    try:
        fields = line.split(',')
        # Vérifier qu'il y a assez de champs et que amount n'est pas vide
        if len(fields) >= 7 and fields[5].strip():
            return {
                'order_id': int(fields[0]),
                'order_date': fields[1],
                'status': fields[3],
                'customer_id': int(fields[4]),
                'amount': float(fields[5]),
                'payment_method': fields[6]
            }
        else:
            return None  # Ligne invalide
    except (ValueError, IndexError) as e:
        # Gérer les erreurs de conversion ou d'index
        print(f"Error parsing line: {line} - Error: {e}")
        return None

# Parser les données et filtrer les lignes None
orders_rdd = orders_data.map(parse_order).filter(lambda x: x is not None)

print(f"Total valid orders: {orders_rdd.count()}")

# =====================================================
# FILTER 1: Condition Simple
# =====================================================
print("\n[FILTER 1] High-value orders (>$5000)\n")
high_value = orders_rdd.filter(lambda o: o['amount'] > 5000)
print(f"High-value orders: {high_value.count()}")
print("Sample high-value orders:")
for order in high_value.take(5):
    print(f" Order #{order['order_id']}: ${order['amount']:.2f}")

# =====================================================
# FILTER 2: Conditions Multiples (ET)
# =====================================================
print("\n[FILTER 2] Shipped orders over $2000\n")
shipped_high = orders_rdd.filter(
    lambda o: o['status'] == 'Shipped' and o['amount'] > 2000
)
print(f"Count: {shipped_high.count()}")
print("Sample:")
for order in shipped_high.take(3):
    print(f" Order #{order['order_id']}: {order['status']} - ${order['amount']:.2f}")

# =====================================================
# FILTER 3: Logique Complexe avec Fonction
# =====================================================
print("\n[FILTER 3] Problem orders (On Hold or Cancelled, >$1000)\n")
def is_problem_order(order):
    return (order['status'] in ['On Hold', 'Cancelled']) and \
           (order['amount'] > 1000)

problem_orders = orders_rdd.filter(is_problem_order)
print(f"Problem orders: {problem_orders.count()}")
print("Sample problem orders:")
for order in problem_orders.take(5):
    print(f" #{order['order_id']}: {order['status']} - ${order['amount']:.2f}")

# =====================================================
# FILTER 4: Filtrage par Date
# =====================================================
print("\n[FILTER 4] Orders from November 2024\n")
from datetime import datetime

def in_november_2024(order):
    try:
        date = datetime.strptime(order['order_date'], '%Y-%m-%d')
        return date.year == 2024 and date.month == 11
    except:
        return False

november_orders = orders_rdd.filter(in_november_2024)
print(f"November 2024 orders: {november_orders.count()}")

# =====================================================
# FILTER 5: Échantillonnage (Filtrage Aléatoire)
# =====================================================
print("\n[FILTER 5] Sample 10% of orders\n")
sample = orders_rdd.sample(withReplacement=False, fraction=0.1, seed=42)
print(f"Original: {orders_rdd.count()} orders")
print(f"Sample: {sample.count()} orders (~10%)")

# =====================================================
# FILTER 6: Chaînage de Filtres
# =====================================================
print("\n[FILTER 6] Chaining multiple filters\n")
# Le chaînage est optimisé par Spark
filtered = orders_rdd \
    .filter(lambda o: o['amount'] > 1000) \
    .filter(lambda o: o['status'] == 'Shipped') \
    .filter(lambda o: o['payment_method'] == 'Credit Card')

print(f"After all filters: {filtered.count()} orders")
print("Criteria: Amount > $1000, Status = Shipped, Payment = Credit Card")

# =====================================================
# FILTER 7: Négation (NOT)
# =====================================================
print("\n[FILTER 7] Exclude cancelled orders\n")
not_cancelled = orders_rdd.filter(lambda o: o['status'] != 'Cancelled')
print(f"Non-cancelled orders: {not_cancelled.count()}")

# =====================================================
# EXERCICES PRATIQUES
# =====================================================
print("\n" + "=" * 70)
print("PRACTICE EXERCISES")
print("=" * 70)
print("""
Complete these exercises:
1. Filter orders with amount between $1000 and $5000
2. Find all 'Processing' orders
3. Get orders from customers #1-100
4. Find orders with PayPal payment over $2000
5. Find orders from Q4 2024 (Oct, Nov, Dec)
""")

order_amount=orders_rdd.filter(lambda a:a['amount'] >1000 and a['amount']<5000 )
print(f"\n {order_amount.count()} orders between $1000-$5000")

processing_orders=orders_rdd.filter(lambda l:l['status']=='Processing')
print(f"Solution 2: {processing_orders.count()} processing orders")

#orders_1_100 = orders_rdd.filter(lambda o: o['customer_id'] in list(range(1, 101)))


sc.stop()