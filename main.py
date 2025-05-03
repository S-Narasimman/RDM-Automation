# Save this as main.py in a GCS bucket (gs://your-bucket/scripts/main.py)

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Retail Metrics Pipeline") \
    .getOrCreate()

sales = spark.read.option("header", "true").csv("gs://your-bucket/data/sales.csv", inferSchema=True)
inventory = spark.read.option("header", "true").csv("gs://your-bucket/data/inventory.csv", inferSchema=True)
promotions = spark.read.option("header", "true").csv("gs://your-bucket/data/promotions.csv", inferSchema=True)
rewards = spark.read.option("header", "true").csv("gs://your-bucket/data/rewards.csv", inferSchema=True)

sales.createOrReplaceTempView("sales")
inventory.createOrReplaceTempView("inventory")
promotions.createOrReplaceTempView("promotions")
rewards.createOrReplaceTempView("rewards")

profit_df = spark.sql("""
SELECT
    s.product_id,
    s.date,
    (s.selling_price - s.cost_price) * s.quantity_sold AS profit,
    CASE WHEN p.is_promoted = 1 THEN 'PROMO' ELSE 'NO_PROMO' END AS promo_indicator
FROM sales s
LEFT JOIN promotions p
ON s.product_id = p.product_id AND s.date = p.date
""")
profit_df.createOrReplaceTempView("profit_metrics")

profit_agg = spark.sql("""
SELECT
    promo_indicator,
    ROUND(SUM(profit), 2) AS total_profit,
    ROUND(AVG(profit), 2) AS avg_profit
FROM profit_metrics
GROUP BY promo_indicator
""")

demand_supply_df = spark.sql("""
SELECT
    s.product_id,
    s.date,
    SUM(s.quantity_sold) AS total_demand,
    SUM(i.quantity_available) AS total_supply,
    ROUND(SUM(s.quantity_sold) / NULLIF(SUM(i.quantity_available), 0), 2) AS demand_supply_ratio
FROM sales s
JOIN inventory i
ON s.product_id = i.product_id AND s.date = i.date
GROUP BY s.product_id, s.date
""")

rewards_metrics_df = spark.sql("""
SELECT
    customer_id,
    SUM(points_earned) AS total_points_earned,
    SUM(points_redeemed) AS total_points_redeemed,
    ROUND(SUM(points_redeemed) / NULLIF(SUM(points_earned), 0), 2) AS redemption_ratio
FROM rewards
GROUP BY customer_id
""")

output_path = "gs://your-bucket/output"
profit_agg.write.mode("overwrite").option("header", "true").csv(f"{output_path}/profit_metrics")
demand_supply_df.write.mode("overwrite").option("header", "true").csv(f"{output_path}/demand_supply_metrics")
rewards_metrics_df.write.mode("overwrite").option("header", "true").csv(f"{output_path}/rewards_metrics")

print("✅ Metrics successfully written to GCS.")