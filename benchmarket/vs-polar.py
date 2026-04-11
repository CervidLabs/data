import polars as pl
import time
from datetime import datetime

# Configuración de ruta
data_path = "./data/yellow_tripdata_2019-03.csv"

def main():
    print("🚕 NYC YELLOW TAXI - POLARS ANALYTICS")
    print("=======================================\n")

    # 1. CARGA DE DATOS
    start_load = time.time()
    # Usamos scan_csv para evaluación perezosa (lazy) si fuera necesario, 
    # pero para comparar con Octopus usaremos read_csv (memoria)
    df = pl.read_csv(data_path, try_parse_dates=True)
    load_time = time.time() - start_load
    print(f"✅ Cargado: {len(df):,} viajes en {load_time:.2f}s")

    # 2. LIMPIEZA
    df = df.drop_nulls(["tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_distance", "fare_amount", "tip_amount"])
    
    # Filtrado de valores inválidos
    df = df.filter(
        (pl.col("trip_distance") > 0) & 
        (pl.col("fare_amount") > 0) &
        (pl.col("tpep_dropoff_datetime") > pl.col("tpep_pickup_datetime"))
    )

    # 3. FEATURE ENGINEERING
    df = df.with_columns([
        pl.col("tpep_pickup_datetime").dt.hour().alias("pickup_hour"),
        ((pl.col("tpep_dropoff_datetime") - pl.col("tpep_pickup_datetime")).dt.total_minutes()).alias("trip_duration_min")
    ])

    df = df.with_columns([
        (pl.col("trip_distance") / (pl.col("trip_duration_min") / 60)).alias("avg_speed_mph"),
        ((pl.col("tip_amount") / pl.col("fare_amount")) * 100).alias("tip_percentage"),
        ((pl.col("fare_amount") + pl.col("tip_amount") + pl.col("tolls_amount")) / pl.col("trip_distance")).alias("profit_per_mile")
    ])

    # 4. QUERY 1: PROPINAS POR HORA
    start1 = time.time()
    q1 = df.group_by("pickup_hour").agg(
        pl.col("tip_percentage").mean().alias("avg_tip_pct"),
        pl.len().alias("trips")
    ).sort("avg_tip_pct", descending=True)
    print(f"⏱️ Q1: {(time.time() - start1)*1000:.2f}ms")

    # 5. QUERY 2: ZONAS RENTABLES
    start2 = time.time()
    q2 = df.group_by("PULocationID").agg(
        pl.col("profit_per_mile").mean().alias("avg_profit")
    ).sort("avg_profit", descending=True).head(10)
    print(f"⏱️ Q2: {(time.time() - start2)*1000:.2f}ms")

    # 6. QUERY 3: OUTLIERS (Basado en percentiles)
    p99 = df["trip_duration_min"].quantile(0.99)
    outliers = df.filter(pl.col("trip_duration_min") > p99).head(100)

    # 7. QUERY 4: PASAJEROS VS PROPINA
    start4 = time.time()
    q4 = df.filter(pl.col("passenger_count").is_between(1, 6)).group_by("passenger_count").agg(
        pl.col("tip_percentage").mean()
    ).sort("passenger_count")
    print(f"⏱️ Q4: {(time.time() - start4)*1000:.2f}ms")

    # 8. QUERY 5: TRÁFICO
    start5 = time.time()
    q5 = df.group_by("pickup_hour").agg(
        pl.col("avg_speed_mph").mean()
    ).sort("pickup_hour")
    print(f"⏱️ Q5: {(time.time() - start5)*1000:.2f}ms")

    print(f"\n⏱️ TIEMPO TOTAL: {time.time() - start_load:.2f}s")

if __name__ == "__main__":
    main()