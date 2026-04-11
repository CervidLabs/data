import pandas as pd
import time
import numpy as np

data_path = "./data/yellow_tripdata_2019-03.csv"

def main():
    print("🐼 NYC YELLOW TAXI - PANDAS ANALYTICS")
    print("=======================================\n")

    # 1. CARGA DE DATOS
    start_load = time.time()
    df = pd.read_csv(data_path, parse_dates=['tpep_pickup_datetime', 'tpep_dropoff_datetime'])
    load_time = time.time() - start_load
    print(f"✅ Cargado: {len(df):,} viajes en {load_time:.2f}s")

    # 2. LIMPIEZA
    df.dropna(subset=['tpep_pickup_datetime', 'tpep_dropoff_datetime', 'trip_distance', 'fare_amount'], inplace=True)
    
    # Filtrado
    df = df[(df['trip_distance'] > 0) & (df['fare_amount'] > 0)]

    # 3. FEATURE ENGINEERING
    df['pickup_hour'] = df['tpep_pickup_datetime'].dt.hour
    df['trip_duration_min'] = (df['tpep_dropoff_datetime'] - df['tpep_pickup_datetime']).dt.total_seconds() / 60
    df = df[df['trip_duration_min'] > 0] # Limpiar duraciones negativas
    
    df['avg_speed_mph'] = df['trip_distance'] / (df['trip_duration_min'] / 60)
    df['tip_percentage'] = (df['tip_amount'] / df['fare_amount']) * 100
    df['profit_per_mile'] = (df['fare_amount'] + df['tip_amount'] + df['tolls_amount']) / df['trip_distance']

    # 4. QUERY 1: PROPINAS POR HORA
    start1 = time.time()
    q1 = df.groupby('pickup_hour')['tip_percentage'].agg(['mean', 'count']).sort_values('mean', ascending=False)
    print(f"⏱️ Q1: {(time.time() - start1)*1000:.2f}ms")

    # 5. QUERY 2: ZONAS RENTABLES
    start2 = time.time()
    q2 = df.groupby('PULocationID')['profit_per_mile'].mean().sort_values(ascending=False).head(10)
    print(f"⏱️ Q2: {(time.time() - start2)*1000:.2f}ms")

    # 6. QUERY 3: OUTLIERS
    p99 = df['trip_duration_min'].quantile(0.99)
    outliers = df[df['trip_duration_min'] > p99].head(100)

    # 7. QUERY 4: PASAJEROS VS PROPINA
    start4 = time.time()
    q4 = df[df['passenger_count'].between(1, 6)].groupby('passenger_count')['tip_percentage'].mean()
    print(f"⏱️ Q4: {(time.time() - start4)*1000:.2f}ms")

    # 8. QUERY 5: TRÁFICO
    start5 = time.time()
    q5 = df.groupby('pickup_hour')['avg_speed_mph'].mean()
    print(f"⏱️ Q5: {(time.time() - start5)*1000:.2f}ms")

    print(f"\n⏱️ TIEMPO TOTAL: {time.time() - start_load:.2f}s")

if __name__ == "__main__":
    main()