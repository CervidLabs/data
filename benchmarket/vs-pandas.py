import pandas as pd
import time
import numpy as np

data_path = "./data/yellow_tripdata_2019-03.csv"

def main():
    print("🐼 NYC YELLOW TAXI - PANDAS AUDIT SYSTEM")
    print("========================================\n")

    # 1. CARGA DE DATOS
    start_load = time.time()
    # Cargamos solo las columnas necesarias para ser justos, o todas para comparar
    df = pd.read_csv(data_path, parse_dates=['tpep_pickup_datetime', 'tpep_dropoff_datetime'])
    load_time = time.time() - start_load
    print(f"✅ Cargado: {len(df):,} viajes en {load_time:.2f}s")

    # 2. LIMPIEZA (Para que los promedios no se alteren por basura)
    df = df[(df['trip_distance'] > 0) & (df['fare_amount'] > 0)].copy()

    # 3. FEATURE ENGINEERING
    start_fe = time.time()
    df['pickup_hour'] = df['tpep_pickup_datetime'].dt.hour
    df['trip_duration_min'] = (df['tpep_dropoff_datetime'] - df['tpep_pickup_datetime']).dt.total_seconds() / 60
    
    # Evitar divisiones por cero o duraciones negativas
    df = df[df['trip_duration_min'] > 0].copy()
    
    df['avg_speed_mph'] = df['trip_distance'] / (df['trip_duration_min'] / 60)
    df['tip_percentage'] = (df['tip_amount'] / df['fare_amount']) * 100
    df['profit_per_mile'] = (df['fare_amount'] + df['tip_amount'] + df['tolls_amount'].fillna(0)) / df['trip_distance']
    
    fe_time = (time.time() - start_fe) * 1000
    print(f"🔧 Feature Engineering: {fe_time:.2f}ms")

    # --- AUDITORÍA DE DATOS REALES ---
    print("\n🔍 MUESTRA DE DATOS PANDAS (Primeras 5 filas):")
    print("---------------------------------------------")
    sample = df.head(5)
    for i, row in sample.iterrows():
        print(f"Fila {i}:")
        print(f"  🕒 Hora: {int(row['pickup_hour'])}h")
        print(f"  🏎️  Velocidad: {row['avg_speed_mph']:.2f} mph")
        print(f"  💰 Propina: {row['tip_percentage']:.2f}%")
        print(f"  📈 Rentabilidad: ${row['profit_per_mile']:.2f}/mi")

    # --- QUERIES ---
    print("\n📊 RESULTADOS DE AGREGACIONES:")

    # Q1: Propinas por hora
    start1 = time.time()
    q1 = df.groupby('pickup_hour')['tip_percentage'].mean().sort_values(ascending=False)
    best_hour = q1.index[0]
    print(f"⏱️ Q1 (Propinas/Hora): {(time.time() - start1)*1000:.2f}ms -> Mejor hora: {best_hour}h ({q1.iloc[0]:.2f}%)")

    # Q2: Zonas Rentables
    start2 = time.time()
    q2 = df.groupby('PULocationID')['profit_per_mile'].mean().sort_values(ascending=False)
    print(f"⏱️ Q2 (Rentabilidad/Zona): {(time.time() - start2)*1000:.2f}ms -> Zona Top: ID {q2.index[0]}")

    # Q5: Tráfico (Velocidad/Hora)
    start5 = time.time()
    q5 = df.groupby('pickup_hour')['avg_speed_mph'].mean().sort_values()
    worst_hour = q5.index[0]
    print(f"⏱️ Q5 (Velocidad/Hora): {(time.time() - start5)*1000:.2f}ms -> Hora más lenta: {worst_hour}h ({q5.iloc[0]:.2f} mph)")

    print(f"\n🏁 TIEMPO TOTAL PANDAS: {time.time() - start_load:.2f}s")

if __name__ == "__main__":
    main()