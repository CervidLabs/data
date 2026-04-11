import polars as pl
import time

data_path = "./data/yellow_tripdata_2019-03.csv"

def main():
    print("❄️ NYC YELLOW TAXI - POLARS AUDIT SYSTEM (FULL 5 Qs)")
    print("==================================================\n")

    start_load = time.time()
    
    # 1. CARGA DE DATOS
    df = pl.read_csv(data_path, try_parse_dates=True)
    
    # 2. LIMPIEZA RIGUROSA (Paridad con Octopus)
    df = df.drop_nulls(["tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_distance", "fare_amount", "payment_type", "passenger_count"])
    
    df = df.filter(
        (pl.col("trip_distance") > 0) & 
        (pl.col("fare_amount") > 0) &
        (pl.col("tpep_dropoff_datetime") > pl.col("tpep_pickup_datetime"))
    )

    # 3. FEATURE ENGINEERING
    df = df.with_columns([
        pl.col("tpep_pickup_datetime").dt.hour().alias("pickup_hour"),
        ((pl.col("tpep_dropoff_datetime") - pl.col("tpep_pickup_datetime")).dt.total_seconds() / 3600).alias("trip_duration_hr")
    ])

    df = df.with_columns([
        (pl.col("trip_distance") / pl.col("trip_duration_hr")).alias("avg_speed_mph"),
        ((pl.col("tip_amount") / pl.col("fare_amount")) * 100).alias("tip_percentage"),
        ((pl.col("fare_amount") + pl.col("tip_amount") + pl.col("tolls_amount").fill_null(0)) / pl.col("trip_distance")).alias("profit_per_mile")
    ])

    # 4. AUDITORÍA DE DATOS
    print("\n🔍 MUESTRA DE DATOS POLARS (Primeras 5 filas):")
    sample = df.head(5).to_dicts()
    for i, row in enumerate(sample):
        print(f"Fila {i}: Propina: {row['tip_percentage']:.2f}% | Rentabilidad: ${row['profit_per_mile']:.2f}/mi")

    # 5. QUERIES (Agregaciones)
    print("\n📊 RESULTADOS DE AGREGACIONES:")

    # Q1: Propinas por hora
    q1 = df.group_by("pickup_hour").agg(pl.col("tip_percentage").mean()).sort("tip_percentage", descending=True)
    print(f"⏱️ Q1 (Mejor hora propinas): {q1[0, 'pickup_hour']}h ({q1[0, 'tip_percentage']:.2f}%)")

    # Q2: Zonas Rentables
    q2 = df.group_by("PULocationID").agg(pl.col("profit_per_mile").mean()).sort("profit_per_mile", descending=True)
    print(f"⏱️ Q2 (Zona más rentable): ID {q2[0, 'PULocationID']} (${q2[0, 'profit_per_mile']:.2f}/mi)")

    # Q3: Distancia promedio por tipo de pago (1=Crédito)
    q3 = df.group_by("payment_type").agg(pl.col("trip_distance").mean()).filter(pl.col("payment_type") == 1)
    if not q3.is_empty():
        print(f"⏱️ Q3 (Distancia Prom. Crédito): {q3[0, 'trip_distance']:.2f} mi")

    # Q4: Tarifa promedio por cantidad de pasajeros (1 pasajero)
    q4 = df.group_by("passenger_count").agg(pl.col("fare_amount").mean()).filter(pl.col("passenger_count") == 1)
    if not q4.is_empty():
        print(f"⏱️ Q4 (Tarifa Prom. 1 pasajero): ${q4[0, 'fare_amount']:.2f}")

    # Q5: Tráfico (Hora más lenta)
    q5 = df.group_by("pickup_hour").agg(pl.col("avg_speed_mph").mean()).sort("avg_speed_mph")
    # Filtramos velocidades 0 para encontrar la lentitud real
    q5_filtered = q5.filter(pl.col("avg_speed_mph") > 0)
    print(f"⏱️ Q5 (Hora más lenta/tráfico): {q5_filtered[0, 'pickup_hour']}h ({q5_filtered[0, 'avg_speed_mph']:.2f} mph)")

    print(f"\n🏁 TIEMPO TOTAL POLARS: {time.time() - start_load:.2f}s")

if __name__ == "__main__":
    main()