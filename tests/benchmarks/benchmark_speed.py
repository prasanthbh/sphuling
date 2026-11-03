"""High-performance speed and throughput benchmark for Sphuling."""

import time
from pysphuling.sql import SphulingSession, functions as F


def run_benchmark():
    print("=" * 60)
    print("  Sphuling (pysphuling) High-Performance Benchmark")
    print("=" * 60)

    # 1. Cold start time
    t0 = time.perf_counter()
    sphuling = (
        SphulingSession.builder
        .appName("BenchmarkApp")
        .master("local[*]")
        .config("sphuling.executor.cores", "8")
        .config("sphuling.executor.memory", "8g")
        .getOrCreate()
    )
    init_time = (time.perf_counter() - t0) * 1000
    print(f"[*] Engine startup latency: {init_time:.2f} ms (PySpark standard JVM is 5,000 - 15,000 ms)")

    # 2. Sequence generation: 1,000,000 rows
    N = 1_000_000
    t0 = time.perf_counter()
    df = sphuling.range(N)
    gen_time = (time.perf_counter() - t0) * 1000
    print(f"[*] Lazy 1,000,000 rows range created in: {gen_time:.2f} ms")

    # 3. Vectorized transformations & aggregations over 1,000,000 rows
    t0 = time.perf_counter()
    processed = (
        df.withColumn("mod_category", F.col("id") % 10)
        .withColumn("val", F.col("id") * 1.5)
        .filter(F.col("mod_category") > 2)
        .groupBy("mod_category")
        .agg(
            F.count("*").alias("row_count"),
            F.avg("val").alias("avg_val"),
            F.max("val").alias("max_val")
        )
        .orderBy(F.col("mod_category").asc())
    )
    plan_time = (time.perf_counter() - t0) * 1000
    print(f"[*] Lazy DAG constructed and optimized in: {plan_time:.2f} ms")

    # 4. Action execution (triggering multithreaded SIMD vectorized execution)
    t0 = time.perf_counter()
    results = processed.collect()
    exec_time = (time.perf_counter() - t0) * 1000
    print(f"[*] Vectorized execution across 1,000,000 rows completed in: {exec_time:.2f} ms")
    print("\nBenchmark Results Table:")
    processed.show()

    # 5. Zero-load schema introspection speed test
    import tempfile, os
    with tempfile.TemporaryDirectory() as tmpdir:
        pq_path = os.path.join(tmpdir, "bench.parquet")
        processed.write.parquet(pq_path)
        
        t0 = time.perf_counter()
        schema = sphuling.read.schema_of(pq_path)
        schema_time = (time.perf_counter() - t0) * 1000
        print(f"[*] Zero-Load Parquet Footer Schema extraction: {schema_time:.2f} ms (0 data blocks read)")

    print("=" * 60)
    print("  Benchmark Completed Successfully!")
    print("=" * 60)


if __name__ == "__main__":
    run_benchmark()

