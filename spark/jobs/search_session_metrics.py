"""PySpark 집계: 검색 로그 → 세션 지표 + 일별 지표 + 인기 검색어.

입력은 착지(landing) 파일인 JSONL 이다. 운영에서는 오브젝트 스토리지(S3 등)에 쌓인 원본을
Spark 가 읽는 형태가 흔하고, 여기서는 그 자리를 로컬 파일이 대신한다 (docs/decisions/0003).

집계 기준은 **event_time**(사건이 일어난 시각)이다. 도착 시각으로 묶으면 늦게 온 이벤트가
엉뚱한 날짜에 섞여 어제 지표가 오늘 바뀐다.

실행:
    scripts/spark_submit.ps1 data/search_events.jsonl data/marts
"""

from __future__ import annotations

import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def read_events(spark: SparkSession, input_path: str):
    events = spark.read.json(input_path)
    return (
        events.withColumn("event_ts", F.to_timestamp("event_time"))
        .withColumn("event_date", F.to_date("event_ts"))
        .filter(F.col("event_ts").isNotNull() & F.col("session_id").isNotNull())
    )


def deduplicate(events):
    """같은 event_id 는 한 번만 센다. 재전송 중복이 지표를 부풀리지 않도록."""
    ordered = Window.partitionBy("event_id").orderBy(F.col("event_ts").asc())
    return (
        events.withColumn("_rank", F.row_number().over(ordered))
        .filter(F.col("_rank") == 1)
        .drop("_rank")
    )


def session_metrics(events):
    """세션 1개 = 1행. 8일차 어뷰징 탐지가 이 표를 입력으로 쓴다."""
    queries = events.filter(F.col("event_type") == "SearchQuery")

    previous = Window.partitionBy("session_id").orderBy(F.col("event_ts").asc())
    gaps = queries.withColumn(
        "gap_seconds",
        F.unix_timestamp("event_ts") - F.unix_timestamp(F.lag("event_ts").over(previous)),
    )

    per_session = gaps.groupBy("session_id").agg(
        F.min("event_date").alias("event_date"),
        F.count("*").alias("queries"),
        F.countDistinct("query").alias("distinct_queries"),
        F.min("event_ts").alias("first_query_at"),
        F.max("event_ts").alias("last_query_at"),
        F.avg("gap_seconds").alias("avg_gap_seconds"),
        F.min("gap_seconds").alias("min_gap_seconds"),
        # 간격의 표준편차가 0에 가까우면 사람이 아니라 기계가 두드리는 것이다
        F.stddev_pop("gap_seconds").alias("stddev_gap_seconds"),
        F.first("client").alias("client"),
    )

    clicks = (
        events.filter(F.col("event_type") == "SearchResultClick")
        .groupBy("session_id")
        .agg(
            F.count("*").alias("clicks"),
            F.avg("rank").alias("avg_click_rank"),
        )
    )

    return (
        per_session.join(clicks, on="session_id", how="left")
        .withColumn("clicks", F.coalesce("clicks", F.lit(0)))
        .withColumn("click_rate", F.col("clicks") / F.col("queries"))
        .withColumn("distinct_query_ratio", F.col("distinct_queries") / F.col("queries"))
        .withColumn(
            "span_seconds",
            F.unix_timestamp("last_query_at") - F.unix_timestamp("first_query_at"),
        )
        .withColumn(
            "queries_per_minute",
            F.when(
                F.col("span_seconds") > 0,
                F.col("queries") / (F.col("span_seconds") / 60.0),
            ).otherwise(F.lit(None)),
        )
    )


def daily_metrics(events):
    """일별 검색량·클릭수·CTR. event_time 기준이라 늦게 온 이벤트도 원래 날짜로 들어간다."""
    return (
        events.groupBy("event_date")
        .agg(
            F.countDistinct("session_id").alias("sessions"),
            F.sum(F.when(F.col("event_type") == "SearchQuery", 1).otherwise(0)).alias("queries"),
            F.sum(F.when(F.col("event_type") == "SearchResultClick", 1).otherwise(0)).alias(
                "clicks"
            ),
            F.countDistinct(F.when(F.col("event_type") == "SearchQuery", F.col("query"))).alias(
                "distinct_queries"
            ),
        )
        .withColumn(
            "ctr",
            F.when(F.col("queries") > 0, F.col("clicks") / F.col("queries")).otherwise(F.lit(None)),
        )
    )


def top_queries(events, top_n: int = 20):
    """날짜별 인기 검색어. 검색량이 같으면 검색어 이름 순으로 정렬해 순위를 고정한다."""
    per_query = (
        events.filter(F.col("event_type") == "SearchQuery")
        .groupBy("event_date", "query")
        .agg(
            F.count("*").alias("queries"),
            F.countDistinct("session_id").alias("sessions"),
        )
    )
    clicks = (
        events.filter(F.col("event_type") == "SearchResultClick")
        .groupBy("event_date", "query")
        .agg(F.count("*").alias("clicks"))
    )
    ranked = Window.partitionBy("event_date").orderBy(F.col("queries").desc(), F.col("query").asc())
    return (
        per_query.join(clicks, on=["event_date", "query"], how="left")
        .withColumn("clicks", F.coalesce("clicks", F.lit(0)))
        .withColumn("ctr", F.col("clicks") / F.col("queries"))
        .withColumn("rank", F.row_number().over(ranked))
        .filter(F.col("rank") <= top_n)
    )


def write_jsonl(df, path: str) -> None:
    """결과는 JSONL 로 쓴다. 적재기가 표준 라이브러리만으로 읽을 수 있게 하기 위해서다."""
    df.coalesce(1).write.mode("overwrite").json(path)


def main() -> None:
    if len(sys.argv) != 3:
        raise SystemExit("usage: search_session_metrics.py <input.jsonl> <output_dir>")
    input_path, output_dir = sys.argv[1], sys.argv[2]

    spark = SparkSession.builder.appName("search-session-metrics").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    events = deduplicate(read_events(spark, input_path)).cache()

    sessions = session_metrics(events)
    daily = daily_metrics(events)
    top = top_queries(events)

    write_jsonl(sessions, f"{output_dir}/mart_search_session")
    write_jsonl(daily, f"{output_dir}/mart_search_daily")
    write_jsonl(top, f"{output_dir}/mart_search_top_query")

    print(f"events(dedup) : {events.count()}")
    print(f"sessions      : {sessions.count()}")
    print(f"daily rows    : {daily.count()}")
    print(f"top query rows: {top.count()}")
    spark.stop()


if __name__ == "__main__":
    main()
