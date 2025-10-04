import pandas as pd
from dagster import asset, Output, Definitions, AssetIn, AssetOut, multi_asset
from etl_pipeline.resources.minio_io_manager import MinIOIOManager
from etl_pipeline.resources.mysql_io_manager import MySQLIOManager
from etl_pipeline.resources.psql_io_manager import PostgreSQLIOManager
from datetime import datetime
from pyspark.sql import SparkSession
from dagster import asset, StaticPartitionsDefinition

weekly_partitions_def = StaticPartitionsDefinition(
    [str(i) for i in range(1,31)]
)


@asset(
    partitions_def=weekly_partitions_def,
    io_manager_key="minio_io_manager",
    ins={
        "silver_goals_assists": AssetIn(key_prefix=["silver", "football"]),
        "silver_player_info": AssetIn(key_prefix=["silver", "football"]),
    },
    compute_kind="PySpark",
    key_prefix=["gold", "football"],
    group_name="gold",
)
def gold_goals_assists(
    context, silver_goals_assists: pd.DataFrame, silver_player_info: pd.DataFrame
) -> Output[pd.DataFrame]:
    context.log.info("Started asset: gold_goals")

    spark = (
        SparkSession.builder
        .appName(
            f"gold-goals-assists-{datetime.today().strftime('%Y%m%d_%H%M%S')}"
        )
        .master("spark://spark-master:7077")
        .getOrCreate()
    )

    # Convert Pandas -> Spark
    dp = spark.createDataFrame(silver_player_info)
    context.log.info('Tạo spark data fram player_info thành công')
    
    fs = spark.createDataFrame(silver_goals_assists)
    context.log.info('Tạo spark data fram silver_goals thành công')
    
    dp.createOrReplaceTempView("silver_player_info")
    fs.createOrReplaceTempView("silver_goals_assists")

    # SQL query
    query = """
        SELECT 
            p.player_id      AS player_id,
            p.first_name     AS first_name,
            p.last_name      AS player_last_name,
            p.club           AS club,
            p.position       AS position,
            p.nation         AS nation,
            p.img_url        AS img_url,
            g.match_id       AS match_id,
            g.goal_minute    AS goal_minute,
            g.cate           AS category
            
        FROM silver_goals_assists g
        LEFT JOIN silver_player_info p ON p.last_name = g.last_name
    """
    df = spark.sql(query)

    result_df = df.toPandas()

    return Output(
        result_df,
        metadata={"row_count": len(result_df)},
    )


@asset(
    partitions_def=weekly_partitions_def,
    io_manager_key="minio_io_manager",
    ins={
        "silver_cards": AssetIn(key_prefix=["silver", "football"]),
        "silver_player_info": AssetIn(key_prefix=["silver", "football"]),
    },
    compute_kind="PySpark",
    key_prefix=["gold", "football"],
    group_name="gold",
)
def gold_cards(
    context, silver_cards: pd.DataFrame, silver_player_info: pd.DataFrame
) -> Output[pd.DataFrame]:
    context.log.info("Started asset: gold_cards")

    spark = (
        SparkSession.builder
        .appName(
            f"gold-cards-{datetime.today().strftime('%Y%m%d_%H%M%S')}"
        )
        .master("spark://spark-master:7077")
        .getOrCreate()
    )

    # Convert Pandas -> Spark
    dp = spark.createDataFrame(silver_player_info)
    context.log.info('Tạo spark data fram player_info thành công')
    
    fs = spark.createDataFrame(silver_cards)
    context.log.info('Tạo spark data fram silver_red_cards thành công')
    
    dp.createOrReplaceTempView("silver_player_info")
    fs.createOrReplaceTempView("silver_cards")

    # SQL query
    query = """
        SELECT 
            p.player_id      AS player_id,
            p.first_name     AS first_name,
            p.last_name      AS player_last_name,
            p.club           AS club,
            p.position       AS position,
            p.nation         AS nation,
            p.img_url        AS img_url,
            g.match_id       AS match_id,
            g.card_minute    AS minute,
            g.cate           AS category
            
        FROM silver_cards g
        LEFT JOIN silver_player_info p ON p.last_name = g.last_name
    """
    df = spark.sql(query)

    result_df = df.toPandas()

    return Output(
        result_df,
        metadata={"records": len(result_df)},
    )

@asset(
    partitions_def=weekly_partitions_def,
    io_manager_key='minio_io_manager',
    ins={
        'silver_appearance': AssetIn(key_prefix=['silver','football'])
    },
    compute_kind = 'pandas',
    key_prefix=["gold", "football"],
    group_name="gold",
)
def gold_appearance(silver_appearance: pd.DataFrame):
    return Output(
        silver_appearance,
        metadata={
            'row_count': len(silver_appearance)
        }
    )

@asset(
    io_manager_key='minio_io_manager',
    ins={
        'silver_player_info': AssetIn(key_prefix=['silver','football'])
    },
    compute_kind = 'pandas',
    key_prefix=["gold", "football"],
    group_name="gold",
)
def gold_player_info(silver_player_info: pd.DataFrame):
    return Output(
        silver_player_info,
        metadata={
            'row_count': silver_player_info.shape[0]
        }
    )

@asset(
    partitions_def=weekly_partitions_def,
    io_manager_key='minio_io_manager',
    ins={
        'silver_match_score': AssetIn(key_prefix=['silver','football'])
    },
    compute_kind = 'pandas',
    key_prefix=["gold", "football"],
    group_name="gold",
)
def gold_match_score(silver_match_score: pd.DataFrame):
    return Output(
        silver_match_score,
        metadata={
            'row_count': silver_match_score.shape[0]
        }
    )

@asset(
    io_manager_key='minio_io_manager',
    ins={
        'silver_team_info': AssetIn(key_prefix=['silver','football'])
    },
    compute_kind = 'pandas',
    key_prefix=["gold", "football"],
    group_name="gold",
)
def gold_team_info(silver_team_info: pd.DataFrame):
    return Output(
        silver_team_info,
        metadata={
            'row_count': silver_team_info.shape[0],
            'columns': silver_team_info.columns.to_list()
        }
    )
