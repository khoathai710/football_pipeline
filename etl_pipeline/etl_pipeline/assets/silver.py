import pandas as pd
from dagster import (
    asset, 
    Output, 
    AssetIn,
    DailyPartitionsDefinition
)
from datetime import datetime
from pyspark.sql import SparkSession
from dagster import asset, StaticPartitionsDefinition
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType
from pyspark.sql.functions import split, col, array_join, slice

from pyspark.sql.functions import col, explode, regexp_extract,lit, col
from pyspark.sql.functions import split, col, array_join, slice, size, when

# from etl_pipeline.resources.spark_io_manager import create_session
from pyspark.sql.functions import col, concat

weekly_partitions_def = StaticPartitionsDefinition(
    [str(i) for i in range(1,31)]
)


@asset(
    partitions_def=weekly_partitions_def,
    io_manager_key="minio_io_manager",
    ins={
        "bronze_matches_dataset": AssetIn(key_prefix=["bronze", "football"]),
    },
    compute_kind="PySpark",
    key_prefix=["silver", "football"],
    group_name="silver",
)
def silver_goals_assists(context, bronze_matches_dataset: pd.DataFrame) -> Output[pd.DataFrame]:
    
    context.log.info("Started asset: bronze_matches_dataset")
    
    spark = (
        SparkSession.builder
        .appName(f"silver-yellow-card-cleaned-{datetime.today().strftime('%Y%m%d_%H%M%S')}")
        .master("spark://spark-master:7077")
        .getOrCreate()
    )
    context.log.info(f'Tạo session cho spark thành công: {spark.version}')
    
    ## GOALS
    # Chọn cột cần thiết cho
    cols = ['goals','id']
    bronze_matches_dataset_1 = bronze_matches_dataset[cols]
    #player_info = spark.createDataFrame(bronze_player_info)
    goal_schema = ArrayType(
        StructType([
            StructField("minute", StringType(), True),
            StructField("name", StringType(), True),
        ])
    )
    schema = StructType([
        StructField("goals", goal_schema, True),
        #StructField("away.goals", goal_schema, True),
        StructField("id", StringType(), True)
    ])

    try:
        match_info = spark.createDataFrame(bronze_matches_dataset_1, schema=schema)
        context.log.info('Tạo DataFrame thành công')
    except Exception as e:
        context.log.error(f"Lỗi khi tạo Spark DataFrame: {str(e)}")
        raise
    
    match_info = match_info.select(
        col('id').alias('match_id'),
        explode('`goals`').alias('goals')
    )
    df_clean_1 = (
        match_info
        .withColumn("goal_minute", F.col("goals.minute"))
        .withColumn("last_name", F.col("goals.name"))
        .drop("goals")
        .withColumn("cate", F.lit(1))
    )

    ## ASSISTS
    cols = ['assists','id']
    bronze_matches_dataset = bronze_matches_dataset[cols]
    #player_info = spark.createDataFrame(bronze_player_info)
    goal_schema = ArrayType(
        StructType([
            StructField("minute", StringType(), True),
            StructField("name", StringType(), True),
        ])
    )
    schema = StructType([
        StructField("assists", goal_schema, True),
        #StructField("away.assists", goal_schema, True),
        StructField("id", StringType(), True)
    ])

    try:
        match_info = spark.createDataFrame(bronze_matches_dataset, schema=schema)
        context.log.info('Tạo DataFrame thành công')
    except Exception as e:
        context.log.error(f"Lỗi khi tạo Spark DataFrame: {str(e)}")
        raise
    
    match_info = match_info.select(
        col('id').alias('match_id'),
        explode('`assists`').alias('assists')
    )
    df_clean = (
        match_info
        .withColumn("goal_minute", F.col("assists.minute"))
        .withColumn("last_name", F.col("assists.name"))
        .drop("assists")  
        .withColumn('cate',F.lit(0))
    )
    
    df_clean = df_clean_1.union(df_clean)
    return Output(
        df_clean.toPandas(),
        metadata={"row_count": df_clean.count()}
    )

@asset(
    partitions_def=weekly_partitions_def,
    io_manager_key="minio_io_manager",
    ins={
        "bronze_matches_dataset": AssetIn(key_prefix=["bronze", "football"]),
    },
    compute_kind="PySpark",
    key_prefix=["silver", "football"],
    group_name="silver",
)
def silver_cards(context, bronze_matches_dataset: pd.DataFrame) -> Output[pd.DataFrame]:
    
    context.log.info("Started asset: bronze_matches_dataset")
    
    spark = (
        SparkSession.builder
        .appName(f"silver-yellow-card-cleaned-{datetime.today().strftime('%Y%m%d_%H%M%S')}")
        .master("spark://spark-master:7077")
        .getOrCreate()
    )
    context.log.info(f'Tạo session cho spark thành công: {spark.version}')

    # Chọn cột cần thiết
    cols = ['yellow_cards','id']
    bronze_matches_dataset_1 = bronze_matches_dataset[cols]
    #player_info = spark.createDataFrame(bronze_player_info)
    goal_schema = ArrayType(
        StructType([
            StructField("minute", StringType(), True),
            StructField("name", StringType(), True),
        ])
    )
    schema = StructType([
        StructField("yellow_cards", goal_schema, True),
        #StructField("away.yellow_cards", goal_schema, True),
        StructField("id", StringType(), True)
    ])

    try:
        match_info = spark.createDataFrame(bronze_matches_dataset_1, schema=schema)
        context.log.info('Tạo DataFrame thành công')
    except Exception as e:
        context.log.error(f"Lỗi khi tạo Spark DataFrame: {str(e)}")
        raise
    
    match_info = match_info.select(
        col('id').alias('match_id'),
        explode('`yellow_cards`').alias('yellow_cards')
    )
    df_clean = (
        match_info
        .withColumn("card_minute", F.col("yellow_cards.minute"))
        .withColumn("last_name", F.col("yellow_cards.name"))
        .drop("yellow_cards") 
    )
    df_clean = df_clean.withColumn('cate',F.lit(1))
     
    ## RED CARDS
    cols = ['red_cards','id']
    bronze_matches_dataset = bronze_matches_dataset[cols]
    #player_info = spark.createDataFrame(bronze_player_info)
    goal_schema = ArrayType(
        StructType([
            StructField("minute", StringType(), True),
            StructField("name", StringType(), True),
        ])
    )
    schema = StructType([
        StructField("red_cards", goal_schema, True),
        #StructField("away.red_cards", goal_schema, True),
        StructField("id", StringType(), True)
    ])

    try:
        match_info = spark.createDataFrame(bronze_matches_dataset, schema=schema)
        context.log.info('Tạo DataFrame thành công')
    except Exception as e:
        context.log.error(f"Lỗi khi tạo Spark DataFrame: {str(e)}")
        raise
    
    match_info = match_info.select(
        col('id').alias('match_id'),
        explode('`red_cards`').alias('red_cards')
    )
    df_clean_1 = (
        match_info
        .withColumn("goal_minute", F.col("red_cards.minute"))
        .withColumn("last_name", F.col("red_cards.name"))
        .drop("red_cards")  
    )
    df_clean_1 = df_clean_1.withColumn('cate',F.lit(0)) 
    df_clean = df_clean.union(df_clean_1)
    return Output(
        df_clean.toPandas(),
        metadata={"row_count": df_clean.count()}
    )

@asset(
    partitions_def= weekly_partitions_def,
    ins={
        "bronze_matches_dataset": AssetIn(key_prefix=["bronze", "football"]),
        "bronze_player_info": AssetIn(key_prefix=['bronze','football'])
    },
    io_manager_key="minio_io_manager",
    group_name='silver',
    compute_kind="PySpark",
    key_prefix=["silver", "football"],
)
def silver_appearance(context,bronze_matches_dataset: pd.DataFrame,bronze_player_info: pd.DataFrame) ->Output[pd.DataFrame]:
    context.log.info('Bắt đầu thực hiện cho silver_appearance_player')
    spark = SparkSession.builder\
        .appName(f"silver-appearance-{datetime.today().strftime('%Y%m%d_%H%M%S')}")\
        .master("spark://spark-master:7077")\
        .getOrCreate()
        
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("startings", ArrayType(StringType()), True),
        StructField("subs", ArrayType(StringType()), True),
    ])
    columns = ['id','startings','subs']
    bronze_matches_dataset = bronze_matches_dataset[columns] 
    context.log.info(f'Số cột của data: {bronze_matches_dataset.shape}')
    staring_sub_info = spark.createDataFrame(bronze_matches_dataset,schema=schema)
    player_info = spark.createDataFrame(bronze_player_info)
    context.log.info(f'Số cột của data: {player_info.count()}')
    
    startings = staring_sub_info.select(
        col('id').alias('match_id'),
        explode('startings').alias("player_id")
    ).withColumn("flag", lit(1))
    
    # Subs 
    subs = staring_sub_info.select(
        col('id').alias('match_id'),
        explode('subs').alias("player_id")
    ).withColumn("flag", lit(0))
    
    total_player = subs.union(startings)
    # total_player = total_player.withColumn("player_id", col("player_id").cast("string"))
    # player_info  = player_info.withColumn("player_id", col("player_id").cast("string"))

    merged_df = total_player.join(player_info, on="player_id", how="left")

    return Output(
        merged_df.toPandas(),
        metadata={
            'row': merged_df.count()
        }
        
    )
    
@asset(
    ins={
        "bronze_player_info": AssetIn(key_prefix=['bronze','football'])
    },
    io_manager_key="minio_io_manager",
    group_name='silver',
    compute_kind="PySpark",
    key_prefix=["silver", "football"],
)
def silver_player_info(context, bronze_player_info: pd.DataFrame):
    spark = SparkSession.builder \
        .appName(f"silver-player-{datetime.today().strftime('%Y%m%d_%H%M%S')}") \
        .master("spark://spark-master:7077") \
        .getOrCreate()

    player_info = spark.createDataFrame(bronze_player_info)

    player_info = (
        player_info
        .withColumn(
            "first_name",
            when(size(split(col("name"), " ")) > 1, split(col("name"), " ").getItem(0))
            .otherwise("")
        )
        .withColumn(
            "last_name",
            when(
                size(split(col("name"), " ")) == 1, 
                split(col("name"), " ").getItem(0)           # chỉ 1 chữ → gán luôn
            ).when(
                size(split(col("name"), " ")) == 2, 
                split(col("name"), " ").getItem(1)           # 2 chữ → lấy chữ cuối
            ).otherwise(
                array_join(
                    slice(split(col("name"), " "), -2, 2),   # >=3 chữ → ghép 2 chữ cuối
                    " "
                )
            )
        )
    )

    return Output(
        player_info.toPandas(),
        metadata={"len": player_info.count()}
    )

@asset(
    partitions_def=weekly_partitions_def,
    ins={
        "bronze_teams_dataset": AssetIn(key_prefix=['bronze','football']),
        "bronze_matches_dataset": AssetIn(key_prefix=['bronze','football']),
        "bronze_player_info": AssetIn(key_prefix=['bronze','football']),
        
        
    },
    io_manager_key="minio_io_manager",
    group_name='silver',
    compute_kind="PySpark",
    key_prefix=["silver", "football"],
)
def silver_match_score(context, bronze_teams_dataset: pd.DataFrame,bronze_matches_dataset: pd.DataFrame,bronze_player_info: pd.DataFrame):
    spark = SparkSession.builder \
        .appName(f"silver-player-{datetime.today().strftime('%Y%m%d_%H%M%S')}") \
        .master("spark://spark-master:7077") \
        .getOrCreate()
    cols = ['id','home','away','score','motm']
    bronze_matches_dataset = bronze_matches_dataset[cols]
    
    teams = spark.createDataFrame(bronze_teams_dataset)
    matches = spark.createDataFrame(bronze_matches_dataset)
    matches = matches.withColumn(
        "home_score",
        split(col("score"), " - ").getItem(0).cast("int")
    ).withColumn(
        "away_score",
        split(col("score"), " - ").getItem(1).cast("int")
    )
    
    player = spark.createDataFrame(bronze_player_info)
    
    player.createTempView('player_info')
    teams.createTempView('teams')
    matches.createTempView('matches')
    
    sql_tm = """
        SELECT 
            m.id AS match_id,
            t.name AS home_name,
            t1.name AS away_name,
            m.home_score AS home_score,
            m.away_score AS away_score,
            p.name AS name,
            p.club AS club,
            p.nation AS nation,
            p.position AS position
        FROM matches m
        LEFT JOIN teams t ON t.team_id = m.home
        LEFT JOIN teams t1 ON t1.team_id = m.away
        LEFT JOIN player_info p ON p.player_id = m.motm
    """
    df = spark.sql(sql_tm)

    return Output(
        df.toPandas(),
        metadata={"len": df.count()}
    )

@asset(
    ins={
        "bronze_football_stadiums_dataset": AssetIn(key_prefix=['bronze','football']),
        "bronze_teams_dataset": AssetIn(key_prefix=['bronze','football'])
    },
    io_manager_key="minio_io_manager",
    group_name='silver',
    compute_kind="PySpark",
    key_prefix=["silver", "football"],
)
def silver_team_info(bronze_football_stadiums_dataset: pd.DataFrame,
                     bronze_teams_dataset: pd.DataFrame) -> Output[pd.DataFrame]:
    # Inner join 2 DataFrame theo team_id
    spark = SparkSession.builder \
        .appName(f"silver-player-{datetime.today().strftime('%Y%m%d_%H%M%S')}") \
        .master("spark://spark-master:7077") \
        .getOrCreate()
    bronze_football_stadiums_dataset = spark.createDataFrame(bronze_football_stadiums_dataset)
    bronze_teams_dataset = spark.createDataFrame(bronze_teams_dataset)
    
    df = bronze_football_stadiums_dataset.join(
        bronze_teams_dataset,
        on="team_id",
        how="inner"
    )
    return Output(
        df.toPandas(),
        metadata={
            'records': df.count()
        }
    )
