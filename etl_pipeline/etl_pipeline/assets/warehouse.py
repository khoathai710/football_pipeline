import pandas as pd
from dagster import asset, Output, multi_asset, AssetIn, AssetOut, StaticPartitionsDefinition

weekly_partitions_def = StaticPartitionsDefinition([str(i) for i in range(1, 31)])

@multi_asset(
    ins={"goals": AssetIn(key=["gold", "football", "gold_goals_assists"])},
    outs={
        "goals_assists": AssetOut(
            io_manager_key="psql_io_manager",
            key_prefix=["warehouse", "public"],
            metadata={
                "columns": ['player_id', 'first_name', 'player_last_name', 'club', 'position',
                            'nation', 'img_url', 'match_id', 'minute', 'category'],
            },
        )
    },
    compute_kind="PostgreSQL",
    partitions_def=weekly_partitions_def,
    group_name="gold",
)
def goals_assists(context, goals) -> Output[pd.DataFrame]:
    return Output(goals, metadata={"schema": "public", "records counts": len(goals)})


@multi_asset(
    ins={"gold_cards": AssetIn(key=["gold", "football", "gold_cards"])},
    outs={
        "cards": AssetOut(
            io_manager_key="psql_io_manager",
            key_prefix=["warehouse", "public"],
            metadata={
                "columns": ['player_id', 'first_name', 'player_last_name', 'club', 'position',
       'nation', 'img_url', 'match_id', 'minute', 'category'],
            },
            
        )
    },
    compute_kind="PostgreSQL",
    partitions_def=weekly_partitions_def,
    group_name="gold",
)
def cards(context, gold_cards) -> Output[pd.DataFrame]:
    return Output(gold_cards, metadata={"schema": "public", "records counts": len(gold_cards)})


@multi_asset(
    ins={"appearance": AssetIn(key=["gold", "football", "gold_appearance"])},
    outs={
        "appearance": AssetOut(
            io_manager_key="psql_io_manager",
            key_prefix=["warehouse", "public"],
            metadata={
                "columns": ["player_id", "match_id", "flag", "team_id", "name", "club", "position", "nation", "img_url"]

            },
        )
    },
    compute_kind="PostgreSQL",
    partitions_def=weekly_partitions_def,
    group_name="gold",
)
def appearance(context, appearance) -> Output[pd.DataFrame]:
    return Output(appearance, metadata={"schema": "public", "records counts": len(appearance)})

@multi_asset(
    ins={"match_score": AssetIn(key=["gold", "football", "gold_match_score"])},
    outs={
        "match_score": AssetOut(
            io_manager_key="psql_io_manager",
            key_prefix=["warehouse", "public"],
            metadata={
                "columns": ['match_id', 'home_name', 'away_name', 'home_score','away_score', 'name', 'club', 'nation','position']
            },
        )
    },
    compute_kind="PostgreSQL",
    partitions_def=weekly_partitions_def,
    group_name="gold",
)
def match_score(context, match_score) -> Output[pd.DataFrame]:
    return Output(match_score, metadata={"schema": "public", "records counts": len(match_score)})

@multi_asset(
    ins={"player_info": AssetIn(key=["gold", "football", "gold_player_info"])},
    outs={
        "player_info": AssetOut(
            io_manager_key="psql_io_manager",
            key_prefix=["warehouse", "public"],
            metadata={
                "columns": ['team_id', 'player_id', 'name', 'club', 'position', 'nation', 'img_url','first_name', 'last_name']
            },
        )
    },
    compute_kind="PostgreSQL",
    group_name="gold",
)
def player_info(context, player_info) -> Output[pd.DataFrame]:
    return Output(player_info, metadata={"schema": "public", "records counts": len(player_info)})

@multi_asset(
    ins={"team_info": AssetIn(key=["gold", "football", "gold_team_info"])},
    outs={
        "team_info": AssetOut(
            io_manager_key="psql_io_manager",
            key_prefix=["warehouse", "public"],
            metadata={
                "columns": [
                    "team_id",
                    "stadium",
                    "capacity",
                    "image_link",
                    "location",
                    "name",
                    "link",
                    "est"]
            },
        )
    },
    compute_kind="PostgreSQL",
    group_name="gold",
)
def teams(context, team_info) -> Output[pd.DataFrame]:
    return Output(team_info, metadata={"schema": "public", "records counts": len(team_info)})
