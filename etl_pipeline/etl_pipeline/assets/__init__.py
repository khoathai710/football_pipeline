from . import bronze,silver,gold,warehouse
from dagster import Definitions, load_assets_from_modules

assets = load_assets_from_modules([bronze,silver,gold,warehouse])