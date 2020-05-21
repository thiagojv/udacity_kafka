"""Defines trends calculations for stations"""
import logging

import faust


logger = logging.getLogger(__name__)


class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool

class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str

app = faust.App("stations-stream", broker="kafka://localhost:9092", store="memory://")

topic = app.topic("raw.cta.stations", value_type=Station)
out_topic = app.topic("com.udacity.starter.cta.stations", partitions=1)

table = app.Table(
   "converted_stations",
   default=int,
   partitions=1,
   changelog_topic=out_topic,
)

#
#
# TODO: Using Faust, transform input `Station` records into `TransformedStation` records. Note that
# "line" is the color of the station. So if the `Station` record has the field `red` set to true,
# then you would set the `line` of the `TransformedStation` record to the string `"red"`
#
#
@app.agent(topic)
async def transformStation(stations):
    async for station in stations:
        if station.blue:
            line = 0
        elif station.green:
            line = 1
        elif station.red:
            line = 2

        table[station.stop_id] = TransformedStation(
            station_id = station.station_id,
            station_name = station.station_name,
            order = station.order,
            line = line
        )

if __name__ == "__main__":
    app.main()
