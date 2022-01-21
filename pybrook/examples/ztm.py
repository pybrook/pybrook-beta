import asyncio
from datetime import datetime, timedelta
from math import atan2, degrees
from time import sleep
from typing import Any, List, Optional, Sequence, Tuple, Union

from pydantic import Field

from pybrook.models import (
    InReport,
    OutReport,
    PyBrook,
    ReportField,
    dependency,
    historical_dependency,
)

brook = PyBrook('redis://localhost')
app = brook.app


@brook.input('ztm-report', id_field='vehicle_number')
class ZTMReport(InReport):
    vehicle_number: int
    time: datetime
    lat: float
    lon: float
    brigade: str
    line: str


@brook.output('location-report')
class LocationReport(OutReport):
    vehicle_number = ReportField(ZTMReport.vehicle_number)
    lat = ReportField(ZTMReport.lat)
    lon = ReportField(ZTMReport.lon)
    line = ReportField(ZTMReport.line)
    time = ReportField(ZTMReport.time)
    brigade = ReportField(ZTMReport.brigade)


@brook.artificial_field('direction')
async def direction(lat_history: Sequence[float] = historical_dependency(
    ZTMReport.lat, history_length=1),
                    lon_history: Sequence[float] = historical_dependency(
                        ZTMReport.lon, history_length=1),
                    lat: float = dependency(ZTMReport.lat),
                    lon: float = dependency(ZTMReport.lon)) -> Optional[float]:
    prev_lat, = lat_history
    prev_lon, = lon_history
    if prev_lat and prev_lon:
        return degrees(atan2(lon - prev_lon, lat - prev_lat))
    else:
        return None


@brook.output('direction-report')
class DirectionReport(OutReport):
    lat = ReportField(ZTMReport.lat)
    long = ReportField(ZTMReport.lon)
    direction = ReportField(direction)


brook.set_meta(latitude_field=LocationReport.lat,
               longitude_field=LocationReport.lon,
               group_field=LocationReport.line,
               time_field=LocationReport.time)
