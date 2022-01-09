import asyncio
from datetime import datetime

from pydantic import Field

from pybrook.models import InReport, OutReport, PyBrook, ReportField, Dependency

brook = PyBrook('redis://localhost')
app = brook.app


@brook.input('ztm-report', id_field='vehicle_id')
class ZTMReport(InReport):
    vehicle_id: int = Field(alias='vehicle_number')
    time: datetime
    latitude: float = Field(alias='lat')
    longitude: float = Field(alias='lon')
    brigade: str
    line: str = Field(alias='lines')


@brook.output('location-report')
class LocationReport(OutReport):
    vehicle_id = ReportField(ZTMReport.vehicle_id)
    latitude = ReportField(ZTMReport.latitude)
    longitude = ReportField(ZTMReport.longitude)
    line = ReportField(ZTMReport.line)
    time = ReportField(ZTMReport.time)


@brook.artificial_field('course')
async def course(lat: float = Dependency(ZTMReport.latitude)) -> float:
    await asyncio.sleep(2)
    return lat+1


@brook.output('course-report')
class CourseReport(OutReport):
    course_id = ReportField(course)


brook.set_meta(latitude_field=LocationReport.latitude,
               longitude_field=LocationReport.longitude,
               group_field=LocationReport.line,
               time_field=LocationReport.time)

if __name__ == '__main__':
    brook.run()
