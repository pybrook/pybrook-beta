"""
This module contains definitions of Pydantic models
used to tell the frontend about available streams & fields.

This is required, because the OpenAPI documentation doesn't support WebSockets.
"""

#  PyBrook
#
#  Copyright (C) 2023  Michał Rokita
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#

from typing import Any, Optional

from pydantic import BaseModel

from pybrook.config import MSG_ID_FIELD, SPECIAL_CHAR


class StreamInfo(BaseModel):
    stream_name: str
    websocket_path: str
    report_schema: dict[Any, Any]


class FieldInfo(BaseModel):
    stream_name: str
    field_name: str


class PyBrookSchema(BaseModel):
    streams: list[StreamInfo] = []
    special_char: str = SPECIAL_CHAR
    msg_id_field: str = MSG_ID_FIELD
    latitude_field: Optional[FieldInfo] = None
    longitude_field: Optional[FieldInfo] = None
    time_field: Optional[FieldInfo] = None
    group_field: Optional[FieldInfo] = None
    direction_field: Optional[FieldInfo] = None
