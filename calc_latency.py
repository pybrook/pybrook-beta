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
import datetime
from collections import defaultdict

import redis

def parse_redis_timestamp(timestamp: str) -> datetime.datetime:
    return datetime.datetime.fromtimestamp(int(timestamp.split('-', maxsplit=1)[0]) / 1000)

def calc_latency(out_streams):
    conn: redis.Redis = redis.from_url('redis://localhost', decode_responses=1, encoding='utf-8')

    deltas = defaultdict(dict)
    for stream in out_streams:
        for timestamp, payload in conn.xrange(stream, '-', '+'):
            msg_id = payload['@pb@msg_id']
            deltas[stream][msg_id] = (parse_redis_timestamp(timestamp) - datetime.datetime.fromisoformat(payload['time'].strip('"'))).total_seconds() * 1000
    deltas = dict(deltas)
    for stream in out_streams:
        print(stream)
        sorted_deltas = sorted(deltas[stream].values())
        msgs = len(deltas[stream])
        print('Messages: ', msgs)
        print('Median: ', sorted_deltas[msgs//2])
        print('Average latency: ', sum(deltas[stream].values()) / msgs)
        print('90th percentile: ', sorted_deltas[round(msgs*0.9)])


if __name__ == '__main__':
    calc_latency([':location-report', ':brigade-report', ':direction-report'])
