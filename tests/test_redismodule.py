import redis

from pybrook.config import MSG_ID_FIELD
from pybrook.redis_plugin_integration import (
    BrookConfig,
    DependencyResolver,
    DependencyField,
    Dependency,
)


def test_resolver(redis_sync: redis.Redis):
    redis_sync.execute_command(
        "PB.SETCONFIG",
        BrookConfig(
            dependency_resolvers={
                "out_dr": DependencyResolver(
                    output_stream_key="out",
                    inputs=[
                        Dependency(
                            stream_key="stream_a",
                            fields=[DependencyField(src="x", dst="x_dest")],
                        ),
                        Dependency(
                            stream_key="stream_b",
                            fields=[DependencyField(src="y", dst="y_dest")],
                        ),
                    ],
                )
            }
        ).json(),
    )
    redis_sync.xadd("stream_a", {MSG_ID_FIELD: '"1:1"', "x": "1"})
    redis_sync.xadd("stream_b", {MSG_ID_FIELD: '"1:1"', "y": "2"})
    assert [m for _, m in redis_sync.xrevrange("out", "+", "-")] == [
        {"@pb@msg_id": '"1:1"', "x_dest": "1", "y_dest": "2"}
    ]
