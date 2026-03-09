import logging
import os
import sys
from typing import Iterable, Tuple

from pyflink.common import Duration, Row, Time
from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)
from pyflink.datastream.formats.json import JsonRowDeserializationSchema, JsonRowSerializationSchema
from pyflink.datastream.functions import ReduceFunction, ProcessWindowFunction
from pyflink.datastream.window import TumblingEventTimeWindows

logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger("window_aggregation")

click_type = Types.ROW_NAMED(
    ["event_id", "user_id", "product_id", "category", "event_ts_ms", "event_ts"],
    [Types.STRING(), Types.INT(), Types.INT(), Types.STRING(), Types.LONG(), Types.STRING()],
)

agg_out_type = Types.ROW_NAMED(
    ["product_id", "window_start_ms", "window_end_ms", "click_count"],
    [Types.INT(), Types.LONG(), Types.LONG(), Types.LONG()],
)


class SumReducer(ReduceFunction):
    def reduce(self, v1: Tuple[int, int], v2: Tuple[int, int]):
        return v1[0], v1[1] + v2[1]


class AddWindowInfo(ProcessWindowFunction):
    def process(self, key: int, context: ProcessWindowFunction.Context, elements: Iterable[Tuple[int, int]]):
        elem = next(iter(elements))
        w = context.window()
        yield Row(int(key), int(w.start), int(w.end), int(elem[1]))


def add_local_jars(env: StreamExecutionEnvironment):
    jars_dir = os.path.join(os.path.dirname(__file__), "jars")
    if not os.path.isdir(jars_dir):
        logger.warning("No jars directory: %s", jars_dir)
        return
    jars = [os.path.join(jars_dir, f) for f in os.listdir(jars_dir) if f.endswith(".jar")]
    if not jars:
        logger.warning("No .jar files found in %s. Kafka connector may be missing.", jars_dir)
        return
    for jar in jars:
        env.add_jars(f"file://{jar}")
    logger.info("Added %d jars from %s", len(jars), jars_dir)


def main():
    bootstrap = "kafka:29092"
    in_topic = os.getenv("KAFKA_TOPIC_CLICKS", "clicks")
    out_topic = os.getenv("KAFKA_TOPIC_AGG", "aggregated_clicks")
    group_id = os.getenv("FLINK_GROUP_ID", "flink-click-agg")

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(int(os.getenv("FLINK_PARALLELISM", "1")))
    add_local_jars(env)

    deser = JsonRowDeserializationSchema.builder().type_info(click_type).build()
    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(bootstrap)
        .set_topics(in_topic)
        .set_group_id(group_id)
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(deser)
        .build()
    )

    wm = (
        WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(5))
        .with_timestamp_assigner(lambda row, ts: int(row[4]))  # event_ts_ms
    )

    stream = env.from_source(source, wm, "KafkaClicks")
    mapped = stream.map(lambda r: (int(r[2]), 1), output_type=Types.TUPLE([Types.INT(), Types.LONG()]))

    windowed = (
        mapped.key_by(lambda x: x[0])
        .window(TumblingEventTimeWindows.of(Time.minutes(5)))
        .reduce(SumReducer(), AddWindowInfo(), output_type=agg_out_type)
    )

    ser = JsonRowSerializationSchema.builder().with_type_info(agg_out_type).build()
    sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(bootstrap)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(out_topic)
            .set_value_serialization_schema(ser)
            .build()
        )
        .build()
    )
    windowed.sink_to(sink)
    env.execute("Click Aggregation (5-min windows)")


if __name__ == "__main__":
    main()
