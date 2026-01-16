#!/usr/bin/env python3
import asyncio
from kafka import KafkaConsumer
import json
from prefect.client import get_client


async def listen_kafka_and_trigger():
    consumer = KafkaConsumer(
        'events-topic',
        bootstrap_servers='localhost:9092',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='latest',
        enable_auto_commit=True
    )

    batch = []
    BATCH_SIZE = 100

    async with get_client() as client:

        for message in consumer:
            batch.append(message.value)

            if len(batch) >= BATCH_SIZE:

                await client.create_flow_run_from_deployment(
                    deployment_id="74a505c5-aefb-49b6-adf5-69f5bc3ebb86",
                    parameters={"events": batch.copy()}
                )

                batch = []  # очищаем батч


if __name__ == "__main__":
    asyncio.run(listen_kafka_and_trigger())