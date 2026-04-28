from taskiq import TaskiqEvents, TaskiqState
from taskiq_nats import PullBasedJetStreamBroker

from src.config.db import init_mongo

broker = PullBasedJetStreamBroker(servers=["nats://localhost:4222"])


@broker.on_event(TaskiqEvents.WORKER_STARTUP)
async def startup(state: TaskiqState):
    state.mongo_client = await init_mongo()
    print("[STARTUP] Mongo Connection Init")


@broker.on_event(TaskiqEvents.WORKER_SHUTDOWN)
async def shut_down(state: TaskiqState):
    await state.mongo_client.aclose()
    print("[SHUTDOWN] Mongo Connection Closed")
