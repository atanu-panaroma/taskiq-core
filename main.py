import asyncio

from src.config.redis import redis_client
from src.config.taskiq_broker import broker
from src.tasks import llm_call


async def main():
    job_id = "pis2026"
    tasks = range(1, 10)

    await broker.startup()

    await redis_client.set(f"job:{job_id}:status", "pending")
    await redis_client.set(f"job:{job_id}:total", len(tasks))
    await redis_client.set(f"job:{job_id}:done", 0)

    for id in tasks:
        await llm_call.kiq(job_id=job_id, task_id=f"{job_id}:task{id}")

    return


if __name__ == "__main__":
    asyncio.run(main())
