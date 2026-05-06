from logging import getLogger
from typing import Annotated

from openai import AsyncClient
from taskiq import Context, TaskiqDepends

from src.config.db import LLMResponse
from src.config.redis import redis_client
from src.config.taskiq_broker import broker
from src.utils import log_nats_metrics, log_redis_cache_metrics

logger = getLogger(__name__)

openai_client = AsyncClient()


@broker.task
async def llm_call(
    job_id: str, task_id: str, context: Annotated[Context, TaskiqDepends()]
):
    try:
        logger.info(f"New Task Added: {task_id}")
        # await log_redis_cache_metrics()
        # await log_nats_metrics(context.broker)

        response = await openai_client.responses.create(
            model="gpt-5.4-nano",
            max_output_tokens=50,
            input="What is the boiling temperature of water?",
        )
        await LLMResponse(task_id=task_id, output_text=response.output_text).save()
        logger.info(f"Task Completed: {task_id}")
        return
    except Exception as e:
        logger.error(e)
    finally:
        done = await redis_client.incrby(f"job:{job_id}:done", 1)
        total = await redis_client.get(f"job:{job_id}:total")
        if done == int(total):
            await redis_client.set(f"job:{job_id}:status", "completed")
