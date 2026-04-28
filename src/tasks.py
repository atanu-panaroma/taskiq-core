from typing import Annotated

from openai import AsyncClient
from taskiq import Context, TaskiqDepends

from src.config.db import LLMResponse
from src.config.taskiq_broker import broker
from src.config.redis import redis_client

openai_client = AsyncClient()


@broker.task
async def mock_llm_call(
    job_id: str, task_id: str, context: Annotated[Context, TaskiqDepends()]
):
    try:
        print(f"[+] New Task Added: {task_id}")

        response = await openai_client.responses.create(
            model="gpt-5.4-nano",
            max_output_tokens=50,
            input="What is the boiling temperature of water?",
        )
        await LLMResponse(task_id=task_id, output_text=response.output_text).save()
        return print(f"[-] Task Completed: {task_id}")
    except Exception as e:
        print(f"[ERROR] {e}")
    finally:
        done = await redis_client.incrby(f"job:{job_id}:done", 1)
        total = await redis_client.get(f"job:{job_id}:total")        
        if done == int(total):
            await redis_client.set(f"job:{job_id}:status", "completed")
