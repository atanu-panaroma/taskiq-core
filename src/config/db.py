import os

from beanie import init_beanie, Document
from pymongo import AsyncMongoClient

uri = os.environ["MONGO_URI"]
db_name = os.environ["DB_NAME"]


class LLMResponse(Document):
    task_id: str
    output_text: str

    class Settings:
        name = "responses"


async def init_mongo():
    client = AsyncMongoClient(uri)
    await init_beanie(
        database=client.get_database(db_name), document_models=[LLMResponse]
    )
    return client
