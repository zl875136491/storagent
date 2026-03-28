from src.core.database import init_db, document_models, close_db
import asyncio
from typing import Type, List
from beanie import Document

document_models: List[Type[Document]] = document_models

async def main():
  await init_db()
  for model in document_models:
    await model.get_motor_collection().drop_indexes()
  await close_db()
  
if __name__ == "__main__":
  asyncio.run(main())