import json
import logging
import os
from dotenv import load_dotenv
from datetime import datetime, timezone
from typing import Dict, List
import asyncio

from aiogram.filters import Command
from motor.motor_asyncio import AsyncIOMotorClient
from aiogram import Bot, Dispatcher, types

load_dotenv()
TOKEN = str(os.environ.get("TOKEN", default=0))

client = AsyncIOMotorClient('mongodb://localhost:27017')
db = client['test']
collection = db['users']


async def aggregate_salaries(dt_from: str, dt_upto: str, group_type: str) -> Dict[str, List]:
    group_day = {
        "$dateToString": {
            "format": "%Y-%m-%d",
            "date": "$dt"
        }
    }
    group_month = {
        "$dateToString": {
            "format": "%Y-%m",
            "date": "$dt"
        }
    }
    group_hour = {
        "$dateToString": {
            "format": "%Y-%m-%dT%H",
            "date": "$dt"
        }
    }

    group_dict = group_day if group_type == 'day' else group_month if group_type == 'month' else group_hour
    pipeline = [
        {
            "$match": {
                "dt": {
                    "$gte": datetime.fromisoformat(dt_from),
                    "$lte": datetime.fromisoformat(dt_upto)
                }
            }
        },
        {
            "$group": {
                "_id": group_dict,
                "totalSalary": {"$sum": "$value"}
            }
        },
        {
            "$project": {
                "_id": {
                    "$dateToString": {
                        "format": "%Y-%m-%dT00:00:00",
                        "date": {"$toDate": "$_id"}
                    }
                },
                "totalSalary": 1
            }
        },
        {
            "$sort": {"_id": 1}
        }
    ]

    cursor = collection.aggregate(pipeline)

    labels = []
    dataset = []
    async for document in cursor:
        labels.append(document['_id'])
        dataset.append(document['totalSalary'])

    return {"dataset": dataset, "labels": labels}


logging.basicConfig(level=logging.INFO)

bot = Bot(token=TOKEN)
# Диспетчер
dp = Dispatcher()


@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    await message.reply(
        f'Привет {message.from_user.username}! \n'
        'Отправьте мне данные для агрегации в формате JSON.')


@dp.message()
async def handle_message(message: types.Message):
    data = json.loads(message.text)
    result = await aggregate_salaries(data['dt_from'], data['dt_upto'], data['group_type'])
    await message.answer(json.dumps(result))


async def main():
    await dp.start_polling(bot)


if __name__ == '__main__':
    asyncio.run(main())
