import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCollection
from pymongo import ASCENDING

logger = logging.getLogger(__name__)


class UserDatabase:
    def __init__(self, collection: AsyncIOMotorCollection):
        self.collection = collection
        self._ensure_indexes()

    async def _safe_db_operation(self, operation):
        try:
            return await operation
        except Exception as e:
            logger.error(f"Database operation failed: {str(e)}")
            return None

    async def _ensure_indexes(self):
        await self._safe_db_operation(
            self.collection.create_index([("user_id", ASCENDING)], unique=True)
        )

    async def get_user(self, user_id: int) -> Optional[dict]:
        return await self._safe_db_operation(
            self.collection.find_one({"user_id": user_id})
        )

    async def create_user(self, user_data: dict) -> None:
        default_data = {
            "created_at": datetime.now(timezone.utc),
            "files_uploaded": 0,
            "total_size": 0,
            "chunks_sent": 0,
            "file_type_counts": {},
            "largest_file_size": 0,
            "smallest_file_size": float('inf'),
            "successful_processes": 0,
            "total_attempts": 0,
            "fastest_process_time": float('inf'),
            "slowest_process_time": 0,
            "activity_hours": [],
            "last_active_date": datetime.now(timezone.utc).strftime('%Y-%m-%d'),
            "current_streak": 0,
            "longest_streak": 0,
            "achievements": []
        }
        user_data = {**default_data, **user_data}
        await self._safe_db_operation(
            self.collection.insert_one(user_data)
        )

    async def update_file_stats(self, user_id: int, file_size: int, file_type: str) -> None:
        update_data = {
            "$inc": {
                "files_uploaded": 1,
                "total_size": file_size,
                f"file_type_counts.{file_type}": 1
            },
            "$max": {"largest_file_size": file_size},
            "$min": {"smallest_file_size": file_size}
        }

        await self._safe_db_operation(
            self.collection.update_one({"user_id": user_id}, update_data)
        )

    async def update_process_stats(self, user_id: int, process_time: float,
                                   chunks_sent: int, success: bool) -> None:
        update_data = {
            "$inc": {
                "successful_processes": 1 if success else 0,
                "total_attempts": 1,
                "chunks_sent": chunks_sent
            },
            "$min": {"fastest_process_time": process_time},
            "$max": {"slowest_process_time": process_time}
        }

        await self._safe_db_operation(
            self.collection.update_one({"user_id": user_id}, update_data)
        )

    async def update_activity(self, user_id: int) -> None:
        now = datetime.now(timezone.utc)
        today = now.strftime('%Y-%m-%d')

        user = await self.get_user(user_id)
        if not user:
            logger.warning(
                f"Failed to fetch user {user_id} for activity update")
            return

        last_active = user.get('last_active_date')
        current_streak = user.get('current_streak', 0)

        if last_active == (now - timedelta(days=1)).strftime('%Y-%m-%d'):
            current_streak += 1
        elif last_active != today:
            current_streak = 1

        await self._safe_db_operation(
            self.collection.update_one(
                {"user_id": user_id},
                {
                    "$set": {
                        "last_active_date": today,
                        "current_streak": current_streak
                    },
                    "$max": {"longest_streak": current_streak},
                    "$addToSet": {"activity_hours": now.hour}
                }
            )
        )

    async def get_user_rank(self, user_id: int) -> tuple[int, int]:
        user = await self.get_user(user_id)
        total_users = await self._safe_db_operation(self.collection.count_documents({}))
        user_rank = await self._safe_db_operation(
            self.collection.count_documents(
                {"total_size": {"$gt": user.get("total_size", 0)}})
        )
        return (user_rank + 1 if user_rank is not None else 0), (total_users if total_users is not None else 0)

    async def check_and_add_achievements(self, user_id: int) -> list[str]:
        user = await self.get_user(user_id)
        if not user:
            return []

        new_achievements = []
        current_achievements = set(user.get('achievements', []))

        achievements = {
            "first_file": (user['files_uploaded'] >= 1, "🎯 First File"),
            "file_master": (user['files_uploaded'] >= 100, "🏆 File Master"),
            "data_heavyweight": (user['total_size'] >= 1_000_000_000, "💪 Data Heavyweight"),
            "speed_demon": (user.get('fastest_process_time', float('inf')) < 1.0, "⚡ Speed Demon"),
            "streak_warrior": (user.get('current_streak', 0) >= 7, "🔥 Streak Warrior"),
            "type_collector": (len(user.get('file_type_counts', {})) >= 5, "📚 Type Collector")
        }

        for condition, name in achievements.values():
            if condition and name not in current_achievements:
                new_achievements.append(name)
                current_achievements.add(name)

        if new_achievements:
            await self._safe_db_operation(
                self.collection.update_one(
                    {"user_id": user_id},
                    {"$set": {"achievements": list(current_achievements)}}
                )
            )

        return new_achievements


# Initialize database
MONGO_URI = "mongodb://localhost:27017"

client = AsyncIOMotorClient(MONGO_URI)
database = client.get_database('chunkit')
users_collection = database.get_collection("users")
user_db = UserDatabase(users_collection)