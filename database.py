#[file name]: database.py
#[file content begin]
from pymongo import MongoClient
from config import MONGO_URI

# Database connection
mongo_client = MongoClient(MONGO_URI)
db = mongo_client["N4_Bots"]
users_collection = db["users_sequence"]

# Initialize broadcast_stats collection if it doesn't exist
if "broadcast_stats" not in db.list_collection_names():
    db.create_collection("broadcast_stats")

# Data storage (global variables)
# These store temporary session data
user_sequences = {}
user_notification_msg = {}
update_tasks = {}
user_settings = {} 
processing_users = set()  # 🔥 ADDED: To prevent multiple "Processing" messages
user_ls_state = {}  # NEW: Store LS command state
user_mode = {}  # NEW: Store user mode (file or caption)

def get_user_stats(user_id):
    """Get user statistics from database"""
    return users_collection.find_one({"user_id": user_id})

def update_user_stats(user_id, files_count, username):
    """Update user statistics in database"""
    users_collection.update_one(
        {"user_id": user_id}, 
        {"$inc": {"files_sequenced": files_count}, "$set": {"username": username}}, 
        upsert=True
    )

def get_top_users(limit=5):
    """Get top users by files sequenced"""
    return list(users_collection.find().sort("files_sequenced", -1).limit(limit))

def get_total_users():
    """Get total number of users"""
    return users_collection.count_documents({})

def get_all_users():
    """Get all users for broadcasting"""
    return list(users_collection.find({}))

def save_broadcast_stats(total, success, failed, blocked):
    """Save broadcast statistics"""
    from datetime import datetime
    db.broadcast_stats.update_one(
        {"_id": "latest"},
        {
            "$set": {
                "total": total,
                "success": success,
                "failed": failed,
                "blocked": blocked,
                "date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
        },
        upsert=True
    )

def get_broadcast_stats():
    """Get latest broadcast statistics"""
    return db.broadcast_stats.find_one({"_id": "latest"})

# NEW: Get user mode
def get_user_mode(user_id):
    """Get user mode (default is 'file' if not set)"""
    return user_mode.get(user_id, "file")

# NEW: Set user mode
def set_user_mode(user_id, mode):
    """Set user mode ('file' or 'caption')"""
    if mode in ["file", "caption"]:
        user_mode[user_id] = mode
        return True
    return False

# NEW: Clear user session data
def clear_user_session(user_id):
    """Clear all temporary session data for a user"""
    user_sequences.pop(user_id, None)
    user_notification_msg.pop(user_id, None)
    user_settings.pop(user_id, None)
    user_ls_state.pop(user_id, None)
    user_mode.pop(user_id, None)
    
    # Cancel any pending update task
    if user_id in update_tasks:
        try:
            update_tasks[user_id].cancel()
        except:
            pass
        update_tasks.pop(user_id, None)
    
    # Remove from processing set
    if user_id in processing_users:
        processing_users.remove(user_id)

# NEW: Initialize user session
def init_user_session(user_id):
    """Initialize a new session for a user"""
    clear_user_session(user_id)  # Clean any old session first
    user_sequences[user_id] = []
    user_settings[user_id] = "per_ep"  # Default mode
    return True

# NEW: Get user's sequence count
def get_user_sequence_count(user_id):
    """Get number of files in user's current sequence"""
    return len(user_sequences.get(user_id, []))

# NEW: Add file to user's sequence
def add_file_to_sequence(user_id, file_data):
    """Add a file to user's sequence"""
    if user_id not in user_sequences:
        user_sequences[user_id] = []
    user_sequences[user_id].append(file_data)
    return len(user_sequences[user_id])

# NEW: Get user's files for sending
def get_user_files_for_sending(user_id):
    """Get and sort user's files based on their current mode"""
    if user_id not in user_sequences or not user_sequences[user_id]:
        return []
    
    files = user_sequences[user_id]
    mode = user_settings.get(user_id, "per_ep")
    
    if mode == "per_ep":
        return sorted(files, key=lambda x: (
            x.get("info", {}).get("season", 1),
            x.get("info", {}).get("episode", 0),
            x.get("info", {}).get("quality", 0)
        ))
    else:
        return sorted(files, key=lambda x: (
            x.get("info", {}).get("season", 1),
            x.get("info", {}).get("quality", 0),
            x.get("info", {}).get("episode", 0)
        ))

# NEW: Check if user has active sequence
def has_active_sequence(user_id):
    """Check if user has an active sequence session"""
    return user_id in user_sequences and len(user_sequences.get(user_id, [])) > 0

# NEW: Create indexes for better performance
def create_indexes():
    """Create database indexes for better performance"""
    users_collection.create_index([("user_id", 1)], unique=True)
    users_collection.create_index([("files_sequenced", -1)])
    db.broadcast_stats.create_index([("date", -1)])
    print("✅ Database indexes created successfully")

# Create indexes on startup
create_indexes()

print(f"✅ Database initialized successfully")
print(f"✅ Connected to: {MONGO_URI}")
print(f"✅ Collections: {db.list_collection_names()}")
#[file content end]
