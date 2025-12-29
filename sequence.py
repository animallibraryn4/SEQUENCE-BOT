# [file name]: sequence.py
import asyncio
import re
import time
import subprocess
from pyrogram import Client, filters
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from pyrogram.errors import UserNotParticipant, FloodWait, ChatAdminRequired, ChannelPrivate
from config import API_HASH, API_ID, BOT_TOKEN, MONGO_URI, START_PIC, START_MSG, HELP_TXT, COMMAND_TXT, OWNER_ID, FSUB_CHANNEL, FSUB_CHANNEL_2, FSUB_CHANNEL_3

# Import from our split modules
from database import (
    user_sequences, user_notification_msg, update_tasks, 
    user_settings, processing_users, user_ls_state,
    users_collection, update_user_stats, get_user_mode, set_user_mode
)
from start import is_subscribed

# Import merging state from merging.py
try:
    from merging import merging_users
    MERGING_AVAILABLE = True
    if not check_ffmpeg_available():
        print("⚠️ FFmpeg is not available. Merging feature will be disabled.")
        MERGING_AVAILABLE = False
except ImportError as e:
    print(f"Merging module import error: {e}")
    MERGING_AVAILABLE = False
    merging_users = {}

# Bot start time for uptime calculation
BOT_START_TIME = time.time()

# Check if FFmpeg is available
def check_ffmpeg_available():
    """Check if FFmpeg is installed and available"""
    try:
        result = subprocess.run(['ffmpeg', '-version'], capture_output=True, text=True)
        return result.returncode == 0
    except:
        return False

# --- REFINED PARSING ENGINE ---
def parse_file_info(text):
    """Parse file information from text (either filename or caption)"""
    if not text:
        return {"season": 1, "episode": 0, "quality": 0}
    
    quality_match = re.search(r'(\d{3,4})[pP]', text)
    quality = int(quality_match.group(1)) if quality_match else 0
    clean_name = re.sub(r'\d{3,4}[pP]', '', text)

    season_match = re.search(r'[sS](?:eason)?\s*(\d+)', clean_name)
    season = int(season_match.group(1)) if season_match else 1
    
    ep_match = re.search(r'[eE](?:p(?:isode)?)?\s*(\d+)', clean_name)
    if ep_match:
        episode = int(ep_match.group(1))
    else:
        nums = re.findall(r'\b\d+\b', clean_name)
        episode = int(nums[-1]) if nums else 0

    return {"season": season, "episode": episode, "quality": quality}

# --- UPDATED: Extract message ID from Telegram link ---
def extract_message_info(link):
    """
    Extract chat ID and message ID from Telegram message link
    Supports formats:
    - https://t.me/c/chat_id/message_id (private channels)
    - https://t.me/username/message_id (public channels/groups)
    """
    try:
        link = link.strip()
        
        if "/c/" in link:
            # Private channel link format: https://t.me/c/1234567890/123
            parts = link.split("/")
            
            if len(parts) < 6:
                return None, None
                
            # Get the chat_id part (it's 1234567890 in the example)
            chat_id_str = parts[4]
            
            # Check if it needs the -100 prefix
            if not chat_id_str.startswith("-100"):
                if chat_id_str.startswith("100"):
                    chat_id = int("-" + chat_id_str)
                else:
                    chat_id = int("-100" + chat_id_str)
            else:
                chat_id = int(chat_id_str)
            
            message_id = int(parts[5])
            return chat_id, message_id
            
        elif "t.me/" in link:
            # Public channel/group link format: https://t.me/username/123
            parts = link.split("/")
            if len(parts) < 5:
                return None, None
                
            username = parts[3].strip()
            if not username:
                return None, None
                
            message_id = int(parts[4])
            return username, message_id
            
    except Exception as e:
        print(f"Error parsing link {link}: {e}")
        
    return None, None

# --- UPDATED: Check if bot is admin in chat ---
async def check_bot_admin(client, chat_id):
    """Check if bot is admin in the given chat/channel"""
    try:
        # If chat_id is a username string, get the actual chat ID
        if isinstance(chat_id, str):
            try:
                chat = await client.get_chat(chat_id)
                chat_id = chat.id
            except Exception as e:
                print(f"Error getting chat from username {chat_id}: {e}")
                return False
        
        # Get bot's member status
        bot_member = await client.get_chat_member(chat_id, "me")
        
        # Check admin status
        admin_statuses = ["administrator", "creator"]
        status_str = str(bot_member.status).lower()
        
        for admin_status in admin_statuses:
            if admin_status in status_str:
                return True
        
        return False
            
    except (ChatAdminRequired, ChannelPrivate) as e:
        print(f"Admin check failed (ChatAdminRequired/ChannelPrivate): {e}")
        return False
    except Exception as e:
        print(f"Admin check error: {e}")
        return False

# --- NEW: Get messages between two message IDs ---
async def get_messages_between(client, chat_id, start_msg_id, end_msg_id):
    """Fetch all messages between start_msg_id and end_msg_id (inclusive)"""
    messages = []
    
    # Ensure start is smaller than end
    if start_msg_id > end_msg_id:
        start_msg_id, end_msg_id = end_msg_id, start_msg_id
    
    try:
        # Fetch messages in batches
        for msg_id in range(start_msg_id, end_msg_id + 1):
            try:
                msg = await client.get_messages(chat_id, msg_id)
                if msg and (msg.document or msg.video or msg.audio):
                    messages.append(msg)
                # Small delay to avoid flood
                await asyncio.sleep(0.1)
            except Exception as e:
                print(f"Error fetching message {msg_id}: {e}")
                continue
    except Exception as e:
        print(f"Error in get_messages_between: {e}")
    
    return messages

# --- UPDATED: Sequence files from messages with mode support ---
async def sequence_messages(client, messages, mode="per_ep", user_id=None):
    """Convert messages to sequence format with File/Caption mode support"""
    files_data = []
    
    # Get user's current mode
    if user_id:
        current_mode = get_user_mode(user_id)
    else:
        current_mode = "file"  # Default to file mode if no user_id provided
    
    for msg in messages:
        file_obj = msg.document or msg.video or msg.audio
        if file_obj:
            if current_mode == "caption":
                # Caption mode: Use caption text
                if msg.caption:
                    text_to_parse = msg.caption
                else:
                    # No caption found, skip this file
                    continue
            else:
                # File mode: Use filename
                file_name = file_obj.file_name if file_obj else "Unknown"
                text_to_parse = file_name
            
            info = parse_file_info(text_to_parse)
            
            files_data.append({
                "filename": text_to_parse,
                "msg_id": msg.id,
                "chat_id": msg.chat.id,
                "info": info
            })
    
    # Sort based on mode (per_ep or group)
    if mode == "per_ep":
        sorted_files = sorted(files_data, key=lambda x: (x["info"]["season"], x["info"]["episode"], x["info"]["quality"]))
    else:
        sorted_files = sorted(files_data, key=lambda x: (x["info"]["season"], x["info"]["quality"], x["info"]["episode"]))
    
    return sorted_files, current_mode

# ----------------------- SEQUENCE FUNCTIONS -----------------------
async def send_sequence_files(client, message, user_id):
    if user_id not in user_sequences or not user_sequences[user_id]:
        await message.edit_text("<blockquote>Nᴏ ғɪʟᴇs ɪɴ sᴇǫᴜᴇɴᴄᴇ!</blockquote>")
        return

    files_data = user_sequences[user_id]
    mode = user_settings.get(user_id, "per_ep")
    
    if mode == "per_ep":
        sorted_files = sorted(files_data, key=lambda x: (x["info"]["season"], x["info"]["episode"], x["info"]["quality"]))
    else:
        sorted_files = sorted(files_data, key=lambda x: (x["info"]["season"], x["info"]["quality"], x["info"]["episode"]))

    await message.edit_text(f"<blockquote>📤 Sending {len(sorted_files)} files... Please wait.</blockquote>")

    for file in sorted_files:
        try:
            await client.copy_message(message.chat.id, from_chat_id=file["chat_id"], message_id=file["msg_id"])
            await asyncio.sleep(0.8) 
        except:
            continue

    update_user_stats(user_id, len(files_data), message.from_user.first_name)
    
    try:
        await message.delete()
    except:
        pass
    
    user_sequences.pop(user_id, None)
    user_notification_msg.pop(user_id, None)
    
    await client.send_message(message.chat.id, "<blockquote><b>✅ ᴀʟʟ ғɪʟᴇs sᴇǫᴜᴇɴᴄᴇᴅ ꜱᴜᴄᴄᴇꜱꜰᴜʟʟʏ!</b></blockquote>")

# ----------------------- UPDATE NOTIFICATION FUNCTION -----------------------
async def update_notification(client, user_id, chat_id):
    await asyncio.sleep(3) 
    if user_id not in user_sequences:
        return
        
    count = len(user_sequences[user_id])
    buttons = InlineKeyboardMarkup([
        [InlineKeyboardButton("Send", callback_data='send_sequence'), 
         InlineKeyboardButton("Cancel", callback_data='cancel_sequence')]
    ])
    
    text = f"<blockquote>ғɪʟᴇs ᴀᴅᴅᴇᴅ! ᴄʟɪᴄᴋ ʙᴜᴛᴛᴏɴs ʙᴇʟᴏᴡ:</blockquote>\n<blockquote>ᴛᴏᴛᴀʟ ғɪʟᴇs: {count}</blockquote>"
    
    if user_id in user_notification_msg:
        try:
            await client.edit_message_text(
                chat_id=user_notification_msg[user_id]["chat_id"],
                message_id=user_notification_msg[user_id]["msg_id"],
                text=text,
                reply_markup=buttons
            )
        except:
            pass
    else:
        msg = await client.send_message(chat_id, text, reply_markup=buttons)
        user_notification_msg[user_id] = {
            "msg_id": msg.id,
            "chat_id": chat_id
        }

# ----------------------- SETUP FUNCTION FOR ALL SEQUENCE HANDLERS -----------------------
def setup_sequence_handlers(app: Client):
    """Setup all sequence-related handlers"""
    
    @app.on_message(filters.command("sequence"))
    async def start_sequence(client, message):
        if not await is_subscribed(client, message):
            return
            
        user_id = message.from_user.id
        user_sequences[user_id] = []
        if user_id in user_notification_msg:
            del user_notification_msg[user_id]
        
        # Get current mode
        current_mode = get_user_mode(user_id)
        mode_text = "File mode (using filename)" if current_mode == "file" else "Caption mode (using file caption)"
        
        await message.reply_text(
            f"<blockquote><b>ғɪʟᴇ sᴇǫᴜᴇɴᴄᴇ ᴍᴏᴅᴇ sᴛᴀʀᴛᴇᴅ!</b></blockquote>\n"
            f"<blockquote>Current mode: {mode_text}</blockquote>\n"
            f"<blockquote>Send your files now</blockquote>"
        )

    @app.on_message(filters.command("fileseq"))
    async def quality_mode_cmd(client, message):
        if not await is_subscribed(client, message):
            return

        text = (
        "<b>➲ CHOOSE FILE ORDERS</b>\n\n"
        "<blockquote>ꜱᴇʟᴇᴄᴛ ʜᴏᴡ ʏᴏᴜʀ ꜰɪʟᴇs ᴡɪʟʟ ʙᴇ sᴇɴᴛ\n</blockquote>"        
        "<b>↬ᴇᴘɪsᴏᴅᴇ ꜰʟᴏᴡ</b>:\n"
        "<blockquote>ꜰɪʟᴇs ᴀʀᴇ sᴇɴᴛ ᴇᴘɪsᴏᴅᴇ ʙʏ ᴇᴘɪsᴏᴅᴇ.\n"
        "ᴏʀᴅᴇʀ: sᴇᴀsᴏɴ → ᴇᴘɪsᴏᴅᴇ → ǫᴜᴀʟɪᴛʏ\n\n"
        "<i>ᴇxᴀᴍᴘʟᴇ:</i>\n"
        "S1E1 → ᴀʟʟ ǫᴜᴀʟɪᴛɪᴇs\n"
        "S1E2 → ᴀʟʟ ǫᴜᴀʟɪᴛɪᴇs\n</blockquote>"
        "<b>↬ǫᴜᴀʟɪᴛʏ ꜰʟᴏᴡ</b>:\n"
        "<blockquote>ꜰɪʟᴇs ᴀʀᴇ sᴇɴᴛ ǫᴜᴀʟɪᴛʏ ʙʏ ǫᴜᴀʟɪᴛʏ ɪɴsɪᴅᴇ ᴇᴀᴄʜ sᴇᴀsᴏɴ.\n"
        "ᴏʀᴅᴇʀ: sᴇᴀsᴏɴ → ǫᴜᴀʟɪᴛʏ → ᴇᴘɪsᴏᴅᴇ\n\n"
        "ᴇxᴀᴍᴘʟᴇ:\n"
        "sᴇᴀsᴏɴ 1 → ᴀʟʟ 480ᴘ\n"
        "sᴇᴀsᴏɴ 1 → ᴀʟʟ 720ᴘ</blockquote>"
        )
        
        buttons = InlineKeyboardMarkup([
            [InlineKeyboardButton("ᴇᴘɪsᴏᴅᴇ ꜰʟᴏᴡ", callback_data='set_mode_per_ep')],
            [InlineKeyboardButton("ǫᴜᴀʟɪᴛʏ ꜰʟᴏᴡ", callback_data='set_mode_group')]
        ])
        await message.reply_text(text, reply_markup=buttons)

    @app.on_message(filters.command("sf"))
    async def switch_mode_cmd(client, message):
        """Handle /sf command to switch between File mode and Caption mode"""
        if not await is_subscribed(client, message):
            return

        user_id = message.from_user.id
        current_mode = get_user_mode(user_id)
        
        # Create buttons based on current mode
        if current_mode == "file":
            buttons = InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ File mode", callback_data="mode_file")],
                [InlineKeyboardButton("Caption mode", callback_data="mode_caption")],
                [InlineKeyboardButton("Close", callback_data="close_mode")]
            ])
        else:
            buttons = InlineKeyboardMarkup([
                [InlineKeyboardButton("File mode", callback_data="mode_file")],
                [InlineKeyboardButton("✅ Caption mode", callback_data="mode_caption")],
                [InlineKeyboardButton("Close", callback_data="close_mode")]
            ])
        
        text = f"""<b>🔄 Sequence Mode Settings</b>

<blockquote><b>Current Mode:</b> {'File mode' if current_mode == 'file' else 'Caption mode'}

<b>File mode:</b> Sequence files using filename
<b>Caption mode:</b> Sequence files using file caption

ℹ️ <i>If no caption is found in Caption mode, those files will be skipped.</i></blockquote>"""
        
        await message.reply_text(text, reply_markup=buttons)

    @app.on_message(filters.command("ls"))
    async def ls_command(client, message):
        """Handle /ls command for channel file sequencing"""
        if not await is_subscribed(client, message):
            return
        
        user_id = message.from_user.id
        
        # Get user's current mode
        current_mode = get_user_mode(user_id)
        mode_text = "File mode" if current_mode == "file" else "Caption mode"
        
        # Initialize LS state for user WITH mode information
        user_ls_state[user_id] = {
            "step": 1,  # 1: waiting for first link, 2: waiting for second link
            "first_link": None,
            "first_chat": None,
            "first_msg_id": None,
            "mode": user_settings.get(user_id, "per_ep"),
            "current_mode": current_mode  # Store user's File/Caption mode
        }
        
        await message.reply_text(
            f"<blockquote><b>📁 LS MODE ACTIVATED</b></blockquote>\n\n"
            f"<blockquote>Current mode: <b>{mode_text}</b></blockquote>\n"
            f"<blockquote>Please send the first file link from the channel/group.</blockquote>\n"
            f"<blockquote>ℹ️ Note: For private channels, the bot must be an admin.</blockquote>"
        )

    @app.on_message(filters.document | filters.video | filters.audio)
    async def store_file(client, message):
        # First check if user is in merging mode
        if MERGING_AVAILABLE and message.from_user.id in merging_users:
            # Let handler_merging.py handle merging files
            return
        
        # Check force subscribe
        if not await is_subscribed(client, message):
            return
            
        user_id = message.from_user.id
        
        # Check if we are currently in a sequence session
        if user_id in user_sequences:
            file_obj = message.document or message.video or message.audio
            current_mode = get_user_mode(user_id)
            
            if current_mode == "caption":
                # Caption mode: Use caption text or ask to switch mode
                if message.caption:
                    text_to_parse = message.caption
                else:
                    # No caption found, ask user to switch mode
                    await message.reply_text(
                        "<blockquote>❌ No caption found for this file!</blockquote>\n"
                        "<blockquote>Please switch to File mode using /sf or add a caption to the file.</blockquote>"
                    )
                    return
            else:
                # File mode: Use filename
                file_name = file_obj.file_name if file_obj else "Unknown"
                text_to_parse = file_name
            
            info = parse_file_info(text_to_parse)
            
            user_sequences[user_id].append({
                "filename": text_to_parse,
                "msg_id": message.id,
                "chat_id": message.chat.id,
                "info": info
            })
            
            # Get current count
            current_count = len(user_sequences[user_id])

            # Send "Processing" ONLY if 20+ files are added
            if user_id not in user_notification_msg and user_id not in processing_users and current_count >= 20:
                processing_users.add(user_id)  # Lock the user
                try:
                    msg = await client.send_message(
                        message.chat.id,
                        "<blockquote>⏳ Processing files… please wait</blockquote>"
                    )
                    user_notification_msg[user_id] = {
                        "msg_id": msg.id,
                        "chat_id": message.chat.id
                    }
                finally:
                    processing_users.remove(user_id)  # Release the lock
            
            # Cancel previous update task and start a new one (Debouncing)
            if user_id in update_tasks: 
                update_tasks[user_id].cancel()
            update_tasks[user_id] = asyncio.create_task(update_notification(client, user_id, message.chat.id))

    @app.on_message(filters.text & filters.regex(r'https?://t\.me/'))
    async def handle_ls_links(client, message):
        """Handle Telegram links for LS mode"""
        user_id = message.from_user.id
        
        if user_id not in user_ls_state:
            return  # Not in LS mode
        
        ls_data = user_ls_state[user_id]
        link = message.text.strip()
        
        try:
            if ls_data["step"] == 1:
                # First link
                chat_info, msg_id = extract_message_info(link)
                
                if not msg_id:
                    await message.reply_text("<blockquote>❌ Invalid link format. Please send a valid Telegram message link.</blockquote>")
                    return
                
                # Store first link data
                user_ls_state[user_id].update({
                    "first_link": link,
                    "first_chat": chat_info,
                    "first_msg_id": msg_id,
                    "step": 2
                })
                
                current_mode = ls_data.get("current_mode", "file")
                mode_text = "File mode" if current_mode == "file" else "Caption mode"
                
                await message.reply_text(
                    f"<blockquote><b>✅ First link received!</b></blockquote>\n\n"
                    f"<blockquote>Current mode: <b>{mode_text}</b></blockquote>\n"
                    f"<blockquote>Now please send the second file link from the same channel/group.</blockquote>"
                )
                
            elif ls_data["step"] == 2:
                # Second link
                second_chat, second_msg_id = extract_message_info(link)
                
                if not second_msg_id:
                    await message.reply_text("<blockquote>❌ Invalid link format. Please send a valid Telegram message link.</blockquote>")
                    return
                
                # Check if both links are from same chat
                first_chat = ls_data["first_chat"]
                
                # Simple comparison for now - we'll handle the actual processing in callback
                if isinstance(first_chat, int) and isinstance(second_chat, int):
                    if first_chat != second_chat:
                        await message.reply_text("<blockquote>❌ Both links must be from the same channel/group.</blockquote>")
                        del user_ls_state[user_id]
                        return
                elif isinstance(first_chat, str) and isinstance(second_chat, str):
                    if first_chat != second_chat:
                        await message.reply_text("<blockquote>❌ Both links must be from the same channel/group.</blockquote>")
                        del user_ls_state[user_id]
                        return
                
                # Store second link data
                user_ls_state[user_id].update({
                    "second_link": link,
                    "second_chat": second_chat,
                    "second_msg_id": second_msg_id
                })
                
                current_mode = ls_data.get("current_mode", "file")
                mode_text = "File mode" if current_mode == "file" else "Caption mode"
                
                # Show buttons for Chat/Channel choice
                buttons = InlineKeyboardMarkup([
                    [InlineKeyboardButton("💬 Chat", callback_data=f"ls_chat_{user_id}")],
                    [InlineKeyboardButton("📢 Channel", callback_data=f"ls_channel_{user_id}")],
                    [InlineKeyboardButton("❌ Close", callback_data=f"ls_close_{user_id}")]
                ])
                
                await message.reply_text(
                    f"<blockquote><b>✅ Both links received!</b></blockquote>\n\n"
                    f"<blockquote>Current mode: <b>{mode_text}</b></blockquote>\n"
                    f"<blockquote>Choose where to send sequenced files:</blockquote>",
                    reply_markup=buttons
                )
                
        except Exception as e:
            print(f"Error handling LS link: {e}")
            await message.reply_text("<blockquote>❌ An error occurred. Please try again with valid links.</blockquote>")
            if user_id in user_ls_state:
                del user_ls_state[user_id]

    # ----------------------- CALLBACK HANDLERS -----------------------
    @app.on_callback_query(filters.regex(r'^set_mode_(group|per_ep)$'))
    async def set_mode_callback(client, query):
        data = query.data
        user_id = query.from_user.id
        
        if data == "set_mode_group":
            user_settings[user_id] = "group"
            await query.message.edit_text(
                "<blockquote><b>✅ MODE SET: QUALITY FLOW</b></blockquote>"
            )
        elif data == "set_mode_per_ep":
            user_settings[user_id] = "per_ep"
            await query.message.edit_text(
                "<blockquote><b>✅ MODE SET: EPISODE FLOW</b></blockquote>"
            )

    @app.on_callback_query(filters.regex(r'^(send_sequence|cancel_sequence)$'))
    async def sequence_control_callback(client, query):
        data = query.data
        user_id = query.from_user.id
        
        if data == "send_sequence":
            if user_id in user_sequences:
                await send_sequence_files(client, query.message, user_id)
        elif data == "cancel_sequence":
            user_sequences.pop(user_id, None)
            await query.message.edit_text("<blockquote>Sequence cancelled.</blockquote>")

    @app.on_callback_query(filters.regex(r'^mode_(file|caption)$|^close_mode$'))
    async def mode_callback_handler(client, query):
        """Handle mode switching callbacks"""
        data = query.data
        user_id = query.from_user.id
        
        if data == "mode_file":
            set_user_mode(user_id, "file")
            buttons = InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ File mode", callback_data="mode_file")],
                [InlineKeyboardButton("Caption mode", callback_data="mode_caption")],
                [InlineKeyboardButton("Close", callback_data="close_mode")]
            ])
            text = """<b>🔄 Sequence Mode Settings</b>

<blockquote><b>Current Mode:</b> File mode

<b>File mode:</b> Sequence files using filename
<b>Caption mode:</b> Sequence files using file caption

✅ <i>Mode switched to File mode!</i></blockquote>"""
            
            await query.message.edit_text(text, reply_markup=buttons)
            await query.answer("Switched to File mode!", show_alert=True)
            
        elif data == "mode_caption":
            set_user_mode(user_id, "caption")
            buttons = InlineKeyboardMarkup([
                [InlineKeyboardButton("File mode", callback_data="mode_file")],
                [InlineKeyboardButton("✅ Caption mode", callback_data="mode_caption")],
                [InlineKeyboardButton("Close", callback_data="close_mode")]
            ])
            text = """<b>🔄 Sequence Mode Settings</b>

<blockquote><b>Current Mode:</b> Caption mode

<b>File mode:</b> Sequence files using filename
<b>Caption mode:</b> Sequence files using file caption

✅ <i>Mode switched to Caption mode!</i></blockquote>"""
            
            await query.message.edit_text(text, reply_markup=buttons)
            await query.answer("Switched to Caption mode!", show_alert=True)
            
        elif data == "close_mode":
            await query.message.delete()
            await query.answer("Closed mode settings", show_alert=False)

    @app.on_callback_query(filters.regex(r'^ls_(chat|channel|close)_'))
    async def ls_callback_handlers(client, query):
        data = query.data
        user_id = query.from_user.id
        
        # Extract target_user_id from callback data
        try:
            parts = data.split("_")
            action = parts[1]  # chat, channel, or close
            target_user_id = int(parts[2])
        except (IndexError, ValueError):
            await query.answer("Invalid callback data.", show_alert=True)
            return
        
        if user_id != target_user_id:
            await query.answer("This button is not for you!", show_alert=True)
            return
        
        if target_user_id not in user_ls_state:
            await query.answer("Session expired. Please start again with /ls", show_alert=True)
            await query.message.delete()
            return
        
        ls_data = user_ls_state[target_user_id]
        current_mode = ls_data.get("current_mode", "file")
        
        if action == "chat":
            await query.message.edit_text("<blockquote>⏳ Fetching files from channel... Please wait.</blockquote>")
            
            try:
                # Get messages between the two links
                chat_id = ls_data["first_chat"]
                start_msg_id = ls_data["first_msg_id"]
                end_msg_id = ls_data["second_msg_id"]
                
                # Fetch messages
                messages = await get_messages_between(client, chat_id, start_msg_id, end_msg_id)
                
                if not messages:
                    await query.message.edit_text("<blockquote>❌ No files found between the specified links.</blockquote>")
                    return
                
                # Process and sequence files WITH user mode
                sorted_files, used_mode = await sequence_messages(client, messages, ls_data["mode"], target_user_id)
                
                if not sorted_files:
                    if used_mode == "caption":
                        await query.message.edit_text(
                            "<blockquote>❌ No files with captions found in the specified range.</blockquote>\n"
                            "<blockquote>Switch to File mode using /sf or ensure files have captions.</blockquote>"
                        )
                    else:
                        await query.message.edit_text("<blockquote>❌ No valid files found to sequence.</blockquote>")
                    return
                
                mode_text = "File mode" if used_mode == "file" else "Caption mode"
                skipped_count = len(messages) - len(sorted_files) if used_mode == "caption" else 0
                
                # Send files to user's chat
                if skipped_count > 0:
                    await query.message.edit_text(
                        f"<blockquote>📤 Sending {len(sorted_files)} files to chat... (Skipped {skipped_count} files without captions)</blockquote>"
                    )
                else:
                    await query.message.edit_text(f"<blockquote>📤 Sending {len(sorted_files)} files to chat... Please wait.</blockquote>")
                
                for file in sorted_files:
                    try:
                        await client.copy_message(user_id, from_chat_id=file["chat_id"], message_id=file["msg_id"])
                        await asyncio.sleep(0.8)
                    except Exception as e:
                        print(f"Error sending file: {e}")
                        continue
                
                # Update user stats
                update_user_stats(user_id, len(sorted_files), query.from_user.first_name)
                
                if skipped_count > 0:
                    await query.message.edit_text(
                        f"<blockquote><b>✅ Successfully sent {len(sorted_files)} files to your chat!</b></blockquote>\n"
                        f"<blockquote>Mode: {mode_text}</blockquote>\n"
                        f"<blockquote>Note: {skipped_count} files skipped (no captions found)</blockquote>"
                    )
                else:
                    await query.message.edit_text(
                        f"<blockquote><b>✅ Successfully sent {len(sorted_files)} files to your chat!</b></blockquote>\n"
                        f"<blockquote>Mode: {mode_text}</blockquote>"
                    )
                
            except Exception as e:
                print(f"LS Chat error: {e}")
                await query.message.edit_text("<blockquote>❌ An error occurred while processing files. Please try again.</blockquote>")
            
            # Clean up
            if target_user_id in user_ls_state:
                del user_ls_state[target_user_id]
        
        elif action == "channel":
            await query.message.edit_text("<blockquote>⏳ Checking bot permissions in channel... Please wait.</blockquote>")
            
            try:
                # Check if bot is admin in the channel
                chat_id = ls_data["first_chat"]
                
                is_admin = await check_bot_admin(client, chat_id)
                
                if not is_admin:
                    await query.message.edit_text(
                        f"<blockquote><b>❌ Bot is not admin in this channel!</b></blockquote>\n\n"
                        f"<blockquote>To send files back to the channel, the bot must be added as an administrator "
                        f"with permission to post messages.</blockquote>"
                    )
                    return
                
                await query.message.edit_text("<blockquote>✅ Bot is admin! Fetching files from channel... Please wait.</blockquote>")
                
                # Get messages between the two links
                start_msg_id = ls_data["first_msg_id"]
                end_msg_id = ls_data["second_msg_id"]
                
                # Fetch messages
                messages = await get_messages_between(client, chat_id, start_msg_id, end_msg_id)
                
                if not messages:
                    await query.message.edit_text("<blockquote>❌ No files found between the specified links.</blockquote>")
                    return
                
                # Process and sequence files WITH user mode
                sorted_files, used_mode = await sequence_messages(client, messages, ls_data["mode"], target_user_id)
                
                if not sorted_files:
                    if used_mode == "caption":
                        await query.message.edit_text(
                            "<blockquote>❌ No files with captions found in the specified range.</blockquote>\n"
                            "<blockquote>Switch to File mode using /sf or ensure files have captions.</blockquote>"
                        )
                    else:
                        await query.message.edit_text("<blockquote>❌ No valid files found to sequence.</blockquote>")
                    return
                
                mode_text = "File mode" if used_mode == "file" else "Caption mode"
                skipped_count = len(messages) - len(sorted_files) if used_mode == "caption" else 0
                
                # Send files back to channel
                if skipped_count > 0:
                    await query.message.edit_text(
                        f"<blockquote>📤 Sending {len(sorted_files)} files to channel... (Skipped {skipped_count} files without captions)</blockquote>"
                    )
                else:
                    await query.message.edit_text(f"<blockquote>📤 Sending {len(sorted_files)} files to channel... Please wait.</blockquote>")
                
                success_count = 0
                for file in sorted_files:
                    try:
                        await client.copy_message(chat_id, from_chat_id=file["chat_id"], message_id=file["msg_id"])
                        
                        # Wait between sending files
                        await asyncio.sleep(2)
                        
                    except FloodWait as e:
                        print(f"FloodWait triggered. Sleeping for {e.value} seconds")
                        await asyncio.sleep(e.value)
                        
                    except Exception as e:
                        print(f"Error sending file to channel: {e}")
                        continue
                    else:
                        success_count += 1
                
                # Update user stats
                update_user_stats(user_id, success_count, query.from_user.first_name)
                
                if skipped_count > 0:
                    await query.message.edit_text(
                        f"<blockquote><b>✅ Successfully sent {success_count} files back to the channel!</b></blockquote>\n"
                        f"<blockquote>Mode: {mode_text}</blockquote>\n"
                        f"<blockquote>Total files found: {len(messages)}\n"
                        f"Files with captions: {len(sorted_files)}\n"
                        f"Successfully sent: {success_count}\n"
                        f"Skipped (no captions): {skipped_count}</blockquote>"
                    )
                else:
                    await query.message.edit_text(
                        f"<blockquote><b>✅ Successfully sent {success_count} files back to the channel!</b></blockquote>\n"
                        f"<blockquote>Mode: {mode_text}</blockquote>\n"
                        f"<blockquote>Total files found: {len(sorted_files)}\n"
                        f"Successfully sent: {success_count}</blockquote>"
                    )
                
            except Exception as e:
                print(f"LS Channel error: {e}")
                await query.message.edit_text(f"<blockquote>❌ An error occurred: {str(e)[:200]}...</blockquote>")
            
            # Clean up
            if target_user_id in user_ls_state:
                del user_ls_state[target_user_id]
                
        elif action == "close":
            # Handle Close button for LS
            await query.message.delete()
            
            # Clean up
            if target_user_id in user_ls_state:
                del user_ls_state[target_user_id]

    # ----------------------- EXPORT SETUP FUNCTION -----------------------
    print("✅ Sequence handlers registered successfully")

# Export the setup function
__all__ = ['setup_sequence_handlers']
