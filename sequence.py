import asyncio
import re
import time
from datetime import datetime
from pyrogram import filters
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from pyrogram.errors import UserNotParticipant, FloodWait, ChatAdminRequired, ChannelPrivate
from config import API_HASH, API_ID, BOT_TOKEN, MONGO_URI, START_PIC, START_MSG, HELP_TXT, COMMAND_TXT, OWNER_ID, FSUB_CHANNEL, FSUB_CHANNEL_2, FSUB_CHANNEL_3

# Import from our split modules
from database import (
    user_sequences, user_notification_msg, update_tasks, 
    user_settings, processing_users, user_ls_state,
    users_collection, update_user_stats, get_user_mode, set_user_mode
)
from start import is_subscribed, setup_start_handlers, set_bot_start_time

# Bot start time for uptime calculation
BOT_START_TIME = time.time()

# --- REFINED PARSING ENGINE ---
def parse_file_info(text):
    """Parse file information from text (either filename or caption)"""
    quality_match = re.search(r'(\d{3,4})[pP]', text)
    quality = int(quality_match.group(1)) if quality_match else 0
    clean_name = re.sub(r'\d{3,4}[pP]', '', text)

    season_match = re.search(r'[sS](?:eason)?\s*(\d+)', clean_name)
    season = int(season_match.group(1)) if season_match else 1
    
    ep_match = re.search(r'[eE](?:p(?:isode)?)?\s*(\d+)', clean_name)
    if ep_match:
        episode = int(ep_match.group(1))
    else:
        nums = re.findall(r'\d+', clean_name)
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
            
            # Get the chat_id part (it's 1234567890 in the example)
            chat_id_str = parts[4]
            
            # Check if it needs the -100 prefix
            if chat_id_str.startswith("-100"):
                chat_id = int(chat_id_str)
            elif chat_id_str.startswith("100"):
                # Some links might have 100xxxxxx format
                chat_id = int("-" + chat_id_str)
            else:
                # Regular negative ID for private channels
                chat_id = int("-100" + chat_id_str)
            
            message_id = int(parts[5])
            return chat_id, message_id
            
        elif "t.me/" in link:
            # Public channel/group link format: https://t.me/username/123
            parts = link.split("/")
            username = parts[3]
            message_id = int(parts[4])
            return username, message_id
            
    except Exception as e:
        print(f"Error parsing link {link}: {e}")
        import traceback
        traceback.print_exc()
        
    return None, None

# --- UPDATED: Check if bot is admin in chat ---
async def check_bot_admin(client, chat_id):
    """Check if bot is admin in the given chat/channel"""
    try:
        print(f"Checking admin status for chat_id: {chat_id}, type: {type(chat_id)}")
        
        # If chat_id is a username string, get the actual chat ID
        if isinstance(chat_id, str):
            try:
                chat = await client.get_chat(chat_id)
                chat_id = chat.id
            except Exception as e:
                print(f"Error getting chat from username {chat_id}: {e}")
                return False
        
        # Try to get chat info first
        try:
            chat = await client.get_chat(chat_id)
            print(f"Chat title: {chat.title}, Chat type: {chat.type}")
        except Exception as e:
            print(f"Error getting chat info: {e}")
        
        # Get bot's member status
        try:
            bot_member = await client.get_chat_member(chat_id, "me")
            print(f"Bot status: {bot_member.status}, Status type: {type(bot_member.status)}")
            
            # Check all possible admin status strings
            admin_statuses = [
                "administrator", 
                "creator",
                "Administrator",
                "Creator",
                "admin",
                "Admin",
                "chat_member_status_administrator",
                "chat_member_status_creator"
            ]
            
            # Also check if status is an object with attributes
            status_str = str(bot_member.status).lower()
            print(f"Status string: {status_str}")
            
            is_admin = False
            for admin_status in admin_statuses:
                if admin_status.lower() in status_str:
                    is_admin = True
                    break
            
            print(f"Is admin: {is_admin}")
            return is_admin
            
        except (ChatAdminRequired, ChannelPrivate) as e:
            print(f"Admin check failed (ChatAdminRequired/ChannelPrivate): {e}")
            return False
        except Exception as e:
            print(f"Admin check error: {e}")
            import traceback
            traceback.print_exc()
            return False
            
    except Exception as e:
        print(f"General error in check_bot_admin: {e}")
        import traceback
        traceback.print_exc()
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

# ----------------------- NEW: /sf COMMAND -----------------------
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

# ----------------------- MODE CALLBACK HANDLER -----------------------
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

# ----------------------- SEQUENCE COMMANDS -----------------------
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

# ----------------------- UPDATED: /ls COMMAND -----------------------
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

# --- Handle Telegram links for LS mode ---
async def handle_ls_links(client, message):
    """Handle Telegram links for LS mode"""
    user_id = message.from_user.id
    
    if user_id not in user_ls_state:
        return  # Not in LS mode
    
    ls_data = user_ls_state[user_id]
    link = message.text.strip()
    
    print(f"Received LS link: {link}, Step: {ls_data['step']}, Mode: {ls_data.get('current_mode', 'file')}")
    
    try:
        if ls_data["step"] == 1:
            # First link
            chat_info, msg_id = extract_message_info(link)
            
            print(f"Extracted first link - Chat info: {chat_info}, Msg ID: {msg_id}")
            
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
            
            print(f"Extracted second link - Chat info: {second_chat}, Msg ID: {second_msg_id}")
            
            if not second_msg_id:
                await message.reply_text("<blockquote>❌ Invalid link format. Please send a valid Telegram message link.</blockquote>")
                return
            
            # Check if both links are from same chat
            print(f"Comparing: First chat: {ls_data['first_chat']} (type: {type(ls_data['first_chat'])}), "
                  f"Second chat: {second_chat} (type: {type(second_chat)})")
            
            # Convert both to same type for comparison
            first_chat = ls_data["first_chat"]
            if isinstance(first_chat, int) and isinstance(second_chat, str):
                # Try to resolve the string to ID for comparison
                try:
                    chat_obj = await client.get_chat(second_chat)
                    second_chat = chat_obj.id
                except:
                    pass
            elif isinstance(first_chat, str) and isinstance(second_chat, int):
                # Try to resolve the int to username for comparison
                try:
                    chat_obj = await client.get_chat(second_chat)
                    if chat_obj.username:
                        second_chat = chat_obj.username
                except:
                    pass
            
            if first_chat != second_chat:
                await message.reply_text("<blockquote>❌ Both links must be from the same channel/group.</blockquote>")
                # Reset LS state
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
        import traceback
        traceback.print_exc()
        await message.reply_text("<blockquote>❌ An error occurred. Please try again with valid links.</blockquote>")
        if user_id in user_ls_state:
            del user_ls_state[user_id]

# ----------------------- SORTING ENGINE -----------------------
async def send_sequence_files(client, message, user_id):
    if user_id not in user_sequences or not user_sequences[user_id]:
        await message.edit_text("<blockquote>Nᴏ ғɪʟᴇs ɪɴ sᴇǫᴜᴇɴᴄᴇ!</blockquote>")
        return

    files_data = user_sequences[user_id]
    mode = user_settings.get(user_id, "per_ep")
    await message.edit_text("<blockquote>📤 sᴇɴᴅɪɴɢ ғɪʟᴇs... ᴘʟᴇᴀsᴇ ᴡᴀɪᴛ.</blockquote>")

    if mode == "per_ep":
        sorted_files = sorted(files_data, key=lambda x: (x["info"]["season"], x["info"]["episode"], x["info"]["quality"]))
    else:
        sorted_files = sorted(files_data, key=lambda x: (x["info"]["season"], x["info"]["quality"], x["info"]["episode"]))

    for file in sorted_files:
        try:
            await client.copy_message(message.chat.id, from_chat_id=file["chat_id"], message_id=file["msg_id"])
            await asyncio.sleep(0.8) 
        except: continue

    update_user_stats(user_id, len(files_data), message.from_user.first_name)
    
    try: await message.delete()
    except: pass
    user_sequences.pop(user_id, None)
    user_notification_msg.pop(user_id, None)
    await client.send_message(message.chat.id, "<blockquote><b>✅ ᴀʟʟ ғɪʟᴇs sᴇǫᴜᴇɴᴄᴇᴅ ꜱᴜᴄᴄᴇꜱꜰᴜʟʟʏ!</b></blockquote>")

async def start_sequence(client, message):
    if not await is_subscribed(client, message):
        return
        
    user_id = message.from_user.id
    
