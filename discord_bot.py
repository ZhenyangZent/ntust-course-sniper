import discord
from discord.ext import commands, tasks
import aiohttp
import asyncio
import json
import os
import aiomqtt
import ssl
import time
import uuid
import sys
from datetime import datetime
from motor.motor_asyncio import AsyncIOMotorClient 
from pymongo import UpdateOne, ReturnDocument
from dotenv import load_dotenv  
load_dotenv()

# 📝 補上這段：從環境變數讀取密鑰與參數
TOKEN = os.getenv("DISCORD_TOKEN")
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("DB_NAME")

MQTT_BROKER = os.getenv("MQTT_BROKER")
# 埠號從環境變數讀出是字串，必須轉成 int，若沒設定則預設為 1883
MQTT_PORT = int(os.getenv("MQTT_PORT", 1883)) 
MQTT_USER = os.getenv("MQTT_USER")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD")

# 📝 修正：統一 Config 頻道，確保 !set 指令能正確傳達給爬蟲端
TOPIC_CONFIG = os.getenv("TOPIC_CONFIG", "ntust/my_bot/config")
TOPIC_LIST = os.getenv("TOPIC_LIST", "ntust/my_bot/monitor_list")
TOPIC_USERS = os.getenv("TOPIC_USERS", "ntust/my_bot/users")
# 📝 修正：與其他服務的 Topic 預設值保持一致
TOPIC_ALERT = os.getenv("TOPIC_ALERT", "ntust/my_bot/alerts")
TOPIC_RESULT = os.getenv("TOPIC_RESULT", "ntust/my_bot/result")
TOPIC_HEARTBEAT = os.getenv("TOPIC_HEARTBEAT", "ntust/my_bot/heartbeat_v5")
TOPIC_EXECUTE = os.getenv("TOPIC_EXECUTE", "ntust/my_bot/execute")

# --- 預設設定 (改由 .env 讀取) ---
DEFAULT_CONFIG = {
    "SEMESTER": os.getenv("SEMESTER", "1151"),
    "CHECK_INTERVAL": float(os.getenv("CHECK_INTERVAL", 6.0)),
    "MAX_CONCURRENT": int(os.getenv("MAX_CONCURRENT", 5)),
    "ENABLE_CRAWLER": os.getenv("ENABLE_CRAWLER", "True").lower() in ["true", "1", "yes"],
    "PEAK_MODE": os.getenv("PEAK_MODE", "True").lower() in ["true", "1", "yes"] # 📝 新增尖峰模式
}

API_URL = "https://querycourse.ntust.edu.tw/QueryCourse/api/courses"
HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Content-Type": "application/json",
    "Referer": "https://querycourse.ntust.edu.tw/"
}
# ========================================================

intents = discord.Intents.default()
intents.message_content = True
intents.members = True 

# 🔥 關閉預設 Help，使用自訂指令
bot = commands.Bot(command_prefix='!', intents=intents, help_command=None)

# 📝 加上全局攔截器：確保資料庫完全就緒後，才允許使用者下達指令
@bot.check
async def globally_block_before_ready(ctx):
    if not bot.is_ready() or col_watching is None:
        try:
            await ctx.send("⏳ 系統正在啟動與連線資料庫中，請稍後再試...")
        except Exception:
            pass
        return False
    return True

# --- MongoDB 初始化 (改由 on_ready 啟動) ---
mongo_client = None
db = None
col_watching = None
col_channels = None
col_config = None
col_lock = None
col_prefs = None  # 📝 新增個人設定資料表

# 全域變數
watching_list = {}
user_channels = {}
user_prefs = {}   # 📝 儲存每個人的 peak_mode 狀態 (預設全開)
current_config = DEFAULT_CONFIG.copy() 
last_crawler_beat = 0 
INSTANCE_ID = str(uuid.uuid4())[:8] # 本次執行的唯一 ID
tasks_started = False # 📝 加上這個鎖，防止斷線重連時重複啟動背景任務

# --- 🔐 雲端鎖定邏輯 ---

async def try_acquire_lock():
    """嘗試獲取雲端鎖，防止重複開啟"""
    now = time.time()
    try:
        # 原子化嘗試：只有當鎖不存在或已超時 (>30s) 時才會被替換
        result = await col_lock.find_one_and_update(
            {
                "_id": "global_bot_lock",
                "$or": [
                    {"holder": {"$exists": False}},
                    {"heartbeat": {"$lt": now - 30}}
                ]
            },
            {"$set": {
                "holder": INSTANCE_ID,
                "heartbeat": now,
                "start_time": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }},
            upsert=True,
            return_document=ReturnDocument.BEFORE
        )

        # 若 result 為 None，表示文件之前不存在，或是 upsert 建立新文件 -> 我們取得鎖
        if result is None:
            print(f"🔒 已成功獲取雲端鎖 (ID: {INSTANCE_ID})")
            return True

        # 若有舊持有者但心跳已超時 (>30s)，我們也會成功接管
        prev_holder = result.get("holder")
        prev_hb = result.get("heartbeat", 0)
        if now - prev_hb >= 30:
            print(f"🔒 已接管過期鎖 (原持有者: {prev_holder})，本機 ID: {INSTANCE_ID}")
            return True

        # 否則鎖仍在有效期內，由其他實例持有
        print(f"❌ 啟動失敗：Bot ({prev_holder}) 正在運行中！")
        return False
    except Exception as e:
        print(f"❌ 鎖定檢查錯誤: {e}")
        return False

@tasks.loop(seconds=10)
async def maintain_lock_task():
    """每 10 秒更新一次心跳鎖"""
    try:
        await col_lock.update_one(
            {"_id": "global_bot_lock", "holder": INSTANCE_ID},
            {"$set": {"heartbeat": time.time()}}
        )
    except: pass

# --- 💾 核心資料存取 (Async) ---

async def load_config():
    """從 MongoDB 讀取設定，並確保以 .env 為最優先"""
    global current_config
    try:
        doc = await col_config.find_one({"_id": "global_config"})
        if doc:
            for k, v in doc.items():
                if k in current_config:
                    current_config[k] = v
            # 📝 強制以 .env 的最新設定覆蓋資料庫中的舊紀錄
            if os.getenv("SEMESTER"): current_config["SEMESTER"] = os.getenv("SEMESTER")
            if os.getenv("CHECK_INTERVAL"): current_config["CHECK_INTERVAL"] = float(os.getenv("CHECK_INTERVAL"))
            if os.getenv("MAX_CONCURRENT"): current_config["MAX_CONCURRENT"] = int(os.getenv("MAX_CONCURRENT"))
            if os.getenv("ENABLE_CRAWLER"): current_config["ENABLE_CRAWLER"] = os.getenv("ENABLE_CRAWLER").lower() in ["true", "1", "yes"]
            # 📝 加入這行：強制同步 PEAK_MODE
            if os.getenv("PEAK_MODE"): current_config["PEAK_MODE"] = os.getenv("PEAK_MODE").lower() in ["true", "1", "yes"]

            # 將最新的狀態存回 MongoDB
            await col_config.update_one({"_id": "global_config"}, {"$set": current_config}, upsert=True)
            print(f"⚙️ 設定已載入 (並與 .env 同步): {current_config}")
        else:
            await col_config.insert_one({"_id": "global_config", **DEFAULT_CONFIG})
            print("⚙️ 初始化預設設定至資料庫")
    except Exception as e:
        print(f"❌ 讀取設定失敗: {e}")

async def save_config_to_db():
    """將設定存回 MongoDB"""
    try:
        await col_config.update_one(
            {"_id": "global_config"}, 
            {"$set": current_config}, 
            upsert=True
        )
    except Exception as e:
        print(f"❌ 儲存設定失敗: {e}")

async def sync_config_to_mqtt():
    """🔥 將設定推送到 MQTT (Retain=True)"""
    payload = json.dumps(current_config)
    try:
        tls_context = ssl.create_default_context()
        async with aiomqtt.Client(
            hostname=MQTT_BROKER, port=MQTT_PORT,
            username=MQTT_USER, password=MQTT_PASSWORD,
            tls_context=tls_context
        ) as client:
            await client.publish(TOPIC_CONFIG, payload, retain=True, qos=1)
            print(f"📤 設定已同步至 MQTT")
    except Exception as e:
        print(f"❌ MQTT 設定同步失敗: {e}")

async def save_data():
    """儲存監控清單與頻道資料"""
    # 📝 確保資料庫已初始化，避免開機瞬間觸發指令導致 AttributeError
    if col_watching is None or col_channels is None:
        print("⚠️ 資料庫尚未初始化，跳過本次存檔")
        return
    try:
        # 使用 bulk_write 更新（upsert），避免先刪除再寫入所造成的瞬間資料遺失風險
        # 更新 watching_list
        if watching_list:
            ops = [UpdateOne({"_id": c_no}, {"$set": {"subscribers": subs}}, upsert=True) for c_no, subs in watching_list.items()]
            await col_watching.bulk_write(ops)
            # 刪除不再存在的舊文件
            try:
                await col_watching.delete_many({"_id": {"$nin": list(watching_list.keys())}})
            except Exception:
                pass
        else:
            await col_watching.delete_many({})

        # 更新 user_channels
        if user_channels:
            ops = [UpdateOne({"_id": str(uid)}, {"$set": {"channel_id": str(ch_id)}}, upsert=True) for uid, ch_id in user_channels.items()]
            await col_channels.bulk_write(ops)
            try:
                await col_channels.delete_many({"_id": {"$nin": [str(u) for u in user_channels.keys()]}})
            except Exception:
                pass

        # 📝 將個人設定寫入資料庫 (包含 peak 與 fast)
        if user_prefs and col_prefs is not None:
            ops = []
            for uid, prefs in user_prefs.items():
                # 相容性轉換：如果讀到舊版的布林值，轉為新版字典
                if not isinstance(prefs, dict):
                    prefs = {"peak_mode": prefs, "fast_mode": False}
                ops.append(UpdateOne({"_id": str(uid)}, {"$set": {
                    "peak_mode": prefs.get("peak_mode", False), 
                    "fast_mode": prefs.get("fast_mode", False)
                }}, upsert=True))
            try:
                await col_prefs.bulk_write(ops)
            except Exception:
                pass

        print("💾 資料已同步至 MongoDB Cloud")
    except Exception as e:
        print(f"❌ 存檔失敗: {e}")

async def load_data():
    """讀取監控清單與個人設定"""
    global watching_list, user_channels, user_prefs
    watching_list = {}
    user_channels = {}
    user_prefs = {} # 📝 初始化

    try:
        async for doc in col_watching.find({}):
            watching_list[doc["_id"]] = doc["subscribers"]

        async for doc in col_channels.find({}):
            user_channels[int(doc["_id"])] = int(doc["channel_id"])
            
        # 📝 載入個人設定 (升級支援光速模式)
        if col_prefs is not None:
            async for doc in col_prefs.find({}):
                try:
                    user_prefs[int(doc["_id"])] = {
                        "peak_mode": doc.get("peak_mode", False),
                        "fast_mode": doc.get("fast_mode", False)
                    }
                except Exception:
                    pass

        print(f"📂 資料載入：監控 {len(watching_list)} 門課 / {len(user_channels)} 個頻道 / {len(user_prefs)} 份個人設定")
    except Exception as e:
        print(f"❌ 讀檔失敗: {e}")

# --- 📡 MQTT 相關 ---

async def sync_list_to_mqtt():
    course_codes = list(watching_list.keys())
    payload = json.dumps(course_codes)
    try:
        tls_context = ssl.create_default_context()
        async with aiomqtt.Client(
            hostname=MQTT_BROKER, port=MQTT_PORT,
            username=MQTT_USER, password=MQTT_PASSWORD,
            tls_context=tls_context
        ) as client:
            await client.publish(TOPIC_LIST, payload, retain=True, qos=1)
            print(f"📤 監控清單已同步")
    except Exception as e:
        print(f"❌ 清單同步失敗: {e}")

async def sync_discord_members_to_mqtt():
    await bot.wait_until_ready()
    if not bot.guilds: return
    guild = bot.guilds[0]
    
    member_map = {}
    for member in guild.members:
        if not member.bot:
            member_map[member.display_name] = str(member.id)
    
    payload = json.dumps(member_map)
    try:
        tls_context = ssl.create_default_context()
        async with aiomqtt.Client(
            hostname=MQTT_BROKER, port=MQTT_PORT,
            username=MQTT_USER, password=MQTT_PASSWORD,
            tls_context=tls_context
        ) as client:
            await client.publish(TOPIC_USERS, payload, retain=True, qos=1)
    except Exception as e:
        print(f"❌ 成員同步失敗: {e}")

# 📝 加入成員變動監聽器，有人加入或離開時自動重新同步名單
@bot.event
async def on_member_join(member):
    try:
        print(f"👋 新成員 {member.display_name} 加入，更新名單...")
        bot.loop.create_task(sync_discord_members_to_mqtt())
    except Exception as e:
        print(f"❌ on_member_join 處理錯誤: {e}")

@bot.event
async def on_member_remove(member):
    try:
        print(f"👋 成員 {member.display_name} 離開，更新名單...")
        bot.loop.create_task(sync_discord_members_to_mqtt())
    except Exception as e:
        print(f"❌ on_member_remove 處理錯誤: {e}")

async def mqtt_listener_task():
    await bot.wait_until_ready()
    print("🎧 MQTT 監聽服務啟動...")
    reconnect_interval = 5
    while not bot.is_closed():
        try:
            tls_context = ssl.create_default_context()
            async with aiomqtt.Client(
                hostname=MQTT_BROKER, port=MQTT_PORT,
                username=MQTT_USER, password=MQTT_PASSWORD,
                tls_context=tls_context
            ) as client:
                await client.subscribe(TOPIC_ALERT)
                await client.subscribe(TOPIC_RESULT)
                await client.subscribe(TOPIC_HEARTBEAT) 
                
                print("✅ MQTT 連線成功")
                
                async for message in client.messages:
                    topic = message.topic.value
                    try:
                        payload = json.loads(message.payload.decode())
                        
                        if topic == TOPIC_HEARTBEAT:
                            global last_crawler_beat
                            last_crawler_beat = time.time()
                        
                        elif topic == TOPIC_ALERT:
                            # ... (這部分保持原樣，只有提醒釋出名額的) ...
                            data = payload['data']
                            course_no = data['course_no']
                            if course_no in watching_list:
                                subscribers = watching_list[course_no]
                                for sub in subscribers:
                                    channel = bot.get_channel(sub["ch"])
                                    if channel:
                                        embed = discord.Embed(
                                            title=f"🔥 {data['name']} 釋出名額！",
                                            description=f"代碼: `{course_no}`\n系統已嘗試為您執行搶課。\n*(若下方停止按鈕失效，請手動輸入 !remove {course_no})*",
                                            color=0x00ff00
                                        )
                                        embed.add_field(name="人數", value=f"**{data['current']}** / {data['limit']}")
                                        embed.set_footer(text=f"時間: {payload['timestamp']}")
                                        view = StopMonitorView(course_no, sub["user"])
                                        # 這裡本來就有標記 <@{sub['user']}>，不用動
                                        await channel.send(f"<@{sub['user']}>", embed=embed, view=view)
                                    
                                    # 觸發搶課，讀取該使用者的個人雙模式設定
                                    u_prefs = user_prefs.get(sub["user"], {})
                                    if not isinstance(u_prefs, dict):
                                        u_prefs = {"peak_mode": u_prefs, "fast_mode": False}

                                    exec_payload = {
                                        "user_id": sub["user"], 
                                        "course_no": course_no, 
                                        "timestamp": time.time(),  # 📝 補上時間戳記，供 Executor 判斷是否過期
                                        "is_peak": u_prefs.get("peak_mode", False),
                                        "is_fast": u_prefs.get("fast_mode", False)
                                    }
                                    await client.publish(TOPIC_EXECUTE, json.dumps(exec_payload), qos=1)

                                # ... (這是在 mqtt_listener_task 迴圈內)
                        elif topic == TOPIC_RESULT:
                            try:
                                # 1. 解析資料
                                raw_uid = payload["user_id"]
                                c_no = payload["course_no"]
                                success = payload["success"]
                                should_stop = payload.get("should_stop", False)
                                msg_reason = payload.get("msg", "未知原因")

                                print(f"📩 收到結果: 課程 {c_no} | ID: {raw_uid} | 停止: {should_stop}")

                                # 2. 發送通知給該使用者 (Discord)
                                target_uid = raw_uid
                                try: target_uid = int(raw_uid)
                                except: pass

                                if target_uid in user_channels:
                                    ch = bot.get_channel(user_channels[target_uid])
                                    if ch:
                                        color = 0x00ff00 if success else (0x555555 if should_stop else 0xff0000)
                                        title = "🎉 搶課成功！" if success else ("⛔ 停止監控" if should_stop else "❌ 搶課失敗")
                                        
                                        embed = discord.Embed(title=title, color=color)
                                        embed.add_field(name="課程", value=f"`{c_no}`", inline=True)
                                        embed.add_field(name="訊息", value=f"**{msg_reason}**", inline=False)
                                        try: await ch.send(f"<@{target_uid}>", embed=embed)
                                        except: pass

                                # 3. 🔥🔥🔥 溫和移除邏輯 (只刪除該使用者) 🔥🔥🔥
                                if should_stop or success:
                                    if c_no in watching_list:
                                        # 備份原本的人數，用來比對有沒有刪成功
                                        original_count = len(watching_list[c_no])
                                        
                                        # ⚡ 核心邏輯：只過濾掉 ID 相符的人，保留其他人
                                        # 強制轉成 str() 字串比較，避免 123 != "123" 的問題
                                        watching_list[c_no] = [
                                            s for s in watching_list[c_no] 
                                            if str(s["user"]) != str(raw_uid)
                                        ]
                                        
                                        new_count = len(watching_list[c_no])
                                        
                                        if original_count == new_count:
                                            print(f"⚠️ [注意] ID {raw_uid} 似乎不在清單中，無人被移除。")
                                        else:
                                            print(f"✂️ [成功] 已移除使用者 {raw_uid}。")

                                        # 只有當這門課「完全沒人看」的時候，才把課程 Key 刪掉
                                        if not watching_list[c_no]: 
                                            del watching_list[c_no]
                                            print(f"🗑️ 課程 {c_no} 已無人監控，從資料庫移除。")
                                        
                                        # 4. 存檔並同步給爬蟲
                                        await save_data()
                                        asyncio.create_task(sync_list_to_mqtt())
                                    else:
                                        print(f"⚠️ 課程 {c_no} 已經不在監控清單中，略過。")

                            except Exception as e:
                                print(f"❌ 處理 Result 訊息時發生錯誤: {e}")                                    
                    except Exception as e:
                        print(f"訊息處理錯誤: {e}")
        except Exception as e:
            print(f"⚠️ MQTT 斷線重連中: {e}")
            await asyncio.sleep(reconnect_interval)

# --- API 檢查 ---
async def check_course_api_async(session, course_no):
    semester = current_config["SEMESTER"]
    payload = {
        "Semester": semester, "CourseNo": course_no, "CourseName": "", "CourseTeacher": "", 
        "Dimension": "", "ForeignLanguage": 0, "Language": "zh", "OnleyNTUST": 0, 
        "OnlyGeneral": 0, "OnlyIntensive": 0, "OnlyMaster": 0, "OnlyNode": 0, "OnlyUnderGraduate": 0
    }
    try:
        # 📝 機器人新增監控時的查詢也保持輕快，超時設為 5 秒
        async with session.post(API_URL, json=payload, headers=HEADERS, ssl=False, timeout=5) as resp:
            if resp.status == 200:
                data = await resp.json()
                target = next((c for c in data if c["CourseNo"] == course_no), None)
                if target:
                    return {"exists": True, "name": target.get("CourseName", "未知")}
    except: pass
    return {"exists": False}

# --- UI View ---
class LobbyView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None) 

    @discord.ui.button(label="開啟選課助手", style=discord.ButtonStyle.primary, emoji="🚀", custom_id="lobby_create_btn")
    async def create_channel_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True)
        user = interaction.user
        guild = interaction.guild

        if user.id in user_channels:
            ch = guild.get_channel(user_channels[user.id])
            if ch:
                await interaction.followup.send(f"你已經有頻道囉：{ch.mention}", ephemeral=True)
                return
            else:
                del user_channels[user.id]

        overwrites = {
            guild.default_role: discord.PermissionOverwrite(read_messages=False),
            guild.me: discord.PermissionOverwrite(read_messages=True, send_messages=True),
            user: discord.PermissionOverwrite(read_messages=True, send_messages=True)
        }
        
        cat = discord.utils.get(guild.categories, name="🤖 個人選課中心")
        if not cat: cat = await guild.create_category("🤖 個人選課中心")
        
        try:
            ch = await guild.create_text_channel(name=f"🔒{user.name}-選課室", category=cat, overwrites=overwrites)
            user_channels[user.id] = ch.id
            await save_data()
            
            embed = discord.Embed(
                title="👋 歡迎來到你的私人選課室", 
                description="常用指令：", 
                color=0x00aaff
            )
            
            user_cmds = (
                "`!add [代碼]` : 新增監控\n"
                "`!remove [代碼]` : 移除監控\n"
                "`!list` : 清單\n"
                "`!help` : 顯示完整指令\n"
                "`!close` : 關閉頻道"
            )
            embed.add_field(name="👤 使用者指令", value=user_cmds, inline=False)

            if user.guild_permissions.administrator:
                # 🔥 詳細的管理員指令說明
                admin_cmds = (
                    "`!init` : 建立「開啟助手」按鈕 (僅首次需用)\n"
                    "`!set [鍵] [值]` : 修改設定 (如開關/學期)\n"
                    "  └ 例: `!set ENABLE_CRAWLER off`\n"
                    "`!show_config` : 查看目前全域參數\n"
                    "`!status` : 檢查爬蟲心跳與系統狀態"
                )
                embed.add_field(name="🛡️ 管理員指令 (僅你看得見)", value=admin_cmds, inline=False)
                embed.set_footer(text="⚠️ 注意：你是管理員，!set 指令會影響所有人的爬蟲運作。")

            await ch.send(f"<@{user.id}>", embed=embed)
            await interaction.followup.send(f"✅ 已建立：{ch.mention}", ephemeral=True)
            
        except Exception as e:
            await interaction.followup.send(f"建立失敗: {e}", ephemeral=True)

class StopMonitorView(discord.ui.View):
    def __init__(self, course_no, user_id):
        super().__init__(timeout=None)
        self.course_no = course_no
        self.user_id = user_id
    @discord.ui.button(label="停止監控", style=discord.ButtonStyle.danger, emoji="🗑️")
    async def stop_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.user_id: return
        global watching_list
        if self.course_no in watching_list:
            watching_list[self.course_no] = [s for s in watching_list[self.course_no] if s["user"] != self.user_id]
            if not watching_list[self.course_no]: del watching_list[self.course_no]
            await save_data()
            asyncio.create_task(sync_list_to_mqtt())
            button.disabled = True
            await interaction.response.edit_message(view=self)
            await interaction.followup.send(f"已停止 `{self.course_no}`", ephemeral=True)

# --- 🔥 自動更新狀態 ---
@tasks.loop(seconds=5)
async def update_status_task():
    try:
        is_enabled = current_config.get("ENABLE_CRAWLER", True)
        time_diff = time.time() - last_crawler_beat
        crawler_online = time_diff < 15
        
        if not is_enabled:
            status = discord.Status.idle
            text = "💤 系統休息中 | 等待喚醒"
        elif not crawler_online:
            status = discord.Status.dnd
            text = f"⚠️ 爬蟲失聯 ({int(time_diff)}s) | 請檢查"
        else:
            status = discord.Status.online
            course_count = len(watching_list)
            text = f"🚀 監控 {course_count} 門課 | 🟢 爬蟲正常"

        await bot.change_presence(status=status, activity=discord.Game(name=text))
    except Exception as e:
        print(f"狀態更新錯誤: {e}")

# --- 指令 ---

@bot.event
async def on_ready():
    print(f'🤖 Bot {bot.user} 上線 (ID: {INSTANCE_ID})')
    
    # 💡 確保事件迴圈啟動後，才建立 MongoDB 連線
    # 📝 補上 col_prefs
    global mongo_client, db, col_watching, col_channels, col_config, col_lock, col_prefs
    if mongo_client is None:
        try:
            mongo_client = AsyncIOMotorClient(MONGO_URI)
            db = mongo_client[DB_NAME]
            col_watching = db["watching_list"]
            col_channels = db["user_channels"]
            col_config = db["bot_config"]
            col_lock = db["system_lock"]
            col_prefs = db["user_prefs"] # 📝 初始化個人設定表
            print("✅ MongoDB (Motor) 連線設定完成！")
        except Exception as e:
            print(f"❌ MongoDB 連線設定失敗: {e}")

    await load_data()
    await load_config()
    
    # 啟動時先搶鎖，成功才繼續
    is_locked = await try_acquire_lock()
    if not is_locked:
        print("❌ 無法獲取雲端鎖，程式即將關閉...")
        await bot.close()
        return

    # 📝 確保背景任務與 UI 只會被註冊一次
    global tasks_started
    if not tasks_started:
        maintain_lock_task.start()
        bot.add_view(LobbyView())
        bot.loop.create_task(sync_discord_members_to_mqtt()) 
        bot.loop.create_task(mqtt_listener_task())
        bot.loop.create_task(sync_list_to_mqtt())
        bot.loop.create_task(sync_config_to_mqtt())
        update_status_task.start()
        tasks_started = True

@bot.command(name="help")
async def help_command(ctx):
    """顯示系統簡介與指令說明"""
    embed = discord.Embed(
        title="🚀 NTUST Distributed Course Sniper",
        description=(
            "**台科大分散式搶課微服務系統**\n"
            "本系統採用 **微服務架構 (Microservices)**，具備分散式爬蟲負載平衡、"
            "防雙主腦雲端鎖、以及嚴格的執行緒安全防護 (Thread Safety)，"
            "確保在選課尖峰時段能達成 24 小時無人值守的高可用性自動化搶課。\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            "*(參數標示 `[]` 代表必要參數，輸入時不需打括號)*"
        ),
        color=0x00aaff
    )

    user_text = (
        "`!add [課程代碼]`\n"
        "   👉 新增監控 (例: `!add CS101`)\n"
        "`!remove [課程代碼]`\n"
        "   👉 移除監控 (例: `!remove CS101`)\n"
        "`!removeall`\n"
        "   👉 清空你所有的監控\n"
        "`!list`\n"
        "   👉 查看你的監控清單\n"
        "`!status`\n"
        "   👉 查看爬蟲與系統狀態\n"
        "`!close`\n"
        "   👉 關閉並刪除此頻道\n"
        "`!peak on/off`\n"
        "   👉 開啟/關閉你個人的「極限耐心」模式\n"
        "`!fast on/off`\n"
        "   👉 開啟/關閉你個人的「光速搶課模式」 (風險較高)"
    )
    embed.add_field(name="👤 一般使用者指令", value=user_text, inline=False)

    if ctx.author.guild_permissions.administrator:
        admin_text = (
            "`!init`\n"
            "   👉 產生「開啟助手」按鈕 (僅限第一次)\n"
            "`!set [參數] [數值]`\n"
            "   👉 修改全域設定並廣播\n"
            "   🔹 `!set ENABLE_CRAWLER on/off` (開關)\n"
            "   🔹 `!set CHECK_INTERVAL 3` (秒)\n"
            "   🔹 `!set SEMESTER 1151` (學期)\n"
            "`!show_config`\n"
            "   👉 顯示目前全域設定\n"
            "`!reset_all`\n"
            "   👉 💥 清空全系統所有人的監控清單\n"
            "`!sync_users`\n"
            "   👉 🔄 強制同步群組名單給狙擊手端"
        )
        embed.add_field(name="🛡️ 管理員專用", value=admin_text, inline=False)
        
    embed.set_footer(text="Architecture: Discord Bot + MQTT + Crawler + Executor + MongoDB")

    await ctx.send(embed=embed)

@bot.command()
@commands.has_permissions(administrator=True)
async def init(ctx):
    embed = discord.Embed(
        title="🤖 選課小幫手控制台", 
        description="歡迎使用台科大搶課君！\n請點擊下方按鈕建立您的 **個人專屬頻道**。", 
        color=0xf1c40f
    )
    user_cmds = (
        "> **監控指令**\n"
        "`!add [課程代碼]` : 開始監控一門課 (例: `!add CS101`)\n"
        "`!remove [課程代碼]` : 停止監控\n"
        "`!removeall` : 清空所有監控\n"
        "\n"
        "> **一般指令**\n"
        "`!list` : 查看目前的監控清單\n"
        "`!help` : 查看所有指令說明\n"
        "`!status` : 檢查機器人狀態\n"
        "`!close` : 關閉並刪除個人頻道"
    )
    embed.add_field(name="📖 使用者指令一覽", value=user_cmds, inline=False)
    embed.set_footer(text="⚠️ 注意：請先建立頻道後，在您的私人頻道中使用這些指令。")
    await ctx.send(embed=embed, view=LobbyView())

@bot.command()
async def add(ctx, *, course_no: str):
    # 📝 徹底清除所有空白，防止手殘輸入 "CS 101" 導致 API 永久失效
    course_no = course_no.replace(" ", "").strip().upper()
    async with aiohttp.ClientSession() as session:
        result = await check_course_api_async(session, course_no)
    if not result["exists"]:
        await ctx.send(f"⚠️ 找不到 `{course_no}` (學期: {current_config['SEMESTER']})")
        return
    if course_no not in watching_list: watching_list[course_no] = []
    if any(s["user"] == ctx.author.id for s in watching_list[course_no]):
        await ctx.send("已在監控中。")
        return
    
    watching_list[course_no].append({"ch": ctx.channel.id, "user": ctx.author.id})
    await save_data()
    await ctx.send(f"✅ 已監控 **{result['name']}** (`{course_no}`)")
    asyncio.create_task(sync_list_to_mqtt())

@bot.command()
async def remove(ctx, *, course_no: str):
    # 📝 也在移除時清除所有空白以對應儲存格式
    course_no = course_no.replace(" ", "").strip().upper()
    if course_no in watching_list:
        watching_list[course_no] = [s for s in watching_list[course_no] if s["user"] != ctx.author.id]
        if not watching_list[course_no]: del watching_list[course_no]
        await save_data()
        await ctx.send(f"🗑️ 已移除 `{course_no}`")
        asyncio.create_task(sync_list_to_mqtt())
    else: await ctx.send("❌ 沒找到這門課。")

@bot.command()
async def removeall(ctx):
    removed_count = 0
    for c in list(watching_list.keys()):
        orig_len = len(watching_list[c])
        watching_list[c] = [s for s in watching_list[c] if s["user"] != ctx.author.id]
        if len(watching_list[c]) != orig_len: removed_count += 1
        if not watching_list[c]: del watching_list[c]
    
    if removed_count:
        await save_data()
        await ctx.send(f"🗑️ 已移除 {removed_count} 門課")
        asyncio.create_task(sync_list_to_mqtt())
    else: await ctx.send("清單是空的。")


@bot.command()
async def peak(ctx, mode: str):
    """個人專屬的尖峰模式開關"""
    uid = ctx.author.id
    if uid not in user_prefs or not isinstance(user_prefs[uid], dict):
        user_prefs[uid] = {"peak_mode": False, "fast_mode": False}

    if mode.lower() in ["on", "true", "1", "yes"]:
        user_prefs[uid]["peak_mode"] = True
        await save_data()
        await ctx.send("✅ 已為您 **開啟**「死亡尖峰模式」(極限耐心 30分鐘)。\n*(建議：早上剛開戰、網頁瘋狂轉圈圈時使用)*")
    elif mode.lower() in ["off", "false", "0", "no"]:
        user_prefs[uid]["peak_mode"] = False
        await save_data()
        await ctx.send("✅ 已為您 **關閉**「死亡尖峰模式」(靈敏重整 15秒)。\n*(建議：伺服器順暢、想要光速重整時使用)*")
    else:
        await ctx.send("❌ 格式錯誤，請輸入 `!peak on` 或 `!peak off`")


@bot.command()
async def fast(ctx, mode: str):
    """個人專屬的光速搶課開關"""
    uid = ctx.author.id
    if uid not in user_prefs or not isinstance(user_prefs[uid], dict):
        user_prefs[uid] = {"peak_mode": False, "fast_mode": False}

    if mode.lower() in ["on", "true", "1", "yes"]:
        user_prefs[uid]["fast_mode"] = True
        await save_data()
        await ctx.send("⚡ 已為您 **開啟**「光速搶課模式」(無延遲)。\n*(⚠️ 警告：若被學校系統偵測封鎖 24 小時，請立即關閉此模式！)*")
    elif mode.lower() in ["off", "false", "0", "no"]:
        user_prefs[uid]["fast_mode"] = False
        await save_data()
        await ctx.send("🛡️ 已為您 **關閉**「光速搶課模式」(恢復安全擬人化)。\n*(目前使用正常人類手速發送請求)*")
    else:
        await ctx.send("❌ 格式錯誤，請輸入 `!fast on` 或 `!fast off`")

@bot.command(name="list")
async def list_courses(ctx):
    my = [c for c, subs in watching_list.items() if any(s["user"] == ctx.author.id for s in subs)]
    if my: await ctx.send(f"📋 你的清單：\n" + "\n".join([f"- `{c}`" for c in my]))
    else: await ctx.send("📭 清單是空的")

@bot.command()
async def close(ctx):
    uid = ctx.author.id
    if uid not in user_channels or user_channels[uid] != ctx.channel.id:
        await ctx.send("❌ 請在你的選課頻道使用")
        return
    await ctx.send("👋 3秒後關閉...")
    for c in list(watching_list.keys()):
        watching_list[c] = [s for s in watching_list[c] if s["user"] != uid]
        if not watching_list[c]: del watching_list[c]
    del user_channels[uid]
    await save_data()
    asyncio.create_task(sync_list_to_mqtt())
    await asyncio.sleep(3)
    try: await ctx.channel.delete()
    except: pass

@bot.command()
@commands.has_permissions(administrator=True)
async def set(ctx, key: str, value: str):
    """更改全域設定並廣播狀態"""
    key = key.upper()
    if key not in current_config:
        await ctx.send(f"❌ 無效的鍵。可用: {list(current_config.keys())}")
        return

    val_converted = value
    try:
        if key in ["ENABLE_CRAWLER", "PEAK_MODE"]: # 📝 加上 PEAK_MODE
            if value.lower() in ["true", "on", "yes", "1", "open"]: val_converted = True
            elif value.lower() in ["false", "off", "no", "0", "close"]: val_converted = False
            else:
                await ctx.send("❌ 請輸入 on 或 off")
                return
        elif key in ["CHECK_INTERVAL", "MAX_CONCURRENT"]:
            val_converted = float(value)
            if key == "MAX_CONCURRENT":
                val_converted = int(val_converted)
                if not (1 <= val_converted <= 50):
                    await ctx.send("❌ 並發數必須介於 1 到 50 之間")
                    return
            if key == "CHECK_INTERVAL":
                # 必須為正數，且不要太小以免造成分散式錯誤
                if val_converted <= 0:
                    await ctx.send("❌ 檢查間隔必須大於 0 秒")
                    return
                if val_converted < 0.5:
                    await ctx.send("❌ 檢查間隔太小，請設定至少 0.5 秒")
                    return
    except ValueError:
        await ctx.send("❌ 數值格式錯誤")
        return

    current_config[key] = val_converted
    await save_config_to_db()
    await sync_config_to_mqtt()
    
    if key == "ENABLE_CRAWLER":
        if val_converted:
            status_title = "🟢 系統公告：服務已啟動"
            status_desc = "搶課小幫手目前 **正常運作中** 🚀\n若有空位將會立即通知。"
            color = 0x00ff00
        else:
            status_title = "💤 系統公告：服務已暫停"
            status_desc = "搶課小幫手目前 **休息中** (或非選課時段)。\n請不用擔心，待系統開啟後會自動恢復監控。"
            color = 0x555555

        embed = discord.Embed(title=status_title, description=status_desc, color=color)
        embed.set_footer(text=f"公告時間: {datetime.now().strftime('%H:%M')}")
        count = 0
        for uid, ch_id in user_channels.items():
            ch = bot.get_channel(ch_id)
            if ch:
                try:
                    await ch.send(embed=embed) 
                    count += 1
                except: pass
        await ctx.send(f"✅ 設定已更新，並已廣播通知 {count} 位使用者。")
    else:
        await ctx.send(f"⚙️ 設定已更新: `{key}` -> `{val_converted}`")

@bot.command()
@commands.has_permissions(administrator=True)
async def show_config(ctx):
    embed = discord.Embed(title="⚙️ 目前全域設定", color=0x3498db)
    for k, v in current_config.items():
        embed.add_field(name=k, value=f"`{v}`", inline=True)
    await ctx.send(embed=embed)


@bot.command()
@commands.has_permissions(administrator=True)
async def reset_all(ctx):
    """⚠️ 危險指令：清空系統內所有人的監控清單"""
    global watching_list
    course_count = len(watching_list)
    watching_list.clear()
    if col_watching is not None:
        try:
            await col_watching.delete_many({})
        except Exception as e:
            print(f"❌ 清空資料庫失敗: {e}")
    asyncio.create_task(sync_list_to_mqtt())
    embed = discord.Embed(
        title="💥 系統重置完畢",
        description=f"管理員已強制清空所有監控紀錄！\n共清除了 `{course_count}` 門課程的監控。",
        color=0xff0000
    )
    await ctx.send(embed=embed)


@bot.command()
@commands.has_permissions(administrator=True)
async def sync_users(ctx):
    """手動強制重新抓取 Discord 成員名單給狙擊手"""
    await ctx.send("🔄 正在重新掃描群組成員並同步至 MQTT...")
    try:
        await sync_discord_members_to_mqtt()
        await ctx.send("✅ 成員名單同步完成！狙擊手端已可看見最新名單。")
    except Exception as e:
        await ctx.send(f"❌ 同步失敗: {e}")

@bot.command()
async def status(ctx):
    """查看系統狀態"""
    bot_latency = round(bot.latency * 1000)
    time_diff = time.time() - last_crawler_beat
    
    if time_diff < 15:
        crawler_status = "🟢 線上"
        crawler_desc = f"最後心跳: {int(time_diff)} 秒前"
        color = 0x00ff00
    else:
        crawler_status = "🔴 離線"
        if last_crawler_beat == 0: crawler_desc = "尚未收到訊號"
        else: crawler_desc = f"已失聯 {int(time_diff)} 秒"
        color = 0xff0000

    is_enabled = current_config.get("ENABLE_CRAWLER", True)
    sys_status = "🚀 運作中" if is_enabled else "💤 休息中"

    embed = discord.Embed(title="📊 系統狀態報告", color=color)
    embed.add_field(name="🤖 Bot", value=f"🟢 線上\n延遲: `{bot_latency}ms`", inline=True)
    embed.add_field(name="🕷️ 爬蟲", value=f"{crawler_status}\n{crawler_desc}", inline=True)
    embed.add_field(name="⚙️ 系統開關", value=sys_status, inline=False)
    embed.set_footer(text=f"查詢時間: {datetime.now().strftime('%H:%M:%S')}")
    await ctx.send(embed=embed)

# --- 🔥 Main 執行區 (含 Windows 修正) ---

async def main():
    async with bot:
        await bot.start(TOKEN)

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass