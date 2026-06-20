import asyncio
import aiohttp
import json
import aiomqtt
import ssl
import time
import uuid
import random
from datetime import datetime
import os
from dotenv import load_dotenv
load_dotenv()

# 📝 補上這段：從環境變數讀取 MQTT 連線資訊與 Topic
MQTT_BROKER = os.getenv("MQTT_BROKER")
MQTT_PORT = int(os.getenv("MQTT_PORT", 1883))  # 轉成 int
MQTT_USER = os.getenv("MQTT_USER")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD")

TOPIC_CONFIG = os.getenv("TOPIC_CONFIG", "ntust/my_bot/config")
TOPIC_LIST = os.getenv("TOPIC_LIST", "ntust/my_bot/monitor_list")
TOPIC_ALERT = os.getenv("TOPIC_ALERT", "ntust/my_bot/alerts")
TOPIC_RESULT = os.getenv("TOPIC_RESULT", "ntust/my_bot/result")
TOPIC_HEARTBEAT = os.getenv("TOPIC_HEARTBEAT", "ntust/my_bot/heartbeat_v5")
TOPIC_EXECUTE = os.getenv("TOPIC_EXECUTE", "ntust/my_bot/execute")

# --- 🔥 全域設定 ---
GLOBAL_CONFIG = {
    "SEMESTER": os.getenv("SEMESTER", "1151"),
    "CHECK_INTERVAL": float(os.getenv("CHECK_INTERVAL", 6.0)),   # 單機間隔 (兩台就會變成 3秒 check 一次)
    "MAX_CONCURRENT": int(os.getenv("MAX_CONCURRENT", 5)),
    "ENABLE_CRAWLER": os.getenv("ENABLE_CRAWLER", "True").lower() in ["true", "1", "yes"]   # 🔥 總開關
}

API_URL = "https://querycourse.ntust.edu.tw/QueryCourse/api/courses"
HEADERS = { "User-Agent": "Mozilla/5.0", "Content-Type": "application/json", "Referer": "https://querycourse.ntust.edu.tw/" }
# ====================================================

current_targets = []

class ClusterManager:
    def __init__(self, client: aiomqtt.Client, unique_id: str):
        self.client = client
        self.my_id = unique_id
        self.peers = {}    
        self.timeout = 10  # 超過 10秒沒心跳就視為離線
        self.hb_task = None  # 🔥 紀錄心跳 Task

    async def start(self):
        print(f"🤖 本機 ID: {self.my_id} | 啟動叢集協作...")
        # 錯開啟動時間，避免同時發心跳
        await asyncio.sleep(random.uniform(0.5, 2.0))
        # 🔥 儲存心跳 task，方便未來停止
        self.hb_task = asyncio.create_task(self._heartbeat_loop())

    async def stop(self):
        # 🔥 新增停止方法，斷線時可以用來殺死心跳任務
        if self.hb_task and not self.hb_task.done():
            self.hb_task.cancel()
            try:
                await self.hb_task
            except asyncio.CancelledError:
                pass

    async def _heartbeat_loop(self):
        """ 持續發送心跳，證明自己還活著 """
        while True:
            try:
                # 即使爬蟲暫停，心跳也不能停，否則分工計算會錯
                payload = json.dumps({"id": self.my_id, "ts": time.time(), "status": GLOBAL_CONFIG["ENABLE_CRAWLER"]})
                await self.client.publish(TOPIC_HEARTBEAT, payload, qos=1)
            except Exception as e:
                print(f"⚠️ 心跳失敗: {e}")
            await asyncio.sleep(2) # 每 2 秒發一次心跳

    def update_peer(self, peer_id):
        self.peers[peer_id] = time.time()

    def get_status(self):
        """ 計算現在有幾台機器，以及我的順位 """
        now = time.time()
        # 篩選出最近 10 秒有心跳的機器
        active_ids = [uid for uid, ts in self.peers.items() if now - ts < self.timeout]
        
        # 確保自己也在清單內
        if self.my_id not in active_ids: 
            active_ids.append(self.my_id)
            
        active_ids.sort() # 排序以確保大家的順序認知一致
        
        try: 
            return len(active_ids), active_ids.index(self.my_id), active_ids
        except: 
            return 1, 0, [self.my_id]

async def check_course(sem, session, course_no):
    """ 查詢課程 API """
    semester_val = GLOBAL_CONFIG["SEMESTER"]
    async with sem:
        payload = {
            "Semester": semester_val, "CourseNo": course_no, "CourseName": "", "CourseTeacher": "", 
            "Dimension": "", "ForeignLanguage": 0, "Language": "zh", "OnleyNTUST": 0, 
            "OnlyGeneral": 0, "OnlyIntensive": 0, "OnlyMaster": 0, "OnlyNode": 0, "OnlyUnderGraduate": 0
        }
        try:
            # 📝 查詢伺服器獨立且穩定，採用 Fail-Fast 策略，超時設為 5 秒即可
            async with session.post(API_URL, json=payload, headers=HEADERS, ssl=False, timeout=5) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    target = next((c for c in data if c["CourseNo"] == course_no), None)
                    if target:
                        return {
                            "course_no": course_no, "exists": True,
                            "name": target.get("CourseName", "未知"),
                            "teacher": target.get("CourseTeacher", ""),
                            # 📝 若 API 回傳空字串或 null，使用 `or 0` 保險處理，避免 ValueError
                            "current": int(target.get("ChooseStudent", 0) or 0),
                            "limit": int(target.get("Restrict2", 0) or 0) 
                        }
                else:
                    print(f"⚠️ API 狀態異常: HTTP {resp.status} (課程: {course_no})")
        except asyncio.TimeoutError:
            print(f"⚠️ API 請求超時 (Timeout)，伺服器無回應 (課程: {course_no})")
        except Exception as e: 
            # 📝 使用 repr() 可以確保印出完整的錯誤型別(例如 ClientConnectorError)
            print(f"❌ API 請求失敗 (可能被擋 IP 或網路不穩): {repr(e)[:200]}")
        return None

async def monitor_loop(client: aiomqtt.Client, cluster: ClusterManager):
    connector = aiohttp.TCPConnector(limit=20, ssl=False)
    
    current_limit = int(GLOBAL_CONFIG.get("MAX_CONCURRENT", 5))
    sem = asyncio.Semaphore(current_limit)
    
    print("🔌 爬蟲核心就緒...")
    
    # 📝 紀錄推播冷卻時間，避免洗版 (CourseNo: Timestamp)
    alert_cooldowns = {}

    async with aiohttp.ClientSession(connector=connector) as session:
        while True:
            # 1. 🔥 檢查總開關
            if not GLOBAL_CONFIG.get("ENABLE_CRAWLER", True):
                print(f"💤 [{cluster.my_id}] 系統暫停中 (等待指令開啟)...")
                await asyncio.sleep(3)
                continue

            if not current_targets:
                print("😴 監控清單為空，待機中...")
                await asyncio.sleep(3)
                continue

            # 2. 更新並發數
            new_limit = int(GLOBAL_CONFIG.get("MAX_CONCURRENT", 5))
            if new_limit != current_limit:
                current_limit = new_limit
                sem = asyncio.Semaphore(new_limit)

            # 3. 🔥🔥🔥 關鍵：分散式間隔計算 🔥🔥🔥
            interval = float(GLOBAL_CONFIG.get("CHECK_INTERVAL", 6.0))
            
            # 取得叢集狀態
            total_workers, my_index, _ = cluster.get_status()
            
            # 計算每台機器應該錯開的時間 (Slice)
            # 例如: 間隔6秒, 2台機器 => offset 3秒
            time_per_slice = interval / total_workers
            my_offset = time_per_slice * my_index
            
            now = time.time()
            # 找出下一個「絕對時間整點」
            # 例如 interval=6, now=12.5 => base=12.0
            base_time = (now // interval) * interval
            
            # 加上我的偏移量
            target_time = base_time + my_offset
            
            # 如果計算出的時間已經過了，就排到下一個週期
            if target_time <= now + 0.1:
                target_time += interval
            
            wait_seconds = target_time - now
            
            # LOG 顯示分工狀態 (讓你確定它有在運作)
            print(f"🕒 [分工: {my_index+1}/{total_workers}] 間隔:{interval}s | 錯開:{my_offset:.1f}s | 等待:{wait_seconds:.2f}s")
            
            await asyncio.sleep(wait_seconds)

            # --- 執行掃描 ---
            active_targets = list(current_targets)
            if not active_targets: continue

            print(f"🚀 掃描 (x{len(active_targets)}) - 學期 {GLOBAL_CONFIG.get('SEMESTER')}")
            
            tasks = [check_course(sem, session, code) for code in active_targets]
            results = await asyncio.gather(*tasks)

            for res in results:
                if res and res["exists"]:
                    # 如果這門課已經不在清單就不處理
                    if res['course_no'] not in current_targets: continue

                    if res["current"] < res["limit"]:
                        print(f"🔥 {res['name']} 釋出名額！ ({res['current']}/{res['limit']})")

                        # 推播冷卻：每門課至少 60 秒才會再推播一次
                        now_ts = time.time()
                        last_ts = alert_cooldowns.get(res['course_no'], 0)
                        if now_ts - last_ts >= 60:
                            msg = {
                                "type": "vacancy_found",
                                "data": res,
                                "timestamp": datetime.now().strftime('%H:%M:%S')
                            }
                            await client.publish(TOPIC_ALERT, json.dumps(msg), qos=1)
                            alert_cooldowns[res['course_no']] = now_ts # 更新最後推播時間
                        else:
                            print(f"⏳ {res['course_no']} 仍在冷卻中，暫不推播...")
            
            # 掃描完成，直接進入下一次迴圈 (會重新計算 wait_seconds)
            
            # 🧹 定期清理過期的推播冷卻紀錄 (清除 5 分鐘前遺留的紀錄)
            try:
                now_ts = time.time()
                expired_alerts = [k for k, ts in list(alert_cooldowns.items()) if now_ts - ts > 300]
                for k in expired_alerts:
                    del alert_cooldowns[k]
            except Exception:
                pass

async def global_message_router(client: aiomqtt.Client, cluster: ClusterManager):
    """ 負責接收指令並更新變數 """
    await client.subscribe(TOPIC_LIST)
    await client.subscribe(TOPIC_HEARTBEAT)
    await client.subscribe(TOPIC_RESULT)
    await client.subscribe(TOPIC_CONFIG) 
    
    print("🎧 指令接收器啟動...")

    async for message in client.messages:
        topic = message.topic.value
        try:
            payload = message.payload.decode()
            data = json.loads(payload)
            
            if topic == TOPIC_HEARTBEAT:
                cluster.update_peer(data["id"])
            
            elif topic == TOPIC_LIST:
                if isinstance(data, list):
                    global current_targets
                    current_targets = data
                    print(f"📋 清單更新: {len(data)} 筆")

            elif topic == TOPIC_CONFIG:
                # 接收 Discord 來的設定 (包含 ENABLE_CRAWLER)
                print(f"⚙️ 收到設定變更: {data}")
                for key, value in data.items():
                    if key in GLOBAL_CONFIG:
                        GLOBAL_CONFIG[key] = value
                print(f"✅ 設定已套用: {GLOBAL_CONFIG}")

            # TOPIC_RESULT 處理移除邏輯已移到 Executor/Discord Bot，Crawler 只負責通報

        except Exception as e:
            print(f"Router Error: {e}")

async def main():
    print("🚀 系統啟動中...")
    my_unique_id = f"worker-{str(uuid.uuid4())[:4]}"
    
    tls_context = ssl.create_default_context()
    
    # 📝 加上外層的無限重連迴圈，防止網路瞬斷導致程式退出
    while True:
        try:
            async with aiomqtt.Client(
                hostname=MQTT_BROKER, port=MQTT_PORT,
                username=MQTT_USER, password=MQTT_PASSWORD,
                tls_context=tls_context,
                identifier=my_unique_id
            ) as client:
                cluster = ClusterManager(client, unique_id=my_unique_id)
                await cluster.start()

                # 🔥 1. 將 router 和 monitor 建立為明確的 Task
                router_task = asyncio.create_task(global_message_router(client, cluster))
                monitor_task = asyncio.create_task(monitor_loop(client, cluster))

                # 🔥 2. 等待直到有任何一個 Task 拋出例外
                done, pending = await asyncio.wait(
                    [router_task, monitor_task],
                    return_when=asyncio.FIRST_EXCEPTION
                )

                # 🔥 3. 觸發重新連線前，徹底殺死所有還在跑的舊任務
                for task in pending:
                    task.cancel()
                await cluster.stop()
        except aiomqtt.MqttError as e:
            print(f"⚠️ MQTT 連線中斷，5 秒後嘗試重新連線: {e}")
            await asyncio.sleep(5)
        except Exception as e:
            print(f"❌ 發生未預期錯誤，5 秒後重啟: {e}")
            await asyncio.sleep(5)

if __name__ == "__main__":
    import sys
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())     
    try: asyncio.run(main())
    except KeyboardInterrupt: pass