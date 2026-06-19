import asyncio
import json
import time
import aiomqtt
import ssl
import threading
import re
import os
import random
from collections import deque
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException
from webdriver_manager.chrome import ChromeDriverManager
import urllib3

# 🔥 關閉 SSL 不安全連線的警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
# 載入 .env
load_dotenv()

# 📝 從環境變數讀取 MQTT 連線資訊
MQTT_BROKER = os.getenv("MQTT_BROKER")
MQTT_PORT = int(os.getenv("MQTT_PORT", 1883))
MQTT_USER = os.getenv("MQTT_USER")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD")

# 📝 Topic
TOPIC_USERS = os.getenv("TOPIC_USERS", "ntust/my_bot/users")
TOPIC_EXECUTE = os.getenv("TOPIC_EXECUTE", "ntust/my_bot/execute")
TOPIC_RESULT = os.getenv("TOPIC_RESULT", "ntust/my_bot/result")


# 從環境讀取預設帳號（如有）
# 📝 讀取無頭模式設定 (預設為 False 顯示畫面)
HEADLESS_MODE = os.getenv("HEADLESS_MODE", "False").lower() in ["true", "1", "yes"]

# 📝 讀取自動部署參數，繞過 input()
ENV_SNIPER_MODE = os.getenv("SNIPER_MODE")
# 📝 與 .env 變數名稱對齊
ENV_DISCORD_ID = os.getenv("DISCORD_USER_ID")

# 從環境讀取預設帳號（如有）
ENV_STUDENT_ID = os.getenv("SNIPER_STUDENT_ID")
ENV_PASSWORD = os.getenv("SNIPER_PASSWORD")
# 自動組裝成原本程式需要的格式
ACCOUNTS_LIST = []
if ENV_STUDENT_ID and ENV_PASSWORD:
    ACCOUNTS_LIST.append({"id": ENV_STUDENT_ID, "pwd": ENV_PASSWORD})
MAX_ERROR_COUNT = 3
MAX_LOGINS_PER_MINUTE = 3  
LOGIN_COOLDOWN_SEC = 20    

COURSE_MODES = {
    "1": { "name": "加退選課 (B01)", "url": "https://courseselection.ntust.edu.tw/AddAndSub/B01/B01", "input_name": "CourseText", "btn_id": "SingleAdd" },
    "2": { "name": "電腦抽選後選課 (A06)", "url": "https://courseselection.ntust.edu.tw/First/A06/A06", "input_name": "CourseText", "btn_id": "SingleAdd" }
}

# 🛡️ 防手震記錄 (3秒冷卻) - 這是為了保護伺服器，保留全域
course_cooldowns = {} 

# 🔥 [修改 1] 移除全域致命錯誤黑名單，改為個別管理
# fatal_error_blacklist = set() 
# ==========================================

class SniperExecutor:
    def __init__(self, user_id, student_id, password, mode_config):
        self.user_id = str(user_id)
        self.student_id = student_id
        self.password = password
        self.config = mode_config 
        self.target_url = mode_config["url"]
        
        self.driver = None
        self.wait = None
        self.fail_counts = {}
        self.current_aim = None 
        self.driver_lock = threading.Lock()
        self.login_timestamps = deque()

        # 🔥 [修改 2] 在這裡新增個人專屬的黑名單 (隔離錯誤)
        self.blacklisted_courses = set()

    def start_driver(self):
        mode_text = "背景無頭模式" if HEADLESS_MODE else "顯示畫面模式"
        print(f"[{self.student_id}] 🔫 狙擊手就位 ({mode_text})，啟動瀏覽器 ({self.config['name']})...")
        
        chrome_options = Options()
        chrome_options.add_argument("--ignore-certificate-errors")
        # 🛡️ 反偵測機制 1：拔除「我是自動化軟體」的標籤
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        # 🛡️ 反偵測機制 2：隱藏 Chrome 正由自動化軟體控制的提示條
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)

        # 📝 根據設定決定是否開啟無頭模式
        if HEADLESS_MODE:
            chrome_options.add_argument("--headless=new")
            
        # 🚀 伺服器防崩潰參數 (無論是否無頭都建議常駐，防記憶體炸裂)
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--no-sandbox") 
        chrome_options.add_argument("--disable-dev-shm-usage") 
        
        service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=service, options=chrome_options)
        
        # 🛑 應對台科大「死亡半小時」的極限耐心設定
        try:
            # 🛑 應對台科大「伺服器死亡」的極限耐心設定
            # 📝 極端卡頓：最多等 5 分鐘(300秒)，超過代表伺服器已經把連線切斷(504)，必須放手讓程式重整
            self.driver.set_page_load_timeout(300)
        except Exception:
            pass
        self.wait = WebDriverWait(self.driver, 600) # 尋找網頁元件最多等 10 分鐘
        
        self.login()

    def _check_login_rate_limit(self):
        now = time.time()
        while self.login_timestamps and now - self.login_timestamps[0] > 60:
            self.login_timestamps.popleft()
        if len(self.login_timestamps) >= MAX_LOGINS_PER_MINUTE:
            wait_time = 60 - (now - self.login_timestamps[0])
            if wait_time < 0: wait_time = 1
            print(f"[{self.student_id}] ⛔ 登入頻率過高！強制冷卻 {wait_time:.1f} 秒...")
            time.sleep(wait_time)
            self.login_timestamps.clear()
        self.login_timestamps.append(time.time())

    def login(self):
        with self.driver_lock:
            try:
                self._check_login_rate_limit()
                print(f"[{self.student_id}] 正在登入...")
                self.driver.get("https://courseselection.ntust.edu.tw/")
                
                login_success = False
                for attempt in range(3): 
                    try:
                        username_input = self.wait.until(EC.visibility_of_element_located((By.ID, "Username")))
                        username_input.clear()
                        username_input.send_keys(self.student_id)
                        
                        password_input = self.driver.find_element(By.ID, "Password")
                        password_input.clear()
                        password_input.send_keys(self.password)
                        
                        login_btn = self.driver.find_element(By.ID, "loginButton")
                        # 🛡️ 改為真實滑鼠點擊，避免 isTrusted=false
                        login_btn.click()
                        
                        # 📝 攔截台科大「重複登入，是否強制登出其他裝置」的跳窗
                        try:
                            WebDriverWait(self.driver, 1.5).until(EC.alert_is_present())
                            alert = self.driver.switch_to.alert
                            alert.accept()
                            print(f"[{self.student_id}] 🔔 偵測到重複登入跳窗，已強制登出其他裝置。")
                        except Exception:
                            pass
                        
                        login_success = True
                        break 
                    except StaleElementReferenceException:
                        print(f"[{self.student_id}] ⚠️ 頁面刷新導致元件過期 (Stale)，重試 {attempt+1}...")
                        time.sleep(0.5)
                    except Exception as e:
                        print(f"[{self.student_id}] ❌ 登入操作異常: {e}")
                        return

                if not login_success:
                    print(f"[{self.student_id}] ❌ 登入操作失敗 (重試次數耗盡)")
                    return

                print(f"[{self.student_id}] 導向戰場頁面...")
                time.sleep(1)
                self.driver.get(self.target_url)
                
                # 📝 新增防護：攔截因為「非選課時間」或其他原因跳出的系統跳窗 (Alert)
                try:
                    # 等待最多 2 秒看有沒有跳窗
                    WebDriverWait(self.driver, 2).until(EC.alert_is_present())
                    alert = self.driver.switch_to.alert
                    try:
                        print(f"[{self.student_id}] 🔔 系統提示: {alert.text}")
                    except Exception:
                        pass
                    try:
                        alert.accept()  # 點擊確定關閉跳窗
                    except Exception:
                        try:
                            alert.dismiss()
                        except Exception:
                            pass
                except Exception:
                    pass  # 沒有跳窗就正常繼續

                # 檢查是否仍在登入頁面
                try:
                    current_url = self.driver.current_url
                except Exception:
                    current_url = ""

                if "Login" in current_url or "signin" in current_url:
                    print(f"[{self.student_id}] ❌ 登入失敗 (可能被踢出或非開放時間)")
                    time.sleep(3)
                else:
                    print(f"[{self.student_id}] ✅ 登入完成，狙擊手待命中")

            except Exception as e:
                print(f"[{self.student_id}] ❌ 登入流程異常: {e}")
                time.sleep(3)

    def keep_alive(self):
        # 嘗試非阻塞取得鎖，並用 lock_owned 標記擁有權，避免解鎖盜竊問題
        if self.driver_lock.acquire(blocking=False):
            lock_owned = True
            try:
                # 📝 優先嘗試捕捉並關閉任何阻擋在畫面上的 Alert，防止後續操作引爆崩潰
                try:
                    alert = self.driver.switch_to.alert
                    alert.accept()
                except Exception:
                    pass

                current_url = self.driver.current_url
                if "Login" in current_url or "signin" in current_url or "oidc" in current_url:
                    print(f"[{self.student_id}] 🔄 偵測到登出，執行重登...")
                    # 先釋放鎖並標記為非擁有，讓 login() 可以安全執行
                    try:
                        self.driver_lock.release()
                    except Exception:
                        pass
                    lock_owned = False
                    self.login()
                    return

                target_code = self.target_url.split("/")[-1]
                if target_code not in current_url:
                    self.driver.get(self.target_url)
            except Exception:
                pass
            finally:
                # 只釋放自己擁有的鎖，避免釋放其他執行緒的鎖
                if lock_owned:
                    try:
                        self.driver_lock.release()
                    except Exception:
                        pass

    def prepare_aim(self, course_no):
        # 🔒 必須加上非阻塞鎖，確保沒有其他執行緒正在操作瀏覽器
        if self.driver_lock.acquire(blocking=False):
            try:
                input_box = self.driver.find_element(By.NAME, self.config["input_name"])
                if input_box.get_attribute('value') != course_no:
                    input_box.clear()
                    input_box.send_keys(course_no)
                    self.current_aim = course_no
            except Exception:
                pass
            finally:
                try:
                    self.driver_lock.release()
                except Exception:
                    pass

    # 📝 接收 is_peak 參數，預設為 True
    def execute_add_course(self, course_no, is_peak=False, is_fast=False):
        # 印出目前套用的模式讓伺服器日誌好辨認
        print(f"[{self.student_id}] ⚡ 執行加選: {course_no} (Peak:{is_peak}, Fast:{is_fast})")
        
        # 📝 動態決定極限耐心值：尖峰模式 1800秒(30分鐘)，平時模式 15秒 (快速放棄重試)
        dynamic_timeout = 1800 if is_peak else 15

        # 🔒 加上 timeout，讓系統在瀏覽器忙碌時願意「等 3 秒」，而不是直接丟掉搶課機會
        if not self.driver_lock.acquire(timeout=3.0):
            return False, "⚠️ 瀏覽器正忙於其他操作，等待超時，略過本次指令", False

        lock_owned = True

        try:
            if course_no not in self.fail_counts:
                self.fail_counts[course_no] = 0

            success = False
            result_msg = "監控中..."
            should_stop = False 

            # 🛡️ 確保絕對在正確的戰場頁面上 (動態判斷 A06、B01 或未來的任何模式)
            target_code = self.target_url.split("/")[-1]  # 自動解析目標代號 (例如 "A06")

            if target_code not in self.driver.current_url:
                print(f"[{self.student_id}] ⚠️ 偏離戰場，正在導回: {self.target_url}")
                self.driver.get(self.target_url)
                
                # 📝 加上「無效登出防護」，如果被踢出則立刻重登
                if "Login" in self.driver.current_url or "signin" in self.driver.current_url:
                    try:
                        self.driver_lock.release() # 先解鎖讓 login 可以順利執行
                    except Exception:
                        pass
                    lock_owned = False
                    self.login() 
                    # 重新取得鎖並標記擁有
                    self.driver_lock.acquire(blocking=True)
                    lock_owned = True
                    # 📝 在重新載入網頁前，套用動態的網頁載入超時
                    try: self.driver.set_page_load_timeout(dynamic_timeout)
                    except: pass
                    self.driver.get(self.target_url) # 再次前往戰場
                    # 🚨 檢查是否還在登入頁 (代表重登失敗或被鎖 IP)，直接跳出避免死等 10 分鐘
                    if "Login" in self.driver.current_url or "signin" in self.driver.current_url:
                        return False, "❌ 重登失敗或伺服器無回應，放棄本次搶課", False

            # 🚨 縮短尋找輸入框的等待時間 (從 600 秒縮短為 10 秒)
            short_wait = WebDriverWait(self.driver, 10)
            input_box = short_wait.until(EC.presence_of_element_located((By.NAME, "CourseText")))
            input_box.clear()

            # 🛡️ 模擬人類「複製貼上」：直接一次性送出字串，符合人類真實搶課行為，且速度極快
            input_box.send_keys(course_no)

            add_btn = self.driver.find_element(By.ID, "SingleAdd")
            # 🛡️ 電競級擬人反應時間：如果不開啟 is_fast，給予 0.05 ~ 0.15 秒的微小亂數延遲
            if not is_fast:
                time.sleep(random.uniform(0.05, 0.15))
            add_btn.click()
            
            alert_text = None
            try:
                # 📝 套用動態等待時間！
                # 尖峰時給伺服器 5 分鐘處理；平時若 15 秒沒反應就果斷報錯，觸發重整重試！
                WebDriverWait(self.driver, dynamic_timeout).until(EC.alert_is_present())
                alert = self.driver.switch_to.alert
                alert_text = alert.text
                alert.accept() 
                print(f"[{self.student_id}] 🔔 Alert: {alert_text}")

                if "成功" in alert_text or "已在" in alert_text:
                    success = True
                    should_stop = True
                    result_msg = f"🎉 {alert_text}"
                    return True, result_msg, True

                if any(x in alert_text for x in ["衝堂", "衝突", "不符合", "擋修"]):
                    print(f"[{self.student_id}] ⛔ {course_no} 致命錯誤！停止監控。")
                    return False, f"⛔ 停止: {alert_text}", True

                self.fail_counts[course_no] += 1
                current_fail = self.fail_counts[course_no]
                
                print(f"[{self.student_id}] ⚠️ {course_no} 失敗 ({current_fail}/{MAX_ERROR_COUNT}): {alert_text}")

                if current_fail >= MAX_ERROR_COUNT:
                    should_stop = True
                    result_msg = f"❌ 已失敗 {MAX_ERROR_COUNT} 次，自動放棄: {alert_text}"
                else:
                    result_msg = f"⚠️ 失敗({current_fail}/{MAX_ERROR_COUNT}): {alert_text}"
                    should_stop = False 

            except:
                pass
            
            if should_stop:
                return success, result_msg, True

            try:
                # 📝 修正：避免使用 600 秒的 self.wait 導致長時間卡死
                short_refresh_wait = WebDriverWait(self.driver, 3)
                short_refresh_wait.until(EC.staleness_of(add_btn)) 
                short_refresh_wait.until(EC.presence_of_element_located((By.ID, "SingleAdd")))
            except: pass 

            if not success:
                xpath_query = f"//table[@id='cartTable']//tr/td[1][contains(text(), '{course_no}')]"
                if len(self.driver.find_elements(By.XPATH, xpath_query)) > 0:
                    success = True
                    should_stop = True
                    result_msg = "🎉 檢查表格發現已選上！"

        except Exception as e:
            print(f"[{self.student_id}] ❌ 執行異常: {e}")
            self.fail_counts[course_no] += 1
            if self.fail_counts[course_no] >= MAX_ERROR_COUNT:
                should_stop = True
                result_msg = f"❌ 程式異常超過 {MAX_ERROR_COUNT} 次，停止。"
            else:
                result_msg = f"異常: {str(e)[:20]}"

        finally:
            # 📝 只有在自己確實擁有鎖的情況下才解開
            try:
                if lock_owned:
                    try:
                        self.driver_lock.release()
                    except Exception:
                        pass
            except UnboundLocalError:
                # 若 lock_owned 未被定義（理論上不會），安全忽略
                pass

        return success, result_msg, should_stop

    # 📝 加入資源清理機制
    def cleanup(self):
        print(f"[{self.student_id}] 🧹 正在關閉瀏覽器並清理資源...")
        try:
            if self.driver:
                try:
                    self.driver.quit()
                except Exception:
                    pass
                self.driver = None
        except Exception:
            pass

async def fetch_discord_users_from_mqtt():
    print("📡 正在從 MQTT 下載 Discord 成員名單...")
    try:
        tls_context = ssl.create_default_context()
        async with aiomqtt.Client(
            hostname=MQTT_BROKER, port=MQTT_PORT,
            username=MQTT_USER, password=MQTT_PASSWORD,
            tls_context=tls_context
        ) as client:
            await client.subscribe(TOPIC_USERS)

            # 📝 使用 asyncio.wait_for 才能真正達到「等待 5 秒後放棄」的效果
            async def get_msg():
                async for message in client.messages:
                    return json.loads(message.payload.decode())

            try:
                payload = await asyncio.wait_for(get_msg(), timeout=5.0)
                print(f"✅ 成功載入 {len(payload)} 位成員！")
                return payload
            except asyncio.TimeoutError:
                print("⚠️ 等待名單超時 (5秒)，Discord Bot 可能尚未發佈資料，將使用手動輸入模式。")
                return {}
    except Exception as e:
        print(f"⚠️ 無法取得名單 ({e})，將使用手動輸入模式。")
        return {}

active_executors = {}

async def mqtt_loop():
    tls_context = ssl.create_default_context()
    # 📝 在這裡加入重連迴圈，防止 MQTT 瞬斷讓整個程序死亡
    while True:
        try:
            async with aiomqtt.Client(
                hostname=MQTT_BROKER, port=MQTT_PORT,
                username=MQTT_USER, password=MQTT_PASSWORD,
                tls_context=tls_context
            ) as client:
                await client.subscribe(TOPIC_EXECUTE)
                print("🎧 狙擊手監聽總部連線成功 (Executor Ready)...")

                async for message in client.messages:
                    try:
                        payload = json.loads(message.payload.decode())

                        target_uid = str(payload.get("user_id"))
                        c_no = payload.get("course_no")
                        msg_time = payload.get("timestamp", time.time()) 
                        # 📝 讀取 Discord 傳來的雙模式設定 (預設皆為 False)
                        is_peak = payload.get("is_peak", False)
                        is_fast = payload.get("is_fast", False)

                        current_time = time.time()

                        if current_time - msg_time > 15:
                            print(f"🗑️ 丟棄過期指令: {c_no}")
                            continue

                        # 🔥 [修改 3] 移除 ALL 判斷，必須指定存在的 user_id
                        if target_uid not in active_executors:
                            continue

                        executor = active_executors[target_uid]

                        # 🔥 [修改 4] 檢查【該使用者】的黑名單，而不是全域變數
                        if c_no in executor.blacklisted_courses:
                            print(f"🛑 [已封鎖] {executor.student_id} 對課程 {c_no} 曾發生致命錯誤，忽略。")
                            continue

                        # 🛡️ 修正版防手震 (加上目標 UID 綁定)
                        cooldown_key = f"{target_uid}_{c_no}"
                        last_exec = course_cooldowns.get(cooldown_key, 0)
                        if current_time - last_exec < 3.0:
                            print(f"🛡️ 觸發防手震: {c_no} 太頻繁，跳過。 (uid={target_uid})")
                            continue
                        course_cooldowns[cooldown_key] = current_time

                        # 直接指派任務給該使用者（同時傳遞是否為尖峰模式）
                        print(f"🔄 指派狙擊任務 [{c_no}] -> {executor.student_id} (Peak:{is_peak}, Fast:{is_fast})")
                        # 📝 把 is_peak 與 is_fast 都傳遞進任務裡
                        asyncio.create_task(run_sniper_task(client, executor, target_uid, c_no, is_peak, is_fast))
                    except Exception as e:
                        print(f"MQTT 訊息處理錯誤: {e}")
        except aiomqtt.MqttError as e:
            print(f"⚠️ MQTT 連線中斷，5 秒後嘗試重新連線: {e}")
            await asyncio.sleep(5)
        except Exception as e:
            print(f"❌ MQTT loop 未預期錯誤，5 秒後重啟: {e}")
            await asyncio.sleep(5)

async def run_sniper_task(client, executor, uid, c_no, is_peak=False, is_fast=False):
    try:
        start_time = time.time()
        # 📝 將 is_peak 與 is_fast 傳給加選主程式
        suc, msg, stop_flag = await asyncio.to_thread(executor.execute_add_course, c_no, is_peak, is_fast)
        duration = time.time() - start_time
        
        status_icon = "✅" if suc else ("⛔" if stop_flag else "❌")
        print(f"{status_icon} 任務結束 ({duration:.2f}s): {msg}")

        # 📝 修正：修正縮排錯誤，必須與 print 對齊
        if stop_flag:
            print(f"🛑 [重要] {executor.student_id} 停止監控 -> {c_no}")
            # 📝 只有在「失敗」且要求停止時(如衝堂、擋修)，才加入黑名單
            if not suc:
                executor.blacklisted_courses.add(c_no)

        res = {"user_id": uid, "course_no": c_no, "success": suc, "msg": msg, "should_stop": stop_flag}
        await client.publish(TOPIC_RESULT, json.dumps(res), qos=1)
        
        if not suc and not stop_flag: await asyncio.to_thread(executor.prepare_aim, c_no)
    except Exception as e:
        print(f"Task 執行錯誤: {e}")

async def keep_alive_loop():
    while True:
        for uid in list(active_executors.keys()):
            try:
                await asyncio.to_thread(active_executors[uid].keep_alive)
            except:
                pass

        # 🧹 定期清理全域的防手震字典 (清除 10 分鐘前遺留的紀錄)
        try:
            current_time = time.time()
            expired_keys = [k for k, ts in list(course_cooldowns.items()) if current_time - ts > 600]
            for k in expired_keys:
                del course_cooldowns[k]
        except Exception:
            pass

        await asyncio.sleep(60)

async def main():
    known_users = await fetch_discord_users_from_mqtt()
    
    print("\n==========================================")
    print("🚀 啟動狙擊手執行端 (CLI 無介面版)")
    print("==========================================\n")

    print("請選擇選課階段：")
    for key, val in COURSE_MODES.items():
        print(f"  [{key}] {val['name']}")
    # 📝 修正：優先讀取環境變數，若無才使用 input，利於伺服器部署
    if ENV_SNIPER_MODE in COURSE_MODES:
        mode_choice = ENV_SNIPER_MODE
        selected_mode = COURSE_MODES[mode_choice]
        print(f"✅ 自動選擇模式: {selected_mode['name']}")
    else:
        try:
            mode_choice = input("請輸入選項 (預設 2): ").strip() or "2"
        except EOFError:
            # 🛡️ 背景執行防呆：若無鍵盤輸入，強制給預設值
            mode_choice = "2"
        selected_mode = COURSE_MODES.get(mode_choice, COURSE_MODES["2"])
        print(f"✅ 模式: {selected_mode['name']}")

    valid_accounts = []
    for acc in ACCOUNTS_LIST:
        if acc["id"]: valid_accounts.append(acc)
    if not valid_accounts:
        print("\n⚠️ ACCOUNTS_LIST 未設定，進入手動輸入模式")
        while True:
            s_id = input("請輸入學號 (直接按 Enter 結束): ").strip()
            if not s_id: break
            s_pwd = input(f"請輸入 {s_id} 的密碼: ").strip()
            valid_accounts.append({"id": s_id, "pwd": s_pwd})

    user_choices = list(known_users.items())

    for account in valid_accounts:
        student_id = account["id"]
        pwd = account["pwd"]
        print(f"\n👉 準備綁定學號: [{student_id}]")
        
        discord_id = "123" 

        # 📝 完美版：如果 .env 有設定就自動綁定，沒設定就跳出選單讓你選，並防止在無終端環境中崩潰
        if ENV_DISCORD_ID:
            discord_id = ENV_DISCORD_ID
            print(f"✅ 自動綁定 Discord ID: {discord_id}")
        else:
            try:
                if HEADLESS_MODE:
                    # 背景模式且沒設定，使用保險值 123
                    discord_id = "123"
                    print("⚠️ [警告] .env 未設定 DISCORD_USER_ID 且為背景模式，強制綁定為 123。")
                else:
                    # 只有在有畫面/互動的情況才會跳出選單
                    if not user_choices:
                        try:
                            discord_id = input("請輸入綁定的 Discord User ID (預設 123): ").strip() or "123"
                        except EOFError:
                            discord_id = "123"
                            print("⚠️ 無法讀取輸入，已使用預設 Discord ID: 123")
                    else:
                        print("請選擇對應的 Discord 使用者：")
                        for idx, (name, uid) in enumerate(user_choices):
                            print(f"  [{idx + 1}] {name}")
                        print(f"  [0] 手動輸入")

                        while True:
                            try:
                                choice = input("請輸入選項: ").strip()
                            except EOFError:
                                discord_id = "123"
                                print("⚠️ 無法讀取輸入，已使用預設 Discord ID: 123")
                                break

                            if choice == "0":
                                try:
                                    discord_id = input("請輸入 Discord ID: ").strip()
                                except EOFError:
                                    discord_id = "123"
                                break
                            elif choice.isdigit():
                                idx = int(choice) - 1
                                if 0 <= idx < len(user_choices):
                                    discord_id = user_choices[idx][1]
                                    print(f"✅ 已選擇: {user_choices[idx][0]}")
                                    break
                            print("輸入錯誤，請重試。")
            except Exception:
                discord_id = "123"

        executor = SniperExecutor(discord_id, student_id, pwd, selected_mode)
        # 先暫存 executor，稍後並行啟動所有瀏覽器
        active_executors[discord_id] = executor
        print(f"✅ 狙擊手預備: {student_id} (Discord: {discord_id}) 已登記")
        
    if not active_executors:
        print("❌ 沒有設定任何帳號，程式結束。")
        return

    # 📝 提前在主執行緒下載/更新 ChromeDriver，避免多執行緒同時下載引發檔案鎖定(Permission denied)
    try:
        print("\n檢查並更新 ChromeDriver 核心...")
        ChromeDriverManager().install()
    except Exception as e:
        print(f"⚠️ 檢查 ChromeDriver 時發生錯誤: {e}")

    # 🚀 同時啟動所有瀏覽器，避免逐一等待導致延遲啟動
    print("\n啟動所有瀏覽器中，請稍候...")
    startup_tasks = [asyncio.to_thread(ex.start_driver) for ex in active_executors.values()]
    await asyncio.gather(*startup_tasks)
    print("✅ 所有狙擊手皆已就位！\n")

    await asyncio.gather(mqtt_loop(), keep_alive_loop())

if __name__ == "__main__":
    import sys
    if sys.platform == 'win32' and hasattr(asyncio, 'WindowsSelectorEventLoopPolicy'):
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 狙擊手系統已手動關閉。")
    except Exception as e:
        print(f"\n⚠️ 系統異常關閉: {e}")
    finally:
        # 📝 程式結束時，強制清理所有活躍的瀏覽器，避免把記憶體吃光
        try:
            for uid, executor in list(active_executors.items()):
                try:
                    executor.cleanup()
                except: pass
        except: pass