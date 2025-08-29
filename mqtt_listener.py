import paho.mqtt.client as mqtt
import json
import time
import os
import importlib.util
from typing import Dict, Callable
from result import Result, Ok, Err

class MQTTListener:
    def __init__(self, config_path: str = "config.json"):
        self.config_path = config_path
        self.config = self._load_config()
        self.last_config_mtime = os.path.getmtime(config_path)
        self.callback_cache = {}  # 缓存 Handler 函数和最后修改时间：{handler_path: (func, mtime)}
        self.subscribed_topics = set()
        self.client = self._init_client()

    def _load_config(self) -> Dict:
        """加载配置文件"""
        with open(self.config_path, "r", encoding="utf-8") as f:
            return json.load(f)

    def _init_client(self) -> mqtt.Client:
        """初始化 MQTT 客户端"""
        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, "python-mqtt-listener")
        client.on_message = self._on_message
        broker = self.config.get("broker", "127.0.0.1")
        port = self.config.get("port", 1883)
        client.connect(broker, port, 60)
        client.loop_start()
        return client

    def _load_callback(self, callback_path: str) -> Callable:
        """动态导入 Callback 脚本中的 callback() 函数"""
        # 检查 Callback 文件是否更新
        current_mtime = os.path.getmtime(callback_path)
        cached = self.callback_cache.get(callback_path)

        # 若未缓存或文件已更新，重新导入
        if not cached or current_mtime != cached[1]:
            try:
                # 动态导入模块
                spec = importlib.util.spec_from_file_location(
                    "callback_module", callback_path
                )
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)
                # 缓存 Callback 函数和修改时间
                self.callback_cache[callback_path] = (module.callback, current_mtime)
                print(f"加载/更新 Callback: {callback_path}")
            except Exception as e:
                print(f"导入 Callback 失败 {callback_path}: {e}")
                return lambda *args: None  # 返回空函数避免崩溃

        return self.callback_cache[callback_path][0]

    def _on_message(self, client, userdata, msg):
        """收到消息时，调用对应的 Callback 处理"""
        begin_time = time.time()
        topic = msg.topic
        payload = msg.payload.decode("utf-8")
        # 从配置获取当前 Topic 的 Callback 路径
        topic_config = self.config.get("topics", {}).get(topic)
        if not topic_config:
            print(f"未配置 {topic} 的 Callback")
            return

        # 加载并执行 Callback
        callback = self._load_callback(topic_config["callback_path"])
        if not callback:
            print(f"加载 {topic_config['callback_path']} 失败")
            return
        try:
            result = callback(topic, payload)  # 调用 Callback 中的 callback() 函数
        except Exception as e:
            print(f"Callback {topic_config['callback_path']} 执行失败: {e}")
            return
        if result.is_ok():
            print(f"Callback {topic_config['callback_path']} 执行成功")
            v = result.unwrap()
            print(f"Callback {topic_config['callback_path']} 执行结果: {v}")
        else:
            print(f"Callback {topic_config['callback_path']} 执行失败: {result.error}")
            e = result.error
        end_time = time.time()
        print(f"Callback {topic_config['callback_path']} 执行耗时: {end_time - begin_time} 单位: 秒")

    def _sync_subscriptions(self):
        """同步订阅状态：新增/取消 Topic"""
        current_topics = set(self.config.get("topics", {}).keys())
        # 取消已删除的 Topic
        for topic in self.subscribed_topics - current_topics:
            self.client.unsubscribe(topic)
            self.subscribed_topics.remove(topic)
            print(f"取消订阅: {topic}")
        # 订阅新增的 Topic
        for topic in current_topics - self.subscribed_topics:
            qos = self.config["topics"][topic].get("qos", 0)
            self.client.subscribe(topic, qos)
            self.subscribed_topics.add(topic)
            print(f"订阅: {topic} (QoS={qos})")

    def run(self):
        """主循环：检测配置和 Handler 更新，同步订阅"""
        while True:
            # 检查配置文件是否更新
            if os.path.getmtime(self.config_path) > self.last_config_mtime:
                print("\n配置文件已更新，重新加载...")
                self.config = self._load_config()
                self.last_config_mtime = os.path.getmtime(self.config_path)

            # 同步订阅
            self._sync_subscriptions()

            # 等待检查间隔
            time.sleep(self.config.get("interval", 5))

if __name__ == "__main__":
    # 确保 handlers 目录存在
    os.makedirs("callback", exist_ok=True)
    client = MQTTListener()
    client.run()