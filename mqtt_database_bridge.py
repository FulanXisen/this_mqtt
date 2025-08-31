import paho.mqtt.client as mqtt
import json
import time
import os
import importlib.util
from typing import Dict, Callable
from result import Result, Ok, Err
from loguru import logger
import inspect
import typing
from typing import Any, Callable, get_origin, get_args


def callback(client, userdata, msg) -> Result[bool, str]:
    """默认的 Callback 函数，用于处理 MQTT 消息"""
    logger.warning(
        f"Default callback received message: {msg.payload.decode()} on topic {msg.topic}"
    )
    return Ok(True)


class MQTTDatabaseBridge:
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

    def _default_callback(self) -> Callable:
        """返回默认的 Callback 函数"""
        return callback

    def _init_client(self) -> mqtt.Client:
        """初始化 MQTT 客户端"""
        client = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2, "python-mqtt-database-bridge"
        )
        client.on_message = self._on_message
        broker = self.config.get("broker", "127.0.0.1")
        port = self.config.get("port", 1883)
        client.connect(broker, port, 60)
        client.loop_start()
        return client

    def _validate_callback_signature(self, module) -> bool:
        """验证 Callback 函数签名是否正确"""
        # 1. 检查模块是否有 callback 属性
        if not hasattr(module, "callback"):
            logger.warning(f"module {module.__name__} 没有 callback 属性")
            return False
        # 2. 检查 callback 是否是函数
        func = getattr(module, "callback")
        if not inspect.isfunction(func):
            logger.warning(f"Callback 不是函数 {module}")
            return False
        # 3. 检查参数名称是否正确
        sig = inspect.signature(func)
        expected_params = ["client", "userdata", "msg"]
        actual_params = list(sig.parameters.keys())
        if actual_params != expected_params:
            logger.warning(f"参数不匹配：期望 {expected_params}，实际 {actual_params}")
            return False
        # 4. 检查所有参数是否没有默认值
        for param in sig.parameters.values():
            if param.default is not inspect.Parameter.empty:
                logger.warning(f"参数 {param.name} 不能有默认值")
                return False
        # 5. 检查返回值是否为 Union 类型
        return_annotation = sig.return_annotation
        origin = get_origin(return_annotation)
        if origin is not typing.Union:
            logger.warning(f"返回值类型错误：期望 {typing.Union}，实际 {origin}")
            return False
        # 6. 检查Union是否为 Ok | Err
        args = get_args(return_annotation)
        if len(args) != 2:
            logger.warning(f"返回值Union数量错误：期望 2, 实际 {len(args)}")
            return False
        # 7. 检查成功类型和错误类型是否匹配
        success_type, error_type = args
        success_origin_type, error_origin_type = (
            get_origin(success_type),
            get_origin(error_type),
        )
        if (success_origin_type is not Ok) or (error_origin_type is not Err):
            logger.warning(
                f"返回值Union类型错误：期望 ({Ok}, {Err})，实际 ({success_origin_type}, {error_origin_type})"
            )
            return False
        success_generic_type, error_generic_type = (
            get_args(success_type)[0],
            get_args(error_type)[0],
        )
        # 8. 检查返回值的泛型参数是否为 (bool, str)
        if success_generic_type is not bool or error_generic_type is not str:
            logger.warning(
                f"返回值泛型参数类型错误：期望 ({bool}, {str})，实际 ({success_generic_type}, {error_generic_type})"
            )
            return False
        return True

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
                # 验证 Callback 函数签名
                if not self._validate_callback_signature(module):
                    logger.warning(f"Callback 函数签名错误 {callback_path}")
                    logger.warning(f"使用默认 Callback 函数代替 {callback_path}")
                    return callback  # 返回默认函数
                # 缓存 Callback 函数和修改时间
                self.callback_cache[callback_path] = (module.callback, current_mtime)
                logger.info(f"加载/更新 Callback: {callback_path}")
            except Exception as e:
                logger.warning(f"导入 Callback 失败 {callback_path}: {e}")
                logger.warning(f"使用默认 Callback 函数代替 {callback_path}")
                return callback  # 返回默认函数

        return self.callback_cache[callback_path][0]

    def _on_message(self, client, userdata, msg):
        """收到消息时，调用对应的 Callback 处理"""
        begin_time = time.time()
        # 从配置获取当前 Topic 的 Callback 路径
        topic_config = self.config.get("topics", {}).get(msg.topic)
        if not topic_config:
            logger.warning(f"未配置 {msg.topic} 的 Callback")
            return

        # 加载并执行 Callback
        callback = self._load_callback(topic_config["callback_path"])
        if not callback:
            logger.warning(f"加载 {topic_config['callback_path']} 失败")
            return
        try:
            result: Result[bool, str] = callback(
                client, userdata, msg
            )  # 调用 Callback 中的 callback() 函数
        except Exception as e:
            logger.warning(
                f"Callback {topic_config['callback_path']} 执行失败: raise error {e}"
            )
            return
        if result.is_ok():
            logger.info(f"Callback {topic_config['callback_path']} 执行成功")
            v = result.unwrap()
            logger.info(
                f"Callback {topic_config['callback_path']} 执行结果: Result.value{v}"
            )
        else:
            logger.warning(
                f"Callback {topic_config['callback_path']} 执行失败: Result Err {result.error}"
            )
        end_time = time.time()
        logger.info(
            f"Callback {topic_config['callback_path']} 执行耗时: {end_time - begin_time} 单位: 秒"
        )

    def _sync_subscriptions(self):
        """同步订阅状态：新增/取消 Topic"""
        current_topics = set(self.config.get("topics", {}).keys())
        # 取消已删除的 Topic
        for topic in self.subscribed_topics - current_topics:
            self.client.unsubscribe(topic)
            self.subscribed_topics.remove(topic)
            logger.info(f"取消订阅: {topic}")
        # 订阅新增的 Topic
        for topic in current_topics - self.subscribed_topics:
            qos = self.config["topics"][topic].get("qos", 0)
            self.client.subscribe(topic, qos)
            self.subscribed_topics.add(topic)
            logger.info(f"订阅: {topic} (QoS={qos})")

    def run(self):
        """主循环：检测配置和 Handler 更新，同步订阅"""
        while True:
            # 检查配置文件是否更新
            if os.path.getmtime(self.config_path) > self.last_config_mtime:
                logger.info("\n配置文件已更新，重新加载...")
                self.config = self._load_config()
                self.last_config_mtime = os.path.getmtime(self.config_path)

            # 同步订阅
            self._sync_subscriptions()

            # 等待检查间隔
            time.sleep(self.config.get("interval", 5))


if __name__ == "__main__":
    # 确保 handlers 目录存在
    os.makedirs("callback", exist_ok=True)
    client = MQTTDatabaseBridge()
    client.run()
