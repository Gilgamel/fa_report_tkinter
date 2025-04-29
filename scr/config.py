# config.py
import os
import sys
import logging
import psycopg2
from logging.handlers import RotatingFileHandler
from pathlib import Path
from dotenv import load_dotenv
from psycopg2 import OperationalError, Error

# -------------------------
# 环境变量加载
# -------------------------
dotenv_path = os.path.join(os.path.dirname(__file__), '../.env')
load_dotenv(dotenv_path)

# -------------------------
# 日志配置
# -------------------------
LOG_DIR = Path(__file__).parent.parent / "logs"
LOG_DIR.mkdir(exist_ok=True)

def setup_audit_logger(name: str, log_file: str) -> logging.Logger:
    """审计日志专用配置"""
    logger = logging.getLogger(name)
    
    # 防止重复初始化
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)
    formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - [%(user)s] - %(user_action)s - %(message)s'
    )
    
    # 文件处理器（UTF-8编码）
    file_handler = RotatingFileHandler(
        LOG_DIR / log_file,
        maxBytes=100*1024*1024,  # 100MB
        backupCount=3,
        encoding='utf-8'
    )
    file_handler.setFormatter(formatter)
    
    logger.addHandler(file_handler)
    return logger

# -------------------------
# 数据库配置类
# -------------------------
class DatabaseConfig:
    # 基础配置
    HOST = os.getenv("DB_HOST", "lin-28498-11503-pgsql-primary.servers.linodedb.net")
    PORT = int(os.getenv("DB_PORT", 5432))  # 类型强制转换
    DB_NAME = "s2report"
    
    # 敏感信息（必须从环境变量读取）
    USER = os.getenv("DB_USER")
    PASSWORD = os.getenv("DB_PASSWORD")

    @classmethod
    def validate(cls) -> None:
        """增强验证：实际测试数据库连接"""
        # 基础检查
        if not all([cls.USER, cls.PASSWORD]):
            raise ValueError("数据库账号或密码未配置")

        # 连接参数检查
        config = cls.get_config_dict()
        if not isinstance(config["port"], int):
            raise TypeError(f"端口号必须为整数，当前类型：{type(config['port'])}")

        # 实际连接测试
        conn = None
        try:
            conn = psycopg2.connect(**config, connect_timeout=5)
            with conn.cursor() as cur:
                cur.execute("SELECT 1")  # 简单查询验证
                if cur.fetchone()[0] != 1:
                    raise OperationalError("基础查询验证失败")
        except OperationalError as e:
            raise ConnectionError(f"数据库连接失败: {cls._parse_error(e)}")
        except Error as e:
            raise ConnectionError(f"数据库错误: {str(e)}")
        finally:
            if conn and not conn.closed:
                conn.close()

    @classmethod
    def get_config_dict(cls) -> dict:
        """生成连接配置字典（隐藏敏感信息）"""
        return {
            "host": cls.HOST,
            "port": cls.PORT,
            "dbname": cls.DB_NAME,
            "user": cls.USER,
            "password": cls.PASSWORD
        }

    @staticmethod
    def _parse_error(error: OperationalError) -> str:
        """解析常见连接错误"""
        err_msg = str(error)
        
        if "password authentication failed" in err_msg:
            return "用户名或密码错误"
        elif "could not connect" in err_msg:
            return "无法连接到数据库服务器（检查网络/防火墙）"
        elif "timeout expired" in err_msg:
            return "连接超时（检查主机地址和端口）"
        elif "does not exist" in err_msg:
            return "数据库不存在"
        else:
            return f"未知连接错误: {err_msg}"

# -------------------------
# 主程序
# -------------------------
def print_config_summary():
    """打印配置摘要（隐藏敏感信息）"""
    print("="*50)
    print("数据库连接配置验证")
    print("="*50)
    print(f"Host: {DatabaseConfig.HOST}")
    print(f"Port: {DatabaseConfig.PORT} ({type(DatabaseConfig.PORT)})")
    print(f"Database: {DatabaseConfig.DB_NAME}")
    print(f"User: {DatabaseConfig.USER or '❌ 未设置'}")
    print(f"Password: {'******' if DatabaseConfig.PASSWORD else '❌ 未设置'}")
    print("-"*50)

if __name__ == "__main__":
    # 初始化日志
    DATABASE_AUDIT_LOGGER = setup_audit_logger('database_audit', 'database_audit.log')
    USER_ACTION_LOGGER = setup_audit_logger('user_action', 'user_actions.log')

    # 执行验证
    print_config_summary()
    try:
        DatabaseConfig.validate()
        print("✅ 数据库连接验证通过")
        DATABASE_AUDIT_LOGGER.info(
            "数据库连接成功",
            extra={
                "user": "system",
                "user_action": "CONNECTION_TEST",
                "host": DatabaseConfig.HOST,
                "port": DatabaseConfig.PORT
            }
        )
    except Exception as e:
        print(f"❌ 验证失败: {str(e)}")
        DATABASE_AUDIT_LOGGER.error(
            "数据库连接失败",
            extra={
                "user": "system",
                "user_action": "CONNECTION_TEST",
                "error": str(e),
                "host": DatabaseConfig.HOST,
                "port": DatabaseConfig.PORT
            }
        )
        sys.exit(1)