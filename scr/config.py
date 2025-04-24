import os
from dotenv import load_dotenv
import sys
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

# load .env
dotenv_path = os.path.join(os.path.dirname(__file__), '../.env')
load_dotenv(dotenv_path)

# log config
# create folder to store log file
LOG_DIR = Path(__file__).parent.parent / "logs"
LOG_DIR.mkdir(exist_ok=True)

def setup_audit_logger(name, log_file):
    """审计日志专用配置"""
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    
    # 防止重复添加handler
    if logger.handlers:
        return logger
    
    # 配置日志格式（包含时间戳、操作类型、用户信息）
    formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - [%(user)s] - %(user_action)s - %(message)s'
    )
    
    # 文件处理器（每个日志文件最大100MB，保留3个备份）
    handler = RotatingFileHandler(
        LOG_DIR / log_file,
        maxBytes=100*1024*1024,
        backupCount=3,
        encoding='utf-8'
    )
    handler.setFormatter(formatter)
    
    logger.addHandler(handler)
    return logger

# initial 
DATABASE_AUDIT_LOGGER = setup_audit_logger('database_audit', 'database_audit.log')
USER_ACTION_LOGGER = setup_audit_logger('user_action', 'user_actions.log')


# database config
class DatabaseConfig:
    # 基础配置
    HOST = os.getenv("DB_HOST", "lin-28498-11503-pgsql-primary.servers.linodedb.net")
    PORT = int(os.getenv("DB_PORT", 5432))
    DB_NAME = "s2report"
    
    # 敏感信息（必须从环境变量读取）
    USER = os.getenv("DB_USER")  # type: ignore
    PASSWORD = os.getenv("DB_PASSWORD")  # type: ignore
    
    # 添加类方法装饰器
    @classmethod
    def validate(cls):
        """验证必要配置项"""
        if not all([cls.USER, cls.PASSWORD]):
            raise ValueError("数据库账号信息未正确配置！")

    @classmethod
    def get_config_dict(cls):
        """生成连接字典"""
        return {
            "host": cls.HOST,
            "port": cls.PORT,
            "dbname": cls.DB_NAME,
            "user": cls.USER,
            "password": cls.PASSWORD
        }

def print_config_summary():
    """打印配置摘要"""
    print("="*50)
    print("当前数据库配置验证")
    print("="*50)
    print(f"Host: {DatabaseConfig.HOST}")
    print(f"Port: {DatabaseConfig.PORT} ({type(DatabaseConfig.PORT)})")
    print(f"Database: {DatabaseConfig.DB_NAME}")
    print(f"User: {DatabaseConfig.USER or '❌ 未设置'}")
    print(f"Password: {'******' if DatabaseConfig.PASSWORD else '❌ 未设置'}")
    print("-"*50)

if __name__ == "__main__":
    print_config_summary()
    try:
        DatabaseConfig.validate()
        print("✅ 配置验证通过")
    except Exception as e:
        print(f"❌ 验证失败: {str(e)}")
        sys.exit(1)