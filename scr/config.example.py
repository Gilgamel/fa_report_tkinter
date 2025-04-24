import os
from dotenv import load_dotenv
import sys

# 修正路径：确保 .env 文件位置正确
dotenv_path = os.path.join(os.path.dirname(__file__), '../.env')
load_dotenv(dotenv_path)

class DatabaseConfig:
    # 基础配置
    HOST = os.getenv("DB_HOST", "localhost")
    PORT = int(os.getenv("DB_PORT", 5432))
    DB_NAME = "dbdbdb"
    
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
