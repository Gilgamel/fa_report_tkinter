import os
from dotenv import load_dotenv
import sys

# 修正路径：确保 .env 文件位置正确
dotenv_path = os.path.join(os.path.dirname(__file__), '../.env')
load_dotenv(dotenv_path)

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