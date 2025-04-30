# database_manager.py
import os
import yaml
import logging
import psycopg2
from pathlib import Path
from dotenv import load_dotenv
from psycopg2 import sql, errors
from datetime import datetime
import hashlib
from config import DatabaseConfig

# -------------------------
# 路径配置
# -------------------------
BASE_DIR = Path(__file__).parent.parent
CONFIG_PATH = BASE_DIR / "config" / "database.yaml"
ENV_PATH = BASE_DIR / ".env"

# -------------------------
# 环境变量加载
# -------------------------
load_dotenv(ENV_PATH, override=True)

# -------------------------
# 日志配置
# -------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(BASE_DIR / "logs" / "db_manager.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("DBManager")

class DatabaseManager:
    def __init__(self):
        self.conn = None
        self.cur = None
        self._connect()
    
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn and not self.conn.closed:
            self.cur.close()
            self.conn.close()
            logger.info("Database connection closed")
        return False

    def _connect(self):
        """建立数据库连接"""
        # 在 database_manager.py 的 _connect 方法开头添加
        logger.debug(f"DB连接参数: host={DatabaseConfig.HOST}, port={DatabaseConfig.PORT}")

        try:
            self.conn = psycopg2.connect(
                host=DatabaseConfig.HOST,
                port=DatabaseConfig.PORT,
                dbname=DatabaseConfig.DB_NAME,
                user=os.getenv("DB_USER"),
                password=os.getenv("DB_PASSWORD"),
                connect_timeout=5
            )
            self.cur = self.conn.cursor()
            logger.info(f"Connected to {os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}")
        except errors.OperationalError as e:
            logger.error(f"Connection failed: {self._parse_error(e)}")
            raise

    # ==================== 核心操作方法 ====================
    def insert_data(self, table_name: str, data: dict) -> bool:
        """通用数据插入方法"""
        try:
            columns = ', '.join(data.keys())
            placeholders = ', '.join(['%s'] * len(data))
            query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
                sql.Identifier(table_name),
                sql.SQL(columns),
                sql.SQL(placeholders)
            )
            self.cur.execute(query, list(data.values()))
            return True
        except errors.Error as e:
            self.conn.rollback()
            logger.error(f"Insert failed: {str(e)}")
            return False

    def check_duplicate(self, file_hash: str) -> bool:
        """文件哈希查重"""
        try:
            self.cur.execute(
                "SELECT EXISTS(SELECT 1 FROM upload_history WHERE file_hash = %s)",
                (file_hash,)
            )
            return self.cur.fetchone()[0]
        except errors.Error as e:
            logger.error(f"Duplicate check failed: {str(e)}")
            return False

    def record_upload(self, file_name: str, file_hash: str, metadata: dict) -> None:
        """记录上传历史"""
        try:
            query = sql.SQL("""
                INSERT INTO upload_history 
                (file_name, file_hash, country_code, platform, channel, data_type)
                VALUES (%s, %s, %s, %s, %s, %s)
            """)
            self.cur.execute(query, (
                file_name, file_hash,
                metadata.get('country'),
                metadata.get('platform'),
                metadata.get('channel'),
                metadata.get('data_type')
            ))
            self.conn.commit()
        except errors.Error as e:
            self.conn.rollback()
            logger.error(f"History record failed: {str(e)}")

    # ==================== 表结构管理 ====================
    def create_hierarchy(self):
        """创建分层表结构"""
        try:
            with open(CONFIG_PATH, "r", encoding="utf-8") as f:
                config = yaml.safe_load(f)

            # 创建主表
            self._execute_sql("""
                CREATE TABLE IF NOT EXISTS transactions (
                    transaction_id BIGSERIAL,
                    country_code CHAR(2) NOT NULL,
                    platform VARCHAR(20) NOT NULL,
                    channel VARCHAR(50) NOT NULL,
                    data_type VARCHAR(20) NOT NULL,
                    transaction_date DATE NOT NULL,
                    amount NUMERIC(12,2),
                    raw_data JSONB,
                    PRIMARY KEY (country_code, platform, channel, data_type, transaction_id)
                ) PARTITION BY LIST (country_code);
            """)

            # 创建上传历史表
            self._execute_sql("""
                CREATE TABLE IF NOT EXISTS upload_history (
                    upload_id SERIAL PRIMARY KEY,
                    upload_time TIMESTAMP DEFAULT NOW(),
                    file_name VARCHAR(255) NOT NULL,
                    file_hash CHAR(64) UNIQUE NOT NULL,
                    country_code CHAR(2),
                    platform VARCHAR(20),
                    channel VARCHAR(50),
                    data_type VARCHAR(20)
                );
            """)

            # 遍历配置创建分区
            for country in config["countries"]:
                self._create_country_partition(country, config)

            logger.info("Database schema initialized")
            
        except Exception as e:
            logger.error(f"Schema creation failed: {str(e)}")
            raise

    def _create_country_partition(self, country: str, config: dict):
        """创建国家层级分区"""

        # 统一转换为小写
        country_lower = country.lower()  # 新增
        country_part = f"country_{country_lower}"  # 修改
    
        self._create_partition(
            parent_table="transactions",
            partition_name=country_part,
            value=country_lower,  # 关键修改：使用小写值
            subpartition="PARTITION BY LIST (platform)"
        )

        for platform in config["platforms"]:
            self._create_platform_partition(country, country_part, platform, config)

    def _create_platform_partition(self, country: str, country_part: str, platform: str, config: dict):
        """创建平台层级分区"""
        safe_platform = platform.replace(" ", "_").lower()
        platform_part = f"{country_part}_{safe_platform}"
        self._create_partition(
            parent_table=country_part,
            partition_name=platform_part,
            value=safe_platform,
            subpartition="PARTITION BY LIST (channel)"
        )

        channels = config["channels"].get(country, {}).get(platform, [])
        for channel in channels:
            self._create_channel_partition(platform_part, channel, config)

    def _create_channel_partition(self, platform_part: str, channel: str, config: dict):
        """创建渠道层级分区"""
        safe_channel = channel.replace(" ", "_").lower()
        channel_part = f"{platform_part}_{safe_channel}"
        self._create_partition(
            parent_table=platform_part,
            partition_name=channel_part,
            value=safe_channel,
            subpartition="PARTITION BY LIST (data_type)"
        )

        for dtype in config["data_types"]:
            self._create_data_type_partition(channel_part, dtype)

        self._create_partition(
            parent_table=channel_part,
            partition_name=f"{channel_part}_default",
            is_default=True
        )

    def _create_data_type_partition(self, channel_part: str, dtype: str):
        """创建数据类型分区"""
        dtype_part = f"{channel_part}_{dtype.lower()}"
        self._create_partition(
            parent_table=channel_part,
            partition_name=dtype_part,
            value=dtype.lower()
        )
        self._create_indexes(dtype_part)

    # ==================== 工具方法 ====================
    def _create_partition(self, parent_table: str, partition_name: str, 
                         value: str = None, subpartition: str = "", 
                         is_default: bool = False):
        """通用分区创建方法"""
        try:
            if is_default:
                query = sql.SQL("""
                    CREATE TABLE IF NOT EXISTS {partition}
                    PARTITION OF {parent} DEFAULT {subpartition}
                """)
            else:
                query = sql.SQL("""
                    CREATE TABLE IF NOT EXISTS {partition}
                    PARTITION OF {parent} FOR VALUES IN (%s) {subpartition}
                """)

            self.cur.execute(
                query.format(
                    partition=sql.Identifier(partition_name),
                    parent=sql.Identifier(parent_table),
                    subpartition=sql.SQL(subpartition)
                ), 
                (value,) if not is_default else None
            )
            self.conn.commit()
        except errors.DuplicateTable:
            logger.debug(f"Partition {partition_name} already exists")
        except errors.Error as e:
            self.conn.rollback()
            logger.error(f"Partition creation failed: {str(e)}")
            raise

    def _create_indexes(self, table_name: str):
        """创建标准索引"""
        try:
            # 交易日期索引
            self._execute_sql(
                sql.SQL("""
                    CREATE INDEX IF NOT EXISTS {idx} 
                    ON {table} (transaction_date)
                """).format(
                    idx=sql.Identifier(f"idx_{table_name}_date"),
                    table=sql.Identifier(table_name)
                )
            )
            
            # JSONB数据索引
            self._execute_sql(
                sql.SQL("""
                    CREATE INDEX IF NOT EXISTS {idx} 
                    ON {table} USING GIN (raw_data)
                """).format(
                    idx=sql.Identifier(f"idx_{table_name}_data"),
                    table=sql.Identifier(table_name)
                )
            )
        except errors.Error as e:
            logger.error(f"Index creation failed: {str(e)}")
            raise

    def _execute_sql(self, query, params=None):
        """执行SQL语句"""
        try:
            self.cur.execute(query, params)
            self.conn.commit()
        except errors.Error as e:
            self.conn.rollback()
            logger.error(f"SQL execution failed: {str(e)}")
            raise

    @staticmethod
    def _parse_error(e: errors.OperationalError) -> str:
        """错误信息解析"""
        error_map = {
            "password authentication failed": "Invalid credentials",
            "could not translate host name": f"Invalid host: {os.getenv('DB_HOST')}",
            "Connection refused": f"Connection refused at {os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}",
            "timeout expired": "Connection timeout"
        }
        return next(
            (v for k, v in error_map.items() if k in str(e)),
            f"Database error: {str(e)}"
        )

if __name__ == "__main__":
    try:
        logger.info("Initializing database...")
        db = DatabaseManager()
        db.create_hierarchy()
        logger.info("Database initialization completed")
    except Exception as e:
        logger.critical(f"Initialization failed: {str(e)}")
        exit(1)