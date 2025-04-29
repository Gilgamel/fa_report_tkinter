# src/database_manager.py
import os
import yaml
import logging
import psycopg2
from pathlib import Path
from dotenv import load_dotenv
from psycopg2 import sql, errors
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
        self._connect()
        logger.info("数据库管理器初始化完成")

    def _connect(self):
        """建立数据库连接"""
        try:
            db_config = {
                "host": DatabaseConfig.HOST,
                "port": DatabaseConfig.PORT,
                "dbname": DatabaseConfig.DB_NAME,
                "user": os.getenv("DB_USER"),
                "password": os.getenv("DB_PASSWORD"),
                "connect_timeout": 5
            }
            
            logger.info(
                "连接参数: host=%s, port=%d, dbname=%s, user=%s",
                db_config["host"],
                db_config["port"],
                db_config["dbname"],
                db_config["user"]
            )

            self.conn = psycopg2.connect(**db_config)
            self.cur = self.conn.cursor()
            logger.info("✅ 数据库连接成功")
            
        except errors.OperationalError as e:
            logger.error("❌ 连接失败: %s", self._parse_error(e))
            raise

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

            # 遍历配置创建分区
            for country in config["countries"]:
                self._create_partition(
                    parent_table="transactions",
                    partition_name=f"country_{country}",
                    value=country,
                    subpartition="PARTITION BY LIST (platform)"
                )

                for platform in config["platforms"]:
                    platform_table = f"country_{country}_{platform}"
                    self._create_partition(
                        parent_table=f"country_{country}",
                        partition_name=platform_table,
                        value=platform,
                        subpartition="PARTITION BY LIST (channel)"
                    )

                    channels = config["channels"].get(country, {}).get(platform, [])
                    for channel in channels:
                        channel_table = f"{platform_table}_{channel}"
                        self._create_partition(
                            parent_table=platform_table,
                            partition_name=channel_table,
                            value=channel,
                            subpartition="PARTITION BY LIST (data_type)"
                        )

                        for dtype in config["data_types"]:
                            dtype_table = f"{channel_table}_{dtype}"
                            self._create_partition(
                                parent_table=channel_table,
                                partition_name=dtype_table,
                                value=dtype
                            )
                            self._create_indexes(dtype_table)

            logger.info("🗃️ 数据库分层结构创建完成")
            
        except Exception as e:  # 添加缺失的 except 块
            logger.error("创建表结构失败: %s", str(e))
            raise

    def _create_partition(self, parent_table: str, partition_name: str, value: str, subpartition: str = ""):
        """创建分区表"""
        try:
            query = sql.SQL("""
                CREATE TABLE IF NOT EXISTS {partition}
                PARTITION OF {parent}
                FOR VALUES IN (%s)
                {subpartition}
            """).format(
                partition=sql.Identifier(partition_name),
                parent=sql.Identifier(parent_table),
                subpartition=sql.SQL(subpartition)
            )
            self._execute_sql(query, (value,))
        except errors.DuplicateTable:
            logger.debug("分区已存在: %s", partition_name)
        except Exception as e:
            logger.error("创建分区失败: %s", str(e))
            raise

    def _create_indexes(self, table_name: str):
        """创建索引"""
        try:
            self._execute_sql(sql.SQL("""
                CREATE INDEX IF NOT EXISTS {idx_date}
                ON {table} (transaction_date)
            """).format(
                idx_date=sql.Identifier(f"idx_{table_name}_date"),
                table=sql.Identifier(table_name)
            ))
            
            self._execute_sql(sql.SQL("""
                CREATE INDEX IF NOT EXISTS {idx_data} 
                ON {table} USING GIN (raw_data)
            """).format(
                idx_data=sql.Identifier(f"idx_{table_name}_data"),
                table=sql.Identifier(table_name)
            ))
        except Exception as e:
            logger.error("索引创建失败: %s", str(e))
            raise

    def _execute_sql(self, query, params=None):
        """执行SQL语句"""
        try:
            self.cur.execute(query, params)
            self.conn.commit()
        except errors.Error as e:
            self.conn.rollback()
            logger.error("SQL执行错误: %s", str(e))
            raise

    @staticmethod
    def _parse_error(e: errors.OperationalError) -> str:
        """解析错误信息"""
        err_msg = str(e)
        if "password authentication failed" in err_msg:
            return "用户名/密码错误"
        elif "could not translate host name" in err_msg:
            return f"主机地址错误: {DatabaseConfig.HOST}"
        elif "Connection refused" in err_msg:
            return f"连接被拒绝（检查 {DatabaseConfig.HOST}:{DatabaseConfig.PORT}）"
        else:
            return f"数据库错误: {err_msg}"

    def __del__(self):
        if hasattr(self, "conn") and not self.conn.closed:
            self.conn.close()
            logger.info("数据库连接已关闭")

if __name__ == "__main__":
    try:
        logger.info("🚀 启动数据库初始化")
        db_manager = DatabaseManager()
        db_manager.create_hierarchy()
        logger.info("✅ 数据库初始化完成")
    except Exception as e:
        logger.critical("‼️ 初始化失败: %s", str(e))
        exit(1)