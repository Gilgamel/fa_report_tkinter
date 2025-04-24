# src/database.py
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime

# 初始化日志配置
def setup_database_logger():
    logger = logging.getLogger('database')
    logger.setLevel(logging.INFO)
    
    # 创建日志处理器（100MB轮转，保留3个备份）
    handler = RotatingFileHandler(
        'database_audit.log',
        maxBytes=100*1024*1024,
        backupCount=3,
        encoding='utf-8'
    )
    
    # 设置日志格式
    formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - [%(db_host)s:%(db_port)s] - %(message)s'
    )
    handler.setFormatter(formatter)
    
    logger.addHandler(handler)
    return logger

# 初始化日志（在类外执行一次）
db_logger = setup_database_logger()

class DatabaseManager:
    def __init__(self, config):
        self.config = config
        self._add_connection_log("初始化数据库连接")
        
    def _add_connection_log(self, message):
        """专用方法记录连接日志"""
        extra = {
            'db_host': self.config.get('host', 'unknown'),
            'db_port': self.config.get('port', 'unknown')
        }
        db_logger.info(message, extra=extra)
    
    def connect(self):
        try:
            self.connection = psycopg2.connect(**self.config)
            self._add_connection_log("数据库连接成功")
        except Exception as e:
            self._add_connection_log(f"连接失败 - {str(e)}")
            raise
    
    def execute_query(self, query, params=None):
        start_time = datetime.now()
        try:
            cursor = self.connection.cursor()
            cursor.execute(query, params)
            
            # 记录操作元数据
            log_data = {
                'query': query,
                'params': str(params)[:100],  # 限制参数长度
                'rows_affected': cursor.rowcount,
                'duration': (datetime.now() - start_time).total_seconds()
            }
            db_logger.info("执行查询成功", extra=log_data)
            
            return cursor
        except Exception as e:
            log_data = {
                'query': query,
                'params': str(params)[:100],
                'error': str(e)
            }
            db_logger.error("执行查询失败", extra=log_data)
            raise