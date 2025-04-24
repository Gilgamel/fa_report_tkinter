# src/database.py
import psycopg2
from datetime import datetime
from config import DATABASE_LOGGER

class Database:
    def __init__(self, config):
        self.config = config
        self._log_operation("SYSTEM", "INIT", "Database connection initialized")

    def _log_operation(self, user, action, details):
        """统一日志记录方法"""
        log_entry = {
            'user': user,
            'action': action,
            'host': self.config.get('host'),
            'port': self.config.get('port'),
            'database': self.config.get('dbname'),
            'details': details,
            'timestamp': datetime.now().isoformat()
        }
        DATABASE_LOGGER.info(str(log_entry))

    def execute(self, query, params=None, user="SYSTEM"):
        start_time = datetime.now()
        try:
            with psycopg2.connect(**self.config) as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query, params)
                    
                    # 记录成功日志
                    self._log_operation(
                        user=user,
                        action="QUERY_EXECUTE",
                        details={
                            'query': query,
                            'params': self._sanitize_params(params),
                            'rows_affected': cursor.rowcount,
                            'duration': (datetime.now() - start_time).total_seconds()
                        }
                    )
                    return cursor.fetchall()
        except Exception as e:
            # 记录错误日志
            self._log_operation(
                user=user,
                action="QUERY_ERROR",
                details={
                    'error': str(e),
                    'query': query,
                    'params': self._sanitize_params(params)
                }
            )
            raise

    def _sanitize_params(self, params):
        """参数脱敏处理"""
        if isinstance(params, dict):
            return {k: '*****' if 'password' in k.lower() else v for k, v in params.items()}
        return params