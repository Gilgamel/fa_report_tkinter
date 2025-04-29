# src/database.py
import psycopg2
from datetime import datetime
from contextlib import contextmanager
from typing import Optional, Union, Dict, List, Tuple
from config import DatabaseConfig, DATABASE_AUDIT_LOGGER

class Database:
    def __init__(self):
        """
        数据库连接初始化
        使用 config.py 中的 DatabaseConfig 配置
        """
        self.config = DatabaseConfig.get_config_dict()
        self._log_operation(
            user="SYSTEM",
            action="INIT",
            details={
                "message": "Database instance initialized",
                "component": "Database"
            }
        )

    @contextmanager
    def _managed_connection(self):
        """
        连接管理上下文管理器
        自动处理连接、事务和错误
        """
        conn = None
        try:
            conn = psycopg2.connect(**self.config)
            yield conn
            conn.commit()
        except psycopg2.DatabaseError as e:
            if conn:
                conn.rollback()
            raise
        finally:
            if conn:
                conn.close()

    def _log_operation(self, user: str, action: str, details: Dict):
        """
        审计日志记录方法
        使用 config.py 中配置的 DATABASE_AUDIT_LOGGER
        """
        log_data = {
            'user': user,
            'action': action,
            'host': self.config['host'],
            'port': self.config['port'],
            'database': self.config['dbname'],
            **details,
            'timestamp': datetime.now().isoformat()
        }
        # 使用 logger 的 log() 方法提供结构化日志
        DATABASE_AUDIT_LOGGER.log(
            level=logging.INFO,
            msg=details.get('message', 'Database operation'),
            extra={
                'user_action': action,
                'log_data': log_data
            }
        )

    def _sanitize_params(self, params: Union[Dict, List, Tuple, None]) -> Union[Dict, List]:
        """
        增强型参数脱敏
        处理不同类型参数并标记敏感字段
        """
        if params is None:
            return {}
        
        sensitive_keys = {'password', 'secret', 'token', 'api_key'}
        
        if isinstance(params, dict):
            return {
                k: '*****' if any(s in k.lower() for s in sensitive_keys) else v
                for k, v in params.items()
            }
        elif isinstance(params, (list, tuple)):
            return [
                '*****' if isinstance(p, str) and any(s in p.lower() for s in sensitive_keys) else p
                for p in params
            ]
        return params

    def execute(
        self,
        query: str,
        params: Optional[Union[Dict, List, Tuple]] = None,
        user: str = "SYSTEM",
        fetch: bool = True,
        **context
    ) -> Optional[List[Tuple]]:
        """
        执行数据库查询（支持审计和错误处理）
        
        :param query: SQL 查询语句
        :param params: 查询参数（支持 dict/list/tuple）
        :param user: 执行操作的用户标识
        :param fetch: 是否获取结果集（SELECT 用 True，INSERT/UPDATE 用 False）
        :param context: 审计上下文（国家、平台等业务参数）
        
        :return: 查询结果（当 fetch=True 时）或 None
        """
        start_time = datetime.now()
        audit_context = {
            'query': query,
            'params': self._sanitize_params(params),
            **context
        }

        try:
            with self._managed_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query, params)
                    
                    # 根据 fetch 参数决定是否获取结果
                    result = cursor.fetchall() if fetch else None
                    
                    # 记录成功日志
                    self._log_operation(
                        user=user,
                        action="QUERY_EXECUTE",
                        details={
                            'status': 'SUCCESS',
                            'rows_affected': cursor.rowcount,
                            'duration_sec': (datetime.now() - start_time).total_seconds(),
                            **audit_context
                        }
                    )
                    return result

        except psycopg2.Error as e:
            # 记录详细的错误日志
            self._log_operation(
                user=user,
                action="QUERY_ERROR",
                details={
                    'status': 'ERROR',
                    'error_type': e.__class__.__name__,
                    'error_message': str(e),
                    'duration_sec': (datetime.now() - start_time).total_seconds(),
                    **audit_context
                }
            )
            raise  # 重新抛出异常供上层处理

    def batch_execute(
        self,
        query: str,
        params_list: List[Union[Dict, List, Tuple]],
        user: str = "SYSTEM",
        **context
    ) -> int:
        """
        批量执行操作（用于大量INSERT/UPDATE）
        
        :return: 总影响行数
        """
        total_rows = 0
        start_time = datetime.now()
        
        try:
            with self._managed_connection() as conn:
                with conn.cursor() as cursor:
                    for params in params_list:
                        cursor.execute(query, params)
                        total_rows += cursor.rowcount
                    
                    self._log_operation(
                        user=user,
                        action="BATCH_EXECUTE",
                        details={
                            'status': 'SUCCESS',
                            'total_rows': total_rows,
                            'duration_sec': (datetime.now() - start_time).total_seconds(),
                            'query': query,
                            'params_samples': self._sanitize_params(params_list[:3]),  # 记录前3个参数样本
                            **context
                        }
                    )
                    return total_rows

        except psycopg2.Error as e:
            self._log_operation(
                user=user,
                action="BATCH_ERROR",
                details={
                    'status': 'ERROR',
                    'error_type': e.__class__.__name__,
                    'error_message': str(e),
                    'processed_rows': total_rows,
                    **context
                }
            )
            raise

    def test_connection(self) -> bool:
        """
        测试数据库连接是否正常
        """
        try:
            with self._managed_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1")
                    return cursor.fetchone()[0] == 1
        except psycopg2.Error:
            return False