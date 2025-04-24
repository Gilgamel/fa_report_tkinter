import unittest
import os
from pathlib import Path
from config import DATABASE_AUDIT_LOGGER, USER_ACTION_LOGGER

class TestAuditSystem(unittest.TestCase):
    log_dir = Path(__file__).parent.parent / "logs"

    def test_database_audit(self):
        # 触发测试日志
        DATABASE_AUDIT_LOGGER.info("自动化测试", extra={
            'user': 'unittest',
            'action': 'AUTO_TEST',
            'details': {'test_case': 'TC001'}
        })
        
        # 验证日志文件存在
        log_file = self.log_dir / "database_audit.log"
        self.assertTrue(log_file.exists())
        
        # 验证日志内容
        with open(log_file, 'r') as f:
            content = f.read()
            self.assertIn("AUTO_TEST", content)

if __name__ == '__main__':
    unittest.main()