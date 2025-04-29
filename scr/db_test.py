import psycopg2
from dotenv import load_dotenv
import os

# 加载环境变量
load_dotenv()

def test_connection():
    conn = psycopg2.connect(
        host=os.getenv("SUPABASE_HOST"),
        port=os.getenv("SUPABASE_PORT"),
        dbname=os.getenv("SUPABASE_DB"),
        user=os.getenv("SUPABASE_USER"),
        password=os.getenv("SUPABASE_PASSWORD")
    )
    
    try:
        with conn.cursor() as cur:
            # 创建测试表（如果不存在）
            cur.execute("""
                CREATE TABLE IF NOT EXISTS test_table (
                    id SERIAL PRIMARY KEY,
                    test_data VARCHAR(100)
            """)
            
            # 插入测试数据
            cur.execute("INSERT INTO test_table (test_data) VALUES (%s)", ("Supabase连接测试",))
            conn.commit()
            
            # 验证数据
            cur.execute("SELECT * FROM test_table")
            print("当前测试数据:", cur.fetchall())
            
    finally:
        conn.close()

if __name__ == "__main__":
    test_connection()