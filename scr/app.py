import tkinter as tk
from tkinter import ttk, messagebox, filedialog
import os
import csv
import pandas as pd
import json
from datetime import datetime
import traceback
import hashlib
import psycopg2
from psycopg2 import Error, OperationalError, sql
from config import USER_ACTION_LOGGER, DatabaseConfig
from database_manager import DatabaseManager


class FileUploadApp:
    def __init__(self, root):
        self.root = root
        self.root.title("File Upload System")
        self.root.geometry("600x450")
        self.current_user = "guest"
        
        # 初始化 StringVar 变量
        self.country_var = tk.StringVar()  # 先初始化
        self.platform_var = tk.StringVar() # 先初始化
        self.channel_var = tk.StringVar()
        
        # Channel database
        self.channel_data = {
            ("US", "Amazon"): ["Edifier Online Store", "Ventmere", "Ventmere Refurbished"],
            ("CA", "Amazon"): ["Edifier Online Store", "Ventmere", "Ventmere Refurbished"],
            ("MX", "Amazon"): ["UKTech", "BritishNews", "UKCooking"],
            ("US", "Shopify"): ["UK_Deals", "Prime_UK", "Amazon_UK"],
            ("CA", "Shopify"): ["JapanTech", "AnimeChannel"],
            ("US", "Walmart"): ["Walmart"],
            ("CA", "Walmart"): ["Walmart"],
            ("JP", "Amazon"): ["Japan_Deals", "Prime_JP"]
        }
        
        # Create main frame
        self.main_frame = ttk.Frame(root, padding="10")
        self.main_frame.pack(fill=tk.BOTH, expand=True)
        
        # Initialize UI components
        self.create_static_widgets()  # 此时变量已存在
        self.create_dynamic_widgets()
        
        # Set up event bindings
        self.country_var.trace_add('write', self.update_ui)
        self.platform_var.trace_add('write', self.update_ui)
        
    def create_static_widgets(self):
        """创建固定位置的组件"""
        # Country selection (row 0)
        ttk.Label(self.main_frame, text="Country:").grid(row=0, column=0, sticky=tk.W, pady=5)
        self.country_combo = ttk.Combobox(
            self.main_frame, 
            textvariable=self.country_var,  # 使用已初始化的变量
            values=["US", "CA", "MX", "JP", "AU"]
        )
        self.country_combo.grid(row=0, column=1, sticky=tk.EW, pady=5, padx=5)
        
        # Platform selection (row 1)
        ttk.Label(self.main_frame, text="Platform:").grid(row=1, column=0, sticky=tk.W, pady=5)
        self.platform_combo = ttk.Combobox(
            self.main_frame, 
            textvariable=self.platform_var,  # 使用已初始化的变量
            values=["Amazon", "Mirakl", "Shopify"]
        )
        self.platform_combo.grid(row=1, column=1, sticky=tk.EW, pady=5, padx=5)
        
        # Channel selection (row 2)
        ttk.Label(self.main_frame, text="Channel Name:").grid(row=2, column=0, sticky=tk.W, pady=5)
        self.channel_combo = ttk.Combobox(
            self.main_frame, 
            textvariable=self.channel_var, 
            state='readonly'
        )
        self.channel_combo.grid(row=2, column=1, sticky=tk.EW, pady=5, padx=5)

        # Configure grid weights
        self.main_frame.columnconfigure(1, weight=1)
        self.main_frame.rowconfigure(8, weight=1)

    def create_dynamic_widgets(self):
        """创建动态组件"""
        self.data_type_label = ttk.Label(self.main_frame, text="Data Type:")
        self.data_type_combo = ttk.Combobox(self.main_frame, values=["Invoiced", "Standard"], state="readonly")
        self.data_type_combo.set("Standard")
        
        self.file_label = ttk.Label(self.main_frame, text="Select File:")
        self.file_path_var = tk.StringVar()
        self.file_entry = ttk.Entry(self.main_frame, textvariable=self.file_path_var, state='readonly')
        self.browse_btn = ttk.Button(self.main_frame, text="Browse...", command=self.browse_file)
        
        self.upload_btn = ttk.Button(self.main_frame, text="Upload", command=self.upload_file)
        
        self.log_label = ttk.Label(self.main_frame, text="Operation Log:")
        self.log_text = tk.Text(self.main_frame, height=8, state='disabled')
        
        self.update_ui()

    def update_ui(self, *args):
        country = self.country_var.get()
        platform = self.platform_var.get()
        is_us_amazon = (country == "US" and platform == "Amazon")

        for widget in [self.data_type_label, self.data_type_combo,
                      self.file_label, self.file_entry, self.browse_btn,
                      self.upload_btn, self.log_label, self.log_text]:
            widget.grid_forget()

        base_row = 3
        if is_us_amazon:
            self.data_type_label.grid(row=base_row, column=0, sticky=tk.W, pady=5)
            self.data_type_combo.grid(row=base_row, column=1, sticky=tk.EW, pady=5, padx=5)
            base_row += 1

        self.file_label.grid(row=base_row, column=0, sticky=tk.W, pady=5)
        self.file_entry.grid(row=base_row, column=1, sticky=tk.EW, pady=5, padx=5)
        self.browse_btn.grid(row=base_row, column=2, pady=5, padx=5)
        base_row += 1

        self.upload_btn.grid(row=base_row, column=1, pady=20)
        base_row += 1

        self.log_label.grid(row=base_row, column=0, sticky=tk.W, pady=5)
        self.log_text.grid(row=base_row+1, column=0, columnspan=3, sticky=tk.NSEW, pady=5)

        if country and platform:
            channels = self.channel_data.get((country, platform), [])
            self.channel_combo['values'] = channels
            self.channel_combo['state'] = 'readonly' if channels else 'disabled'
            self.channel_combo.set(channels[0] if channels else '')

    def browse_file(self):
        file_path = filedialog.askopenfilename(
            filetypes=[("TXT Files", "*.txt"), ("CSV Files", "*.csv"), 
                      ("Excel Files", "*.xlsx"), ("All Files", "*.*")]
        )
        log_data = {
            "user": self.current_user,
            "action": "FILE_SELECTION",
            "file_name": os.path.basename(file_path) if file_path else None,
            "success": bool(file_path)
        }
        USER_ACTION_LOGGER.info("File selection attempt", extra=log_data)

        if file_path:
            self.file_path_var.set(file_path)
            self.add_log(f"已选择文件: {os.path.basename(file_path)}")

    def add_log(self, message):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.log_text.config(state='normal')
        self.log_text.insert(tk.END, f"[{timestamp}] {message}\n")
        self.log_text.see(tk.END)
        self.log_text.config(state='disabled')

    def upload_file(self):
        country = self.country_var.get()
        platform = self.platform_var.get()
        channel = self.channel_var.get()
        file_path = self.file_path_var.get()
        data_type = self.data_type_combo.get() if (country == "US" and platform == "Amazon") else ""

        audit_data = {
            "user": self.current_user,
            "user_action": "UPLOAD_STARTED",
            "country": country,
            "platform": platform,
            "channel": channel,
            "data_type": data_type,
            "file_name": os.path.basename(file_path) if file_path else None
        }
        USER_ACTION_LOGGER.info("上传流程启动", extra=audit_data)

        # 参数验证
        error_msg = []
        if not all([country, platform, channel, file_path]):
            error_msg.append("请填写所有必填字段")
        if country == "US" and platform == "Amazon" and not data_type:
            error_msg.append("美国亚马逊渠道必须选择数据类型")
        
        if error_msg:
            error_audit = audit_data.copy()
            error_audit.update({"errors": error_msg})
            USER_ACTION_LOGGER.warning("参数验证失败", extra=error_audit)
            messagebox.showerror("错误", "\n".join(error_msg))
            return
            
        if not os.path.exists(file_path):
            USER_ACTION_LOGGER.error("文件不存在", extra=audit_data)
            messagebox.showerror("错误", "文件不存在")
            return
            
        try:
            file_hash = self.calculate_file_hash(file_path)
            if self.check_duplicate_upload(file_hash):
                USER_ACTION_LOGGER.warning("重复文件检测", extra=audit_data)
                messagebox.showwarning("警告", "该文件已上传过")
                return

            # 核心上传流程
            self.add_log("▶ 开始处理文件...")
            file_data = self.parse_file(file_path)
            table_name = self._generate_table_name(country, platform, channel, data_type)
            
            success_count = 0
            with DatabaseManager() as db:
                for idx, record in enumerate(file_data, 1):
                    full_record = {
                        "country_code": country,
                        "platform": platform.replace(" ", "_"),
                        "channel": channel.replace(" ", "_"),
                        "data_type": data_type,
                        "transaction_date": datetime.now().date(),
                        "amount": float(record.get("amount", 0)),
                        "raw_data": json.dumps(record)
                    }
                    if "transaction_date" in record:
                        try:
                            full_record["transaction_date"] = datetime.strptime(
                                str(record["transaction_date"]), "%Y-%m-%d"
                            ).date()
                        except Exception as e:
                            self.add_log(f"⚠️ 记录{idx}日期错误: {str(e)}")
                    
                    if db.insert_data(table_name, full_record):
                        success_count += 1
                    
                    if idx % 10 == 0:
                        self.add_log(f"📊 进度: {idx}/{len(file_data)}")
                        self.root.update()

                db.record_upload(os.path.basename(file_path), file_hash, audit_data)

            # 在计算成功率前添加空值检查
            if len(file_data) == 0:
                messagebox.showerror("错误", "文件内容为空")
                return

            # 修改计算逻辑
            success_rate = (success_count / len(file_data)) * 100 if len(file_data) > 0 else 0
                            
            msg = f"""
            🎉 上传成功！
            成功记录: {success_count}/{len(file_data)} ({success_rate:.1f}%)
            目标表名: {table_name}
            """
            messagebox.showinfo("上传结果", msg.strip())
            self.add_log(msg.replace("\n", " "))

        except Exception as e:
            error_msg = f"❌ 上传失败: {str(e)}"
            error_audit = audit_data.copy()
            error_audit.update({
                "error_type": type(e).__name__,
                "error_msg": str(e),
                "traceback": traceback.format_exc()
            })
            USER_ACTION_LOGGER.error("上传异常", extra=error_audit)
            messagebox.showerror("错误", error_msg)
            self.add_log(error_msg)

    def parse_file(self, file_path: str) -> list:
        ext = os.path.splitext(file_path)[1].lower()
        try:
            if ext == '.txt':
                return self._parse_txt(file_path)
            elif ext == '.csv':
                return self._parse_csv(file_path)
            elif ext in ('.xls', '.xlsx'):
                return self._parse_excel(file_path)
            else:
                raise ValueError("不支持的文件格式")
        except Exception as e:
            USER_ACTION_LOGGER.error("文件解析失败", extra={
                "file": file_path,
                "error": str(e)
            })
            raise

    def _parse_txt(self, file_path: str) -> list:
        data = []
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                parts = line.strip().split('|')
                if len(parts) == 3:
                    try:
                        data.append({
                            "transaction_date": parts[0],
                            "amount": parts[1],
                            "description": parts[2]
                        })
                    except:
                        continue
        return data

    def _parse_csv(self, file_path: str) -> list:
        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            if 'amount' not in reader.fieldnames:
                raise ValueError("CSV文件必须包含amount列")
            return list(reader)

    def _parse_excel(self, file_path: str) -> list:
        df = pd.read_excel(file_path)
        if 'amount' not in df.columns:
            raise ValueError("Excel文件必须包含amount列")
        return df.replace({pd.NaT: None}).to_dict('records')

    def calculate_file_hash(self, file_path: str) -> str:
        hasher = hashlib.sha256()
        with open(file_path, 'rb') as f:
            while chunk := f.read(8192):
                hasher.update(chunk)
        return hasher.hexdigest()

    def check_duplicate_upload(self, file_hash: str) -> bool:
        with DatabaseManager() as db:
            return db.check_duplicate(file_hash)

    def _generate_table_name(self, country: str, platform: str, channel: str, data_type: str) -> str:
        elements = [
            "country",
            country.lower(),
            platform.lower().replace(" ", "_"),
            channel.lower().replace(" ", "_")
        ]
        if data_type:
            elements.append(data_type.lower())
        return "_".join(elements)

if __name__ == "__main__":
    root = tk.Tk()
    app = FileUploadApp(root)
    root.mainloop()