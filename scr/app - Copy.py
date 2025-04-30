import tkinter as tk
from tkinter import ttk, messagebox, filedialog
import os
from datetime import datetime
from config import USER_ACTION_LOGGER
import traceback
import hashlib


class FileUploadApp:
    def __init__(self, root):
        self.root = root
        self.root.title("File Upload System")
        self.root.geometry("600x450")
        self.current_user = "guest"
        
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
        self.create_static_widgets()
        self.create_dynamic_widgets()
        
        # Set up event bindings
        self.country_var.trace_add('write', self.update_ui)
        self.platform_var.trace_add('write', self.update_ui)
        
    def create_static_widgets(self):
        """创建固定位置的组件"""
        # Country selection (row 0)
        ttk.Label(self.main_frame, text="Country:").grid(row=0, column=0, sticky=tk.W, pady=5)
        self.country_var = tk.StringVar()
        self.country_combo = ttk.Combobox(self.main_frame, textvariable=self.country_var, 
                                        values=["US", "CA", "MX", "JP", "AU"])
        self.country_combo.grid(row=0, column=1, sticky=tk.EW, pady=5, padx=5)
        
        # Platform selection (row 1)
        ttk.Label(self.main_frame, text="Platform:").grid(row=1, column=0, sticky=tk.W, pady=5)
        self.platform_var = tk.StringVar()
        self.platform_combo = ttk.Combobox(self.main_frame, textvariable=self.platform_var, 
                                        values=["Amazon", "Mirakl", "Shopify"])
        self.platform_combo.grid(row=1, column=1, sticky=tk.EW, pady=5, padx=5)
        
        # Channel selection (row 2)
        ttk.Label(self.main_frame, text="Channel Name:").grid(row=2, column=0, sticky=tk.W, pady=5)
        self.channel_var = tk.StringVar()
        self.channel_combo = ttk.Combobox(self.main_frame, textvariable=self.channel_var, state='readonly')
        self.channel_combo.grid(row=2, column=1, sticky=tk.EW, pady=5, padx=5)

        # Configure grid weights
        self.main_frame.columnconfigure(1, weight=1)
        self.main_frame.rowconfigure(8, weight=1)

    def create_dynamic_widgets(self):
        """创建需要动态调整位置的组件"""
        # Data Type components
        self.data_type_label = ttk.Label(self.main_frame, text="Data Type:")
        self.data_type_combo = ttk.Combobox(self.main_frame, 
                                          values=["Invoiced", "Standard"],
                                          state="readonly")
        self.data_type_combo.set("Standard")
        
        # File selection
        self.file_label = ttk.Label(self.main_frame, text="Select File:")
        self.file_path_var = tk.StringVar()
        self.file_entry = ttk.Entry(self.main_frame, textvariable=self.file_path_var, state='readonly')
        self.browse_btn = ttk.Button(self.main_frame, text="Browse...", command=self.browse_file)
        
        # Upload button
        self.upload_btn = ttk.Button(self.main_frame, text="Upload", command=self.upload_file)
        
        # Log area
        self.log_label = ttk.Label(self.main_frame, text="Operation Log:")
        self.log_text = tk.Text(self.main_frame, height=8, state='disabled')
        
        # Initial layout
        self.update_ui()

    def update_ui(self, *args):
        """动态更新界面布局"""
        country = self.country_var.get()
        platform = self.platform_var.get()
        is_us_amazon = (country == "US" and platform == "Amazon")

        # 清除所有动态组件
        for widget in [self.data_type_label, self.data_type_combo,
                      self.file_label, self.file_entry, self.browse_btn,
                      self.upload_btn, self.log_label, self.log_text]:
            widget.grid_forget()

        # 基础行号配置
        base_row = 3
        if is_us_amazon:
            # 显示Data Type组件
            self.data_type_label.grid(row=base_row, column=0, sticky=tk.W, pady=5)
            self.data_type_combo.grid(row=base_row, column=1, sticky=tk.EW, pady=5, padx=5)
            base_row += 1

        # 文件选择组件
        self.file_label.grid(row=base_row, column=0, sticky=tk.W, pady=5)
        self.file_entry.grid(row=base_row, column=1, sticky=tk.EW, pady=5, padx=5)
        self.browse_btn.grid(row=base_row, column=2, pady=5, padx=5)
        base_row += 1

        # 上传按钮
        self.upload_btn.grid(row=base_row, column=1, pady=20)
        base_row += 1

        # 日志区域
        self.log_label.grid(row=base_row, column=0, sticky=tk.W, pady=5)
        self.log_text.grid(row=base_row+1, column=0, columnspan=3, sticky=tk.NSEW, pady=5)

        # 更新频道列表
        if country and platform:
            channels = self.channel_data.get((country, platform), [])
            self.channel_combo['values'] = channels
            self.channel_combo['state'] = 'readonly' if channels else 'disabled'
            self.channel_combo.set(channels[0] if channels else '')

    def browse_file(self):
        file_path = filedialog.askopenfilename(
            title="Select File",
            filetypes=[("TXT Files", "*.txt"), ("CSV Files", "*.csv"), ("Excel Files", "*.xlsx"), ("All Files", "*.*")]
        )

        # add audit log
        log_data = {
            "user": self.current_user,
            "action": "FILE_SELECTION",
            "file_name": os.path.basename(file_path) if file_path else None,
            "success": bool(file_path)
            }
        USER_ACTION_LOGGER.info("File selection attempt", extra=log_data)


        if file_path:
            self.file_path_var.set(file_path)
            self.add_log(f"Selected file: {os.path.basename(file_path)}")

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

        # upload -> audit start
        upload_start_time = datetime.now()
        audit_data = {
            "user": self.current_user,
            "user_action": "UPLOAD_STARTED",
            "country": country,
            "platform": platform,
            "channel": channel,
            "data_type": data_type,
            "file_name": os.path.basename(file_path) if file_path else None
        }
        USER_ACTION_LOGGER.info("Upload process initiated", extra=audit_data)        


        # Validation
        error_msg = []
        if not all([country, platform, channel, file_path]):
            error_msg.append("Please fill all required fields")
        if country == "US" and platform == "Amazon" and not data_type:
            error_msg.append("Data Type is required for US Amazon")
            
        if error_msg:
        # =========== 新增：验证失败审计 ===========
            error_audit = audit_data.copy()
            error_audit.update({
                "action": "VALIDATION_FAILED",
                "errors": error_msg,
                "missing_fields": {
                "country": not bool(country),
                "platform": not bool(platform),
                "channel": not bool(channel),
                "file": not bool(file_path),
                "data_type": (country == "US" and platform == "Amazon" and not data_type)
                }
            })
            USER_ACTION_LOGGER.warning("Upload validation failed", extra=error_audit)
        # ========================================
            messagebox.showerror("Error", "\n".join(error_msg))
            return
            
        if not os.path.exists(file_path):
            # =========== 新增：文件不存在审计 ===========
            file_error = audit_data.copy()
            file_error.update({
                "action": "FILE_NOT_FOUND",
                  "file_path": file_path
                  })
            USER_ACTION_LOGGER.error("File not found", extra=file_error)
        # ========================================            
            messagebox.showerror("Error", "File does not exist")
            return
            
        if self.check_duplicate_upload(country, platform, channel, file_path):
            # =========== 新增：重复上传审计 ===========
            dup_audit = audit_data.copy()
            dup_audit["action"] = "DUPLICATE_UPLOAD"
            USER_ACTION_LOGGER.warning("Duplicate upload detected", extra=dup_audit)
        # ========================================            
            messagebox.showwarning("Warning", "Duplicate upload detected")
            return
            
        try:
            self.add_log(f"Uploading {file_path}...")

            # =========== 新增：上传进度审计 ===========
            progress_audit = audit_data.copy()
            def log_progress(message):
                progress_audit["stage"] = message
                USER_ACTION_LOGGER.info("Upload progress", extra=progress_audit)
            # ========================================

            # Simulate upload process
            self.root.after(1000, lambda: [self.add_log("Processing data..."), log_progress("PROCESSING")])
            self.root.after(2000, lambda: [self.add_log("Validation passed"), log_progress("VALIDATING")])
            self.root.after(3000, lambda: [self.add_log("Writing to database..."), log_progress("WRITING_TO_DB")])
            
            # Record upload
            self.record_upload_history(country, platform, channel, file_path, data_type)
            
            # =========== 新增：上传成功审计 ===========
            success_audit = audit_data.copy()
            success_audit.update({
                "user_action": "UPLOAD_SUCCESS",
                "duration_sec": (datetime.now() - upload_start_time).total_seconds(),
                "file_size": os.path.getsize(file_path),
                "file_hash": self.calculate_file_hash(file_path),  # 需要实现哈希计算方法
                "timestamp": datetime.now().isoformat()
                })
            USER_ACTION_LOGGER.info("Upload completed successfully", extra=success_audit)
            # ========================================


            messagebox.showinfo("Success", "Upload completed successfully")
            self.add_log("Upload finished")
        except Exception as e:

            # =========== 新增：上传失败审计 ===========
            error_audit = audit_data.copy()
            error_audit.update({
                "user_action": "UPLOAD_FAILED",
                "error_type": type(e).__name__,
                "error_msg": str(e),
                "duration_sec": (datetime.now() - upload_start_time).total_seconds(),
                "traceback": str(traceback.format_exc())
                })
            USER_ACTION_LOGGER.error("Upload failed with exception", extra=error_audit)
            # ========================================

            messagebox.showerror("Error", f"Upload failed: {str(e)}")
            self.add_log(f"Upload error: {str(e)}")
    
    def calculate_file_hash(self, file_path, algorithm='sha256'):
        hasher = hashlib.new(algorithm)
        with open(file_path, 'rb') as f:
            while True:
                chunk = f.read(65536)  # 64KB chunks
                if not chunk:
                    break
                hasher.update(chunk)
        return hasher.hexdigest()

    def check_duplicate_upload(self, country, platform, channel, file_path):
        return False  # Implement actual duplicate check

    def record_upload_history(self, country, platform, channel, file_path, data_type):
        file_name = os.path.basename(file_path)
        log_msg = f"Uploaded {file_name} to {country}/{platform}/{channel}"
        if data_type:
            log_msg += f" ({data_type})"
        self.add_log(log_msg)

if __name__ == "__main__":
    root = tk.Tk()
    app = FileUploadApp(root)
    root.mainloop()