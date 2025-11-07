# config.py

import os
from datetime import datetime
# from dotenv import load_dotenv  # <--- THÊM IMPORT NÀY

# Tải các biến môi trường từ file .env (nếu có - dùng cho development)
# Nó sẽ tự động tìm file .env cùng cấp.
#load_dotenv()  # <--- THÊM DÒNG NÀY Ở ĐẦU
# =========================================================================
# CẤU HÌNH ỨNG DỤNG VÀ UPLOAD
# =========================================================================
# BƯỚC 1: BỎ load_dotenv()

# BƯỚC 2: Đọc các biến trực tiếp từ môi trường OS
DB_SERVER = os.getenv('DB_SERVER')
DB_NAME = os.getenv('DB_NAME') # Giả định DB_NAME được cấu hình ở đâu đó
DB_UID = os.getenv('DB_UID')
DB_PWD = os.getenv('DB_PWD')

APP_SECRET_KEY = os.getenv('APP_SECRET_KEY')
if not APP_SECRET_KEY:
    raise ValueError("LỖI: APP_SECRET_KEY không được thiết lập trong biến môi trường hoặc file .env")
UPLOAD_FOLDER_PATH = os.path.abspath('attachments')
UPLOAD_FOLDER = 'path/to/your/attachments'
ALLOWED_EXTENSIONS = {'pdf', 'png', 'jpg', 'jpeg', 'gif', 'docx', 'xlsx', 'pptx', 'txt', 'zip', 'rar'}
# =========================================================================
# CẤU HÌNH REAL-TIME NOTIFICATION (REDIS)
# =========================================================================
# Lấy từ biến môi trường hoặc dùng mặc định
REDIS_HOST = os.getenv('REDIS_HOST') or 'localhost'
REDIS_PORT = int(os.getenv('REDIS_PORT') or 6379)
REDIS_CHANNEL = 'crm_task_notifications_channel' # Kênh thông báo chính
# =========================================================================
# CẤU HÌNH DATABASE
# =========================================================================
DB_DRIVER = '{ODBC Driver 17 for SQL Server}' 
# DB_SERVER = r'113.161.43.96,1433'         
# DB_DATABASE = 'CRM_STDD'                 
# DB_USER = 'sa'
# === PHẦN THAY ĐỔI QUAN TRỌNG ===
# Đọc DB_PASSWORD từ biến môi trường (đã được load_dotenv() nạp vào)
# DB_PASSWORD = os.environ.get('DB_PASSWORD')                                        
# Kiểm tra xem mật khẩu có tồn tại không
if not DB_PWD:
    raise ValueError("LỖI: DB_PASSWORD không được thiết lập trong biến môi trường hoặc file .env")
# === KẾT THÚC PHẦN THAY ĐỔI ===
CONNECTION_STRING = (
    f"DRIVER={DB_DRIVER};" f"SERVER={DB_SERVER};" f"DATABASE={DB_NAME};"
    f"UID={DB_UID};" f"PWD={DB_PWD};" f"Timeout=10;"
)

# =========================================================================
# TÊN BẢNG HỆ THỐNG (CRM)
# =========================================================================
TEN_BANG_BAO_CAO = '[HD_BAO CAO]'       
TEN_BANG_NGUOI_DUNG = '[GD - NGUOI DUNG]'
TEN_BANG_KHACH_HANG = '[HD_KHACH HANG]' 
TEN_BANG_LOAI_BAO_CAO = '[GD - LOAI BAO CAO]'
TEN_BANG_NOI_DUNG_HD = '[NOI DUNG HD]'  
TEN_BANG_NHAN_SU_LH = '[HD_NHAN SU LIEN HE]' 
TEN_BANG_GIAI_TRINH = '[GIAI TRINH]' 
CRM_DTCL = '[CRM_STDD].[dbo].[DTCL]' # Bảng Đăng ký Doanh số
TEN_BANG_CAP_NHAT_BG = '[HD_CAP NHAT BAO GIA]' # <--- BẢNG MỚI
ERP_APPROVER_MASTER = '[OT0006]' # Master người duyệt theo loại chứng từ
CRM_BACK_ORDER_VIEW = '[OMEGA_STDD].[dbo].[CRM_TON KHO BACK ORDER]'
# CẤU HÌNH CHO TASK MANAGEMENT MODULE
TASK_TABLE = 'dbo.Task_Master'
BOSUNG_CHAOGIA_TABLE = 'dbo.BOSUNG_CHAOGIA'
# =========================================================================
# TÊN BẢNG ERP (OMEGA_STDD)
# =========================================================================
ERP_DB = '[OMEGA_STDD]'
ERP_GIAO_DICH = f'{ERP_DB}.[dbo].[GT9000]'        
ERP_SALES_DETAIL = f'{ERP_DB}.[dbo].[OT2002]'
ERP_OT2001 = f'{ERP_DB}.[dbo].[OT2001]' # Sales Order Header
ERP_QUOTES = f'{ERP_DB}.[dbo].[OT2101]' # <--- BẢNG BÁO GIÁ (OT2101)
ERP_IT1202 = f'{ERP_DB}.[dbo].[IT1202]' # Khách hàng ERP
ERP_IT1302 = f'{ERP_DB}.[dbo].[IT1302]' # Mặt hàng ERP (Giả định)
ERP_QUOTE_DETAILS = f'{ERP_DB}.[dbo].[OT2102]'  # Chi tiết báo giá (Giả định)
ERP_ITEM_PRICING = f'{ERP_DB}.[dbo].[IT1302]'
ERP_GENERAL_LEDGER = '[OMEGA_STDD].[dbo].[GT9000]'

ERP_ITEM_PRICING = '[OMEGA_STDD].[dbo].[IT1302]'
ERP_GOODS_RECEIPT_MASTER = '[OMEGA_STDD].[dbo].[WT2006]' 
ERP_GOODS_RECEIPT_DETAIL = '[OMEGA_STDD].[dbo].[WT2007]'
# ERP_CUSTOMER_AR = f'{ERP_DB}.[dbo].[AR_BALANCE]' # Đã bỏ qua theo yêu cầu