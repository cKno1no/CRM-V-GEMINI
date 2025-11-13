# config.py
# (PHIÊN BẢN GIAI ĐOẠN 1 - ĐÃ SỬA LỖI TƯƠNG THÍCH NGƯỢC VÀ LỖI 42S02)

import os
from datetime import datetime

# =========================================================================
# CẤU HÌNH ỨNG DỤNG VÀ UPLOAD
# =========================================================================
DB_SERVER = os.getenv('DB_SERVER')
DB_NAME = os.getenv('DB_NAME')
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
REDIS_HOST = os.getenv('REDIS_HOST') or 'localhost'
REDIS_PORT = int(os.getenv('REDIS_PORT') or 6379)
REDIS_CHANNEL = 'crm_task_notifications_channel'

# =========================================================================
# CẤU HÌNH DATABASE
# =========================================================================
DB_DRIVER = '{ODBC Driver 17 for SQL Server}' 
CONNECTION_STRING = (
    f"DRIVER={DB_DRIVER};" f"SERVER={DB_SERVER};" f"DATABASE={DB_NAME};"
    f"UID={DB_UID};" f"PWD={DB_PWD};" f"Timeout=10;"
)

# =========================================================================
# TÊN BẢNG HỆ THỐNG (CSDL CHÍNH: CRM_STDD)
# (Các biến này KHÔNG CÓ [dbo]. vì app.py đang thêm thủ công)
# =========================================================================
TEN_BANG_BAO_CAO = '[HD_BAO CAO]'       
TEN_BANG_NGUOI_DUNG = '[GD - NGUOI DUNG]'
TEN_BANG_KHACH_HANG = '[HD_KHACH HANG]' 
TEN_BANG_LOAI_BAO_CAO = '[GD - LOAI BAO CAO]'
TEN_BANG_NOI_DUNG_HD = '[NOI DUNG HD]'  # <-- SỬA LỖI 42S02 TẠI ĐÂY
TEN_BANG_NHAN_SU_LH = '[HD_NHAN SU LIEN HE]' # <-- SỬA LỖI 42S02 TẠI ĐÂY
TEN_BANG_GIAI_TRINH = '[GIAI TRINH]' # <-- SỬA LỖI 42S02 TẠI ĐÂY
TEN_BANG_CAP_NHAT_BG = '[HD_CAP NHAT BAO GIA]'
ERP_APPROVER_MASTER = '[OT0006]' # Master người duyệt

# (Các biến này CÓ [dbo]. vì service/db_manager gọi trực tiếp)
TASK_TABLE = 'dbo.Task_Master'
TASK_LOG_TABLE = 'dbo.Task_Progress_Log' # <-- BẢNG MỚI CHO LỊCH SỬ TIẾN ĐỘ
BOSUNG_CHAOGIA_TABLE = 'dbo.BOSUNG_CHAOGIA'
CRM_DTCL = '[dbo].[DTCL]' # Bảng Đăng ký Doanh số
LOG_DUYETCT_TABLE = 'DUYETCT' # (Không có dbo, theo code service)
LOG_AUDIT_TABLE = 'dbo.AUDIT_LOGS'

# =========================================================================
# TÊN BẢNG ERP (CSDL PHỤ: OMEGA_STDD)
# (Giữ tên biến cũ)
# =========================================================================
ERP_DB = '[OMEGA_STDD]'
ERP_GIAO_DICH = f'{ERP_DB}.[dbo].[GT9000]'        
ERP_SALES_DETAIL = f'{ERP_DB}.[dbo].[OT2002]'
ERP_OT2001 = f'{ERP_DB}.[dbo].[OT2001]' # Sales Order Header
ERP_QUOTES = f'{ERP_DB}.[dbo].[OT2101]'
ERP_QUOTE_DETAILS = f'{ERP_DB}.[dbo].[OT2102]'
ERP_IT1202 = f'{ERP_DB}.[dbo].[IT1202]' # Khách hàng ERP
ERP_IT1302 = f'{ERP_DB}.[dbo].[IT1302]' # Mặt hàng ERP
ERP_ITEM_PRICING = f'{ERP_DB}.[dbo].[IT1302]' # (Alias)
ERP_GENERAL_LEDGER = f'{ERP_DB}.[dbo].[GT9000]'
ERP_GOODS_RECEIPT_MASTER = f'{ERP_DB}.[dbo].[WT2006]' 
ERP_GOODS_RECEIPT_DETAIL = f'{ERP_DB}.[dbo].[WT2007]'
ERP_DELIVERY_DETAIL = f'{ERP_DB}.[dbo].[OT2302]' # Chi tiết Lệnh xuất hàng

# =========================================================================
# TÊN VIEW (Views)
# =========================================================================
# 1. View Hệ thống (CRM_STDD)
CRM_AR_AGING_SUMMARY = '[dbo].[CRM_AR_AGING_SUMMARY]'
DELIVERY_WEEKLY_VIEW = '[dbo].[Delivery_Weekly]'

# 2. View ERP (OMEGA_STDD)
VIEW_BACK_ORDER = f'{ERP_DB}.[dbo].[CRM_TON KHO BACK ORDER]' # Dùng cho SUM(con)
VIEW_BACK_ORDER_DETAIL = f'{ERP_DB}.[dbo].[CRM_BACKORDER]' # Dùng cho chi tiết Modal
CRM_VIEW_DHB_FULL = f'{ERP_DB}.[dbo].[CRM_TV_THONG TIN DHB_FULL]'
CRM_VIEW_DHB_FULL_2 = f'{ERP_DB}.[dbo].[CRM_TV_THONG TIN DHB_FULL 2]'

# =========================================================================
# TÊN STORED PROCEDURE (SP)
# =========================================================================
SP_GET_SALES_LOOKUP = 'dbo.sp_GetSalesLookup_Block1'
SP_GET_REALTIME_KPI = 'dbo.sp_GetRealtimeSalesKPI'
SP_GET_INVENTORY_AGING = 'dbo.sp_GetInventoryAging'