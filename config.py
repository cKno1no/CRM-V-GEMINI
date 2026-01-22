# config.py
# (PHIÊN BẢN CHUẨN HÓA TOÀN DIỆN - STDD)

import os
import urllib.parse
from datetime import datetime


# Load biến môi trường từ file .env

# =========================================================================
# 1. CẤU HÌNH HẠ TẦNG (INFRASTRUCTURE)
# =========================================================================
DB_SERVER = os.getenv('DB_SERVER')
DB_NAME = os.getenv('DB_NAME')
DB_UID = os.getenv('DB_UID')
DB_PWD = os.getenv('DB_PWD')

APP_SECRET_KEY = os.getenv('APP_SECRET_KEY')
if not APP_SECRET_KEY:
    raise ValueError("LỖI: APP_SECRET_KEY không được thiết lập trong biến môi trường hoặc file .env")
GEMINI_API_KEY = os.getenv('GEMINI_API_KEY')

UPLOAD_FOLDER_PATH = os.path.abspath('attachments')
UPLOAD_FOLDER = 'path/to/your/attachments' # Cần trỏ đúng đường dẫn thực tế trên Server
ALLOWED_EXTENSIONS = {'pdf', 'png', 'jpg', 'jpeg', 'gif', 'docx', 'xlsx', 'pptx', 'txt', 'zip', 'rar'}

# Redis (Real-time)
REDIS_HOST = os.getenv('REDIS_HOST') or 'localhost'
REDIS_PORT = int(os.getenv('REDIS_PORT') or 6379)
REDIS_CHANNEL = 'crm_task_notifications_channel'

# --- CẤU HÌNH KẾT NỐI CSDL (HYBRID) ---

# 1. Chuỗi kết nối gốc (Legacy - dùng cho các script backup hoặc debug)
DB_DRIVER = '{ODBC Driver 17 for SQL Server}' 
CONNECTION_STRING = (
    f"DRIVER={DB_DRIVER};SERVER={DB_SERVER};DATABASE={DB_NAME};"
    f"UID={DB_UID};" f"PWD={DB_PWD};"
)

# 2. Chuỗi kết nối SQLAlchemy (NEW - Dùng cho App chính)
# Mã hóa chuỗi kết nối để tương thích với SQLAlchemy
params = urllib.parse.quote_plus(CONNECTION_STRING)
SQLALCHEMY_DATABASE_URI = f"mssql+pyodbc:///?odbc_connect={params}"

# =========================================================================
# 2. CẤU HÌNH TÀI CHÍNH & KẾ TOÁN (ACCOUNT MAPPING)
# =========================================================================
ACC_DOANH_THU = '511%'        # Tài khoản Doanh thu
ACC_GIA_VON = '632%'          # Tài khoản Giá vốn
ACC_CHI_PHI_BH = '64%'       # Chi phí Bán hàng
ACC_CHI_PHI_QL = '64%'       # Chi phí Quản lý
ACC_CHI_PHI_TC = '635%'       # Chi phí Tài chính
ACC_CHI_PHI_KHAC = '811%'     # Chi phí Khác
ACC_TIEN = '11%'              # Tiền mặt/Ngân hàng (111, 112...)
ACC_PHAI_THU_KH = '13111'     # Phải thu khách hàng
ACC_PHAI_TRA_NB = '331%' # Dùng cho các truy vấn ad-hoc nếu có
ORDER_TYPE_STOCK = ['SO', 'SIG']  # Hàng có sẵn
ORDER_TYPE_ORDER = ['DDH']        # Hàng đặt


# Các mã loại trừ
EXCLUDE_ANA03_CP2014 = 'cp2014' # Mã phân tích chi phí cần loại trừ (Kết chuyển)

# =========================================================================
# 3. CẤU HÌNH NGƯỠNG & HẠN MỨC (THRESHOLDS & LIMITS)
# =========================================================================
# Đơn vị hiển thị
DIVISOR_VIEW = 1000000.0      # Chia cho 1 triệu để hiển thị (M)

# Ngưỡng phân loại Khách hàng & Rủi ro
LIMIT_SMALL_CUSTOMER = 20000000.0  # 20 Triệu - Dưới mức này là khách nhỏ lẻ
RISK_INVENTORY_VALUE = 5000000.0   # 5 Triệu - Tồn kho lâu năm > mức này mới tính là CLC
RISK_DEBT_VALUE = 2000000.0           # 1 Nghìn - Nợ nhỏ hơn mức này bỏ qua

# Ngưỡng Phê duyệt (Approval Logic)
LIMIT_AUTO_APPROVE_DTK = 100000000.0 # 100 Triệu - DTK dưới mức này tự duyệt
LIMIT_AUTO_APPROVE_SO = 20000000.0   # 20 Triệu - Đơn hàng dưới mức này tự duyệt

# Tỷ số duyệt (Approval Ratios)
RATIO_REQ_CLASS_M = 150 # Yêu cầu 150% cho khách loại M
RATIO_REQ_CLASS_T = 138 # Yêu cầu 138% cho khách loại T

# [NEW] Cấu hình Chống Gian lận (Anti-Fraud) cho DDH
# Nếu (Tổng Tồn Kho / Tổng Đặt Hàng) > 30% -> Báo lỗi gian lận
DDH_FRAUD_THRESHOLD = 30.0

# =========================================================================
# 4. CẤU HÌNH CROSS-SELL & KPI
# =========================================================================
CROSS_SELL_LOW_MARGIN = 10.0       # Dưới 10% là biên lợi nhuận thấp
NEW_BUSINESS_DAYS = 360            # 360 ngày coi là khách mới
NEW_BUSINESS_MIN_SALES = 10000000.0 # Doanh số tối thiểu để tính là New Business thực

# =========================================================================
# 5. CẤU HÌNH TRẠNG THÁI (CONSTANTS)
# =========================================================================
# Task Status
TASK_STATUS_BLOCKED = 'BLOCKED'
TASK_STATUS_HELP = 'HELP_NEEDED'
TASK_STATUS_COMPLETED = 'COMPLETED'
TASK_STATUS_PENDING = 'PENDING'
TASK_STATUS_OPEN = 'OPEN'

# Delivery Status
DELIVERY_STATUS_DONE = 'Da Giao'

# Date Formats
DATE_FORMAT_DISPLAY = '%d/%m/%Y'  # Hiển thị lên web
DATE_FORMAT_DB = '%Y-%m-%d'       # Lưu xuống DB / So sánh
DATETIME_FORMAT_DISPLAY = '%d/%m/%Y %H:%M'

# =========================================================================
# 6. CẤU HÌNH PHÂN QUYỀN & TỔ CHỨC (ROLES & DEPTS)
# =========================================================================
# Vai trò (Role)
ROLE_ADMIN = 'ADMIN'
ROLE_GM = 'GM'
ROLE_MANAGER = 'MANAGER'
ROLE_SALES = 'SALES'

# Mã Phòng ban (Đồng bộ với dữ liệu trong [GD - NGUOI DUNG])
DEPT_KHO = '5.KHO'
DEPT_THUKY = '3.THUKY'       # Xóa khoảng trắng
DEPT_KTTC = '6.KTTC'
DEPT_KINHDOANH = '2.KINHDOANH' # Xóa khoảng trắng

# --- CẤU HÌNH BẢO MẬT CUSTOMER 360 ---
CUSTOMER_360_VIEW_LIMIT = 7  # Giới hạn số lần xem/ngày
# =========================================================================
# 7. CẤU HÌNH DATABASE OBJECTS (TABLES, VIEWS, SPs)
# =========================================================================

# --- A. BẢNG HỆ THỐNG (CRM_STDD) ---
# (Các biến này KHÔNG CÓ [dbo]. vì app.py đang xử lý thêm thủ công,
# tuy nhiên các Service mới sẽ dùng trực tiếp nên cần lưu ý khi gọi)
# Bảng Hệ thống mới
TABLE_SYS_PERMISSIONS = '[dbo].[SYS_PERMISSIONS]'

# Cấu trúc: 'TÊN NHÓM': { 'MÃ_QUYỀN': 'Tên hiển thị' }
SYSTEM_FEATURES_GROUPS = {
    "1. QUẢN TRỊ & HỆ THỐNG": {
        'MANAGE_USER': 'Quản lý User & Phân quyền',
        'VIEW_CEO_COCKPIT': 'Xem CEO Cockpit (Tổng hợp)',
        'VIEW_COMPARISON': 'Phân tích So sánh Kỳ này/Kỳ trước',
        # --- Đảm bảo 3 dòng này có mặt ---
        'THEME_DARK': 'Kích hoạt: Giao diện Tối (Dark Mode)',
        'THEME_FANTASY': 'Kích hoạt: Giao diện Fantasy (Sci-fi)',
        'THEME_ADORABLE': 'Kích hoạt: Giao diện Adorable (GenZ)',
        'VIEW_PROFILE': 'Xem Hồ sơ & Góc Flex', # <--- THÊM DÒNG NÀY
        'USE_CHATBOT': 'Sử dụng Trợ lý AI'
    },
    "2. CRM & BÁO CÁO THỊ TRƯỜNG": {
        'VIEW_PORTAL': 'Truy cập Portal Cá nhân',
        'CREATE_REPORT': 'Tạo Báo cáo Khách hàng',
        'VIEW_REPORT_LIST': 'Xem Dashboard Danh sách Báo cáo',
        'VIEW_ALL_REPORTS': 'Xem Báo cáo của Tất cả nhân viên (Manager)',
        'CREATE_CONTACT': 'Tạo mới Nhân sự liên hệ'
    },
    "3. KPI & HIỆU SUẤT KINH DOANH": {
        'VIEW_SALES_DASHBOARD': 'Xem Bảng Tổng hợp Hiệu suất Sales',
        'VIEW_REALTIME_KPI': 'Xem Dashboard Sales Real-time',
        'VIEW_PROFIT_ANALYSIS': 'Phân tích Lợi nhuận gộp',
        'VIEW_CROSS_SELL': 'Phân tích Bán chéo (Cross-sell DNA)',
        'VIEW_SALES_LOOKUP': 'Tra cứu Thông tin Bán hàng (Giá/Tồn)',
        'VIEW_CUSTOMER_360': 'xem trang khách hàng 360'
    },
    "4. QUYỀN PHÊ DUYỆT (APPROVAL)": {
        'APPROVE_QUOTE': 'Duyệt Báo giá',
        'APPROVE_ORDER': 'Duyệt Đơn hàng Bán',
        'VIEW_QUICK_APPROVAL': 'Truy cập Form Duyệt Nhanh',
        'OVERRIDE_QUOTE_COST': 'Sửa giá vốn (Cost) Báo giá',
        'CHANGE_QUOTE_SALESMAN': 'Đổi NVKD cho Báo giá'
    },
    "5. TÀI CHÍNH & NGÂN SÁCH": {
        'VIEW_BUDGET': 'Xem Ngân sách & Tạo Đề nghị',
        'APPROVE_BUDGET': 'Duyệt Đề nghị Thanh toán',
        'EXECUTE_PAYMENT': 'Thực chi / Lệnh chi (Kế toán)',
        'VIEW_BUDGET_REPORT': 'Xem Báo cáo Ngân sách YTD',
        'CREATE_COMMISSION': 'Tạo Đề xuất Hoa hồng',
        'VIEW_AR_AGING': 'Xem Công nợ Phải thu (AR)',
        'VIEW_AP_AGING': 'Xem Nợ Phải trả (AP)'
    },
    "6. KHO VẬN & CHUỖI CUNG ỨNG": {
        'VIEW_INVENTORY_AGING': 'Phân tích Tuổi hàng Tồn kho',
        'VIEW_TOTAL_REPLENISH': 'Xem Dự báo Dự phòng Tổng thể',
        'VIEW_CUST_REPLENISH': 'Xem Dự báo Dự phòng theo KH',
        'VIEW_DELIVERY': 'Xem Bảng Điều phối Giao vận',
        'PLAN_DELIVERY': 'Lập Kế hoạch Giao (Thư ký)',
        'DISPATCH_DELIVERY': 'Thực thi Xuất hàng (Thủ kho)'
    },
    "7. QUẢN LÝ CÔNG VIỆC (TASK)": {
        'VIEW_TASK': 'Sử dụng Task Dashboard',
        'VIEW_TASK_TEAM': 'Xem Task của nhân viên cấp dưới'
    }
}

TEN_BANG_BAO_CAO = '[HD_BAO CAO]'       
TEN_BANG_NGUOI_DUNG = '[GD - NGUOI DUNG]'
TEN_BANG_KHACH_HANG = '[HD_KHACH HANG]' 
TEN_BANG_LOAI_BAO_CAO = '[GD - LOAI BAO CAO]'
TEN_BANG_NOI_DUNG_HD = '[NOI DUNG HD]'
TEN_BANG_NHAN_SU_LH = '[HD_NHAN SU LIEN HE]'
TEN_BANG_GIAI_TRINH = '[GIAI TRINH]'
TEN_BANG_CAP_NHAT_BG = '[HD_CAP NHAT BAO GIA]'
ERP_APPROVER_MASTER = '[OT0006]' 

# Các bảng có Schema đầy đủ (Dùng trong Service)
TASK_TABLE = 'dbo.Task_Master'
TASK_LOG_TABLE = 'dbo.Task_Progress_Log'
BOSUNG_CHAOGIA_TABLE = 'dbo.BOSUNG_CHAOGIA'
CRM_DTCL = '[dbo].[DTCL]' 
LOG_DUYETCT_TABLE = 'DUYETCT' 
LOG_AUDIT_TABLE = 'dbo.AUDIT_LOGS'

# Bảng Ngân sách & Hoa hồng
TABLE_BUDGET_MASTER = 'dbo.BUDGET_MASTER'
TABLE_BUDGET_PLAN = 'dbo.BUDGET_PLAN'
TABLE_EXPENSE_REQUEST = 'dbo.EXPENSE_REQUEST'

# --- B. BẢNG ERP (OMEGA_STDD / OMEGA_TEST) ---
ERP_DB = '[OMEGA_STDD]' # <-- Đang dùng bản TEST theo file gốc
ERP_GIAO_DICH = f'{ERP_DB}.[dbo].[GT9000]'        
ERP_SALES_DETAIL = f'{ERP_DB}.[dbo].[OT2002]'
ERP_OT2001 = f'{ERP_DB}.[dbo].[OT2001]' # Sales Order Header
ERP_QUOTES = f'{ERP_DB}.[dbo].[OT2101]' # Quotation Header
ERP_QUOTE_DETAILS = f'{ERP_DB}.[dbo].[OT2102]'
ERP_IT1202 = f'{ERP_DB}.[dbo].[IT1202]' # Khách hàng ERP
ERP_IT1302 = f'{ERP_DB}.[dbo].[IT1302]' # Vật tư ERP
ERP_ITEM_PRICING = f'{ERP_DB}.[dbo].[IT1302]' # (Alias)
ERP_GENERAL_LEDGER = f'{ERP_DB}.[dbo].[GT9000]'
ERP_GOODS_RECEIPT_MASTER = f'{ERP_DB}.[dbo].[WT2006]' 
ERP_GOODS_RECEIPT_DETAIL = f'{ERP_DB}.[dbo].[WT2007]'
ERP_DELIVERY_DETAIL = f'{ERP_DB}.[dbo].[OT2302]' 
ERP_DELIVERY_MASTER = f'{ERP_DB}.[dbo].[OT2301]' 

# --- C. VIEWS ---
CRM_AR_AGING_SUMMARY = '[dbo].[CRM_AR_AGING_SUMMARY]' # Dùng bản HN
CRM_AP_AGING_SUMMARY = '[dbo].[CRM_AP_AGING_SUMMARY]'

DELIVERY_WEEKLY_VIEW = '[dbo].[Delivery_Weekly]'
VIEW_BACK_ORDER = f'{ERP_DB}.[dbo].[CRM_TON KHO BACK ORDER]'
VIEW_BACK_ORDER_DETAIL = f'{ERP_DB}.[dbo].[CRM_BACKORDER]'
CRM_VIEW_DHB_FULL = f'{ERP_DB}.[dbo].[CRM_TV_THONG TIN DHB_FULL]'
CRM_VIEW_DHB_FULL_2 = f'{ERP_DB}.[dbo].[CRM_TV_THONG TIN DHB_FULL 2]'
# [NEW] View Tổng hợp Dòng chảy Kinh doanh (Sales Flow) cho Chatbot
VIEW_CHATBOT_SALES_FLOW = "[dbo].[View_Chatbot_SalesFlow_Summary]"


# --- D. STORED PROCEDURES (SP) ---
# (Ánh xạ tên chuẩn -> Tên thực tế trong DB HN)
SP_GET_SALES_LOOKUP = 'dbo.sp_GetSalesLookup_Block1'
SP_GET_REALTIME_KPI = 'dbo.sp_GetRealtimeSalesKPI'
# 1. Dùng cho trang Chi tiết (Inventory Aging Page) -> Trỏ vào SP mới tạo ở Bước 1
SP_GET_INVENTORY_AGING = 'dbo.sp_GetInventoryAging_Detail_Cache'

# 2. [THÊM MỚI] Dùng cho Dashboard (CEO Cockpit) -> Trỏ vào SP cộng gộp bạn đã tạo trước đó
SP_GET_INVENTORY_AGING_SUMMARY = 'dbo.sp_GetInventoryAging_Cache'
SP_SALES_PERFORMANCE = 'dbo.sp_GetSalesPerformanceSummary'      # Cần kiểm tra xem có bản _HN không
SP_CROSS_SELL_GAP = 'dbo.sp_GetCustomerReplenishmentSuggest'    # Cần kiểm tra xem có bản _HN không
SP_AR_AGING_DETAIL = 'dbo.sp_GetARAgingDetail'                  # Cần kiểm tra xem có bản _HN không
SP_REPLENISH_TOTAL = 'dbo.sp_GetTotalReplenishmentNeeds'        
SP_REPLENISH_GROUP = 'dbo.sp_GetReplenishmentGroupDetails'
# Phân tích Lợi nhuận (Dùng trong sales_service.get_profit_analysis)
SP_SALES_GROSS_PROFIT = 'dbo.sp_GetSalesGrossProfit_Analysis' 
SP_AP_AGING_DETAIL = 'dbo.sp_GetAPAgingDetail'
# Tính Hoa hồng (Dùng trong commission_service)
SP_CREATE_COMMISSION = 'dbo.sp_CreateCommissionProposal'

# Widget Portal (Dùng trong portal_service để hiện gợi ý dự phòng ngoài trang chủ)
SP_REPLENISH_PORTAL = 'dbo.sp_GetPortalReplenishment'

# --- E. BẢNG GAMIFICATION & PROFILE (TITAN OS) ---
TABLE_TITAN_ITEMS = '[dbo].[TitanOS_SystemItems]'
TABLE_TITAN_PROFILE = '[dbo].[TitanOS_UserProfile]'
TABLE_TITAN_INVENTORY = '[dbo].[TitanOS_UserInventory]'

# =========================================================================
# 3. BỔ SUNG: JOB & SP HỖ TRỢ (Dành cho Scheduled Tasks hoặc Admin Tool)
# =========================================================================
SP_JOB_UPDATE_AR_AGING = 'dbo.sp_UpdateARAgingSummary'       # Job cập nhật Công nợ
SP_JOB_CALC_VELOCITY = 'dbo.sp_CalculateAllSalesVelocity'    # Job tính tốc độ bán
SP_SALES_LOOKUP_COMMON = 'dbo.sp_GetSalesLookup_Common'      # Tra cứu chung (Dự phòng)

QUOTE_STATUS_PENDING = 'CHỜ'
QUOTE_STATUS_WIN = 'WIN'
QUOTE_STATUS_LOST = 'LOST'
QUOTE_STATUS_CANCEL = 'CANCEL'
QUOTE_STATUS_DELAY = 'DELAY'

# Ngưỡng Rủi ro & KPI Báo giá
QUOTE_RISK_DELAY_DAYS = 10          # Cảnh báo nếu trễ > 10 ngày
QUOTE_RISK_NO_ACTION_DAYS = 5       # Cảnh báo nếu không có hành động > 5 ngày
QUOTE_RISK_AVG_VALUE = 30000000.0   # Giá trị trung bình (30 Triệu)

# Trong config.py, thêm vào phần 7 (Cấu hình Database Objects)
TABLE_COMMISSION_MASTER = '[dbo].[DE XUAT BAO HANH_MASTER]'
TABLE_COMMISSION_DETAIL = '[dbo].[DE XUAT BAO HANH_DS]'