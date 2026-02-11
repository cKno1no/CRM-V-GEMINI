# factory.py
from flask import current_app
from flask import Flask, session, current_app, request
from datetime import timedelta
import os
import redis
import config
from flask_session import Session
# [NEW] Import Logger Setup
from logger_setup import setup_production_logging
from flask_caching import Cache  # <--- IMPORT MỚI


# 1. Import DB Manager & Services
from db_manager import DBManager
from sales_service import SalesService, InventoryService
from customer_service import CustomerService
from quotation_approval_service import QuotationApprovalService
from sales_order_approval_service import SalesOrderApprovalService
from services.sales_lookup_service import SalesLookupService
from services.task_service import TaskService
from services.chatbot_service import ChatbotService
from services.ar_aging_service import ARAgingService
from services.delivery_service import DeliveryService
from services.budget_service import BudgetService
from services.executive_service import ExecutiveService
from services.cross_sell_service import CrossSellService
from services.ap_aging_service import APAgingService
from services.commission_service import CommissionService
# [FIX] Thêm import PortalService
from services.portal_service import PortalService
from services.user_service import UserService
from services.customer_analysis_service import CustomerAnalysisService
from services.gamification_service import GamificationService
from services.training_service import TrainingService  # <--- [THÊM MỚI]

# 2. Import Blueprints
from blueprints.crm_bp import crm_bp
from blueprints.kpi_bp import kpi_bp
from blueprints.portal_bp import portal_bp
from blueprints.approval_bp import approval_bp
from blueprints.delivery_bp import delivery_bp
from blueprints.task_bp import task_bp
from blueprints.chat_bp import chat_bp
from blueprints.lookup_bp import lookup_bp
from blueprints.budget_bp import budget_bp
from blueprints.commission_bp import commission_bp
from blueprints.executive_bp import executive_bp
from blueprints.cross_sell_bp import cross_sell_bp
from blueprints.ap_bp import ap_bp
from blueprints.user_bp import user_bp
from blueprints.customer_analysis_bp import customer_analysis_bp
from blueprints.training_bp import training_bp         # <--- [THÊM MỚI]
# Khởi tạo đối tượng Cache (chưa gắn app)
cache = Cache()

def create_app():
    """Nhà máy khởi tạo ứng dụng Flask"""
    app = Flask(__name__, static_url_path='/static', static_folder='static')
    
    # [NEW] KÍCH HOẠT LOGGING NGAY TẠI ĐÂY
    setup_production_logging(app)
    # Cấu hình App
    app.secret_key = config.APP_SECRET_KEY

    # --- CẤU HÌNH SERVER-SIDE SESSION (FIX LỖI COOKIE TRÀN) ---
    # CẤU HÌNH SESSION HARD TIMEOUT (3 GIỜ)
    # =========================================================
    app.config['SESSION_TYPE'] = 'redis'
    app.config['SESSION_PERMANENT'] = True  # Bắt buộc True để dùng Lifetime
    app.config['SESSION_USE_SIGNER'] = True
    
    # Cấu hình Redis cho Session (DB 1)
    app.config['SESSION_REDIS'] = redis.Redis(host=config.REDIS_HOST, port=config.REDIS_PORT, db=1)
    
    # 1. Thời gian sống của Session: 6 Tiếng
    app.config['PERMANENT_SESSION_LIFETIME'] = timedelta(hours=6)
    
    # 2. [QUAN TRỌNG] Tắt chế độ tự động gia hạn
    # False: Thời gian đếm ngược KHÔNG được reset khi user thao tác.
    # User login lúc 8:00 -> Đúng 11:00 sẽ bị đá ra, dù lúc 10:59 đang bấm nút.
    app.config['SESSION_REFRESH_EACH_REQUEST'] = False 

    # 3. Cấu hình Cookie (Để tránh lỗi đăng nhập chập chờn)
    app.config['SESSION_COOKIE_NAME'] = 'titan_session'
    app.config['SESSION_COOKIE_HTTPONLY'] = True
    app.config['SESSION_COOKIE_SECURE'] = False # Đặt True nếu web chạy https
    app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'
    
    # Khởi tạo Session Interface
    Session(app)

    app.config['UPLOAD_FOLDER'] = config.UPLOAD_FOLDER_PATH
    app.config['PERMANENT_SESSION_LIFETIME'] = timedelta(hours=3)

    # Route phục vụ file đính kèm
    from flask import send_from_directory
    @app.route('/attachments/<path:filename>')
    def serve_attachments(filename):
        return send_from_directory(config.UPLOAD_FOLDER_PATH, filename)

    # Khởi tạo Redis
    try:
        redis_client = redis.Redis(host=config.REDIS_HOST, port=config.REDIS_PORT, db=0, decode_responses=True)
        redis_client.ping()
    except Exception as e:
        app.logger.error(f"Redis connection failed: {e}")
        redis_client = None

    # --- CẤU HÌNH CACHE VỚI REDIS ---
    app.config['CACHE_TYPE'] = 'RedisCache'
    app.config['CACHE_REDIS_HOST'] = config.REDIS_HOST
    app.config['CACHE_REDIS_PORT'] = config.REDIS_PORT
    app.config['CACHE_REDIS_DB'] = 2  # Dùng DB số 2 (để tách biệt với Session DB 1)
    app.config['CACHE_DEFAULT_TIMEOUT'] = 300  # Mặc định cache 5 phút
    app.config['CACHE_KEY_PREFIX'] = 'titan_cache_' # Tiền tố để dễ quản lý key

    # Kích hoạt Cache cho App
    cache.init_app(app)
    
    # Gắn cache vào app để có thể gọi từ nơi khác (current_app.cache)
    app.cache = cache

    # 3. KHỞI TẠO SERVICES (DEPENDENCY INJECTION)
    db_manager = DBManager()
    
    # Gắn DB và Redis vào app
    app.db_manager = db_manager
    app.redis_client = redis_client

    # Khởi tạo các Service và gắn vào app
    app.sales_service = SalesService(db_manager)
    app.inventory_service = InventoryService(db_manager)
    app.customer_service = CustomerService(db_manager)
    app.approval_service = QuotationApprovalService(db_manager)
    app.order_approval_service = SalesOrderApprovalService(db_manager)
    app.lookup_service = SalesLookupService(db_manager)
    app.task_service = TaskService(db_manager)
    app.ar_aging_service = ARAgingService(db_manager)
    app.delivery_service = DeliveryService(db_manager)
    app.budget_service = BudgetService(db_manager)
    app.executive_service = ExecutiveService(db_manager)
    app.cross_sell_service = CrossSellService(db_manager)
    app.ap_aging_service = APAgingService(db_manager)
    app.commission_service = CommissionService(db_manager)
    app.customer_analysis_service = CustomerAnalysisService(app.db_manager, app.redis_client)
    
    # [FIX] Khởi tạo và gắn PortalService
    app.portal_service = PortalService(db_manager)
    app.user_service = UserService(db_manager)
    app.gamification_service = GamificationService(db_manager)
    # [THÊM MỚI] Khởi tạo Training Service và gắn vào App
    app.training_service = TrainingService(db_manager, app.gamification_service)
    app.chatbot_service = ChatbotService(
        app.lookup_service,
        app.customer_service,
        app.delivery_service,
        app.task_service,    # <--- THÊM DÒNG NÀY
        app.config,
        db_manager           # <--- THÊM DÒNG NÀY (để query trực tiếp)
    )

    # 4. ĐĂNG KÝ BLUEPRINTS
    app.register_blueprint(portal_bp)
    app.register_blueprint(crm_bp)
    app.register_blueprint(kpi_bp)
    app.register_blueprint(approval_bp)
    app.register_blueprint(delivery_bp)
    app.register_blueprint(task_bp)
    app.register_blueprint(chat_bp)
    app.register_blueprint(lookup_bp)
    app.register_blueprint(budget_bp)
    app.register_blueprint(commission_bp)
    app.register_blueprint(executive_bp)
    app.register_blueprint(cross_sell_bp)
    app.register_blueprint(ap_bp)
    app.register_blueprint(user_bp)
    app.register_blueprint(customer_analysis_bp) # Đường dẫn mặc định sẽ theo định nghĩa trong bp
    app.register_blueprint(training_bp) # <--- [THÊM MỚI] Đăng ký đường dẫn /training
    # 5. Inject User Context
   
    @app.context_processor
    # 5. Inject User Context (ĐÃ FIX LỖI)
    def inject_user():
        def check_permission(feature_code):
            user_role = session.get('user_role', '').strip().upper()
            if user_role == config.ROLE_ADMIN: return True
            permissions = session.get('permissions', [])
            return feature_code in permissions

        # Dữ liệu mặc định (Tránh lỗi NoneType khi chưa login)
        user_code = session.get('user_code')
        user_data_combined = {
            'Level': 1, 'CurrentXP': 0, 'TotalCoins': 0, 
            'NextLevelXP': 100, 'ProgressPercent': 0,
            'AvatarUrl': '', 'ThemeColor': 'light', 
            'EquippedPet': '', 'Title': 'Newbie'
        }
        unlocked_themes = ['light'] # Mặc định luôn có Light

        # [NEW] LẤY DỮ LIỆU TỪ DB NẾU ĐÃ LOGIN
        if user_code:
            try:
                # 1. Gọi Service lấy Full Profile (Gộp cả Stats + Visuals)
                # Hàm này đã có cơ chế "Self-healing" (Tự tạo data nếu thiếu)
                profile_data = current_app.user_service.get_user_profile(user_code)
                
                if profile_data:
                    user_data_combined.update(profile_data)
                    
                    # [LOGIC MỚI] Ưu tiên Nickname
                    if profile_data.get('Nickname'):
                        # Ghi đè SHORTNAME hiển thị bằng Nickname
                        session['user_shortname'] = profile_data['Nickname'] 
                        # Hoặc tạo biến riêng hiển thị
                        user_data_combined['DisplayName'] = profile_data['Nickname']
                    else:
                        user_data_combined['DisplayName'] = session.get('user_shortname')

                # 2. Lấy danh sách Theme đã mở khóa (Query trực tiếp bảng Inventory)
                # Vì UserService không return full inventory trong hàm get_user_profile
                inv_query = "SELECT ItemCode FROM TitanOS_UserInventory WHERE UserCode = ?"
                inv_data = current_app.db_manager.get_data(inv_query, (user_code,))
                
                if inv_data:
                    owned_items = [row['ItemCode'] for row in inv_data]
                    # Chỉ lọc lấy các item là theme để đưa vào switcher
                    valid_themes = ['dark', 'fantasy', 'adorable']
                    for t in valid_themes:
                        if t in owned_items:
                            unlocked_themes.append(t)

            except Exception as e:
                current_app.logger.error(f"Lỗi load User Context: {e}")

        # Tính toán danh hiệu (Title) hiển thị nếu DB chưa có
        try:
            lvl = int(user_data_combined.get('Level', 1))
        except (ValueError, TypeError):
            lvl = 1 # Fallback về level 1 nếu lỗi
        
        # [QUAN TRỌNG] Cập nhật lại vào dictionary để template nhận được số INT
        user_data_combined['Level'] = lvl 
            
        if not user_data_combined.get('Title'):
            if lvl < 5: title = "Newbie (Tập sự)"
            elif lvl < 20: title = "Junior (Nhân viên)"
            elif lvl < 30: title = "Senior (Chuyên viên)"
            else: title = "Master (Doanh nhân)"
            user_data_combined['Title'] = title

        # Đóng gói dữ liệu trả về template
        final_context = {
            'is_authenticated': session.get('logged_in', False),
            'usercode': user_code,
            'username': session.get('username'),
            'shortname': session.get('user_shortname'),
            'role': session.get('user_role'),
            'bo_phan': session.get('bo_phan'),
            'can': check_permission,
            
            # --- DỮ LIỆU GAME & UI ---
            # Ưu tiên lấy Theme từ DB (Equipped), nếu không có thì lấy Session
            'theme': user_data_combined.get('ThemeColor') or session.get('theme', 'light'),
            
            # Truyền object chứa toàn bộ thông tin (Level, XP, Coin, Frame...)
            'stats': user_data_combined, 
            'current_user': user_data_combined, # Alias để dùng biến nào cũng được
            
            'unlocked_themes': unlocked_themes,
            'title': user_data_combined['Title']
        }

        # Trả về 2 biến global để template dùng: user_context và current_user
        return dict(user_context=final_context, current_user=final_context)
# 6. Global Before Request (Chạy trước mỗi request)
    @app.before_request
    def before_request():
        # Có thể thêm logic kiểm tra DB connection hoặc Global Security tại đây
        pass
    
    # [QUAN TRỌNG] Phải trả về biến app, nếu không bên app.py sẽ nhận được None
    return app