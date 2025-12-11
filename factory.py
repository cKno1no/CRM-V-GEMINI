# factory.py
from flask import Flask
from datetime import timedelta
import os
import redis
import config

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

def create_app():
    """Nhà máy khởi tạo ứng dụng Flask"""
    app = Flask(__name__, static_url_path='/static', static_folder='static')
    
    # Cấu hình App
    app.secret_key = config.APP_SECRET_KEY
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
        print(f"Redis connection failed: {e}")
        redis_client = None

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
    
    # [FIX] Khởi tạo và gắn PortalService
    app.portal_service = PortalService(db_manager)
    
    app.chatbot_service = ChatbotService(
        app.lookup_service, 
        app.customer_service, 
        app.delivery_service, 
        redis_client
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

    # 5. Inject User Context
    from flask import session
    @app.context_processor
    def inject_user():
        return dict(current_user={
            'is_authenticated': session.get('logged_in', False),
            'usercode': session.get('user_code'),
            'username': session.get('username'),
            'shortname': session.get('user_shortname'),
            'role': session.get('user_role'),
            'cap_tren': session.get('cap_tren'),
            'bo_phan': session.get('bo_phan')
        })

    return app