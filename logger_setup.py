# logger_setup.py
import logging
from logging.handlers import TimedRotatingFileHandler
import os
from flask import request, session

def setup_production_logging(app):
    # 1. Tạo thư mục 'logs' nếu chưa có
    if not os.path.exists('logs'):
        os.mkdir('logs')

    # 2. Định dạng Log (Format)
    # Cấu trúc: [Thời gian] [Mức độ] [File:Dòng] [User] - Nội dung
    formatter = logging.Formatter(
        '[%(asctime)s] %(levelname)s in %(module)s:%(lineno)d [%(user_code)s]: %(message)s'
    )

    # 3. Cấu hình Handler (Xoay vòng theo ngày)
    # filename: Đường dẫn file log
    # when='midnight': Cắt file vào lúc nửa đêm
    # interval=1: Mỗi 1 ngày cắt 1 lần
    # backupCount=30: Chỉ giữ lại log của 30 ngày gần nhất (tự xóa log cũ hơn)
    # encoding='utf-8': Để ghi tiếng Việt không bị lỗi font
    file_handler = TimedRotatingFileHandler(
        filename='logs/titan_os.log',
        when='midnight',
        interval=1,
        backupCount=30, 
        encoding='utf-8'
    )
    
    # Gán formatter cho handler
    file_handler.setFormatter(formatter)
    
    # Thiết lập mức độ ghi log (INFO trở lên sẽ được ghi: INFO, WARNING, ERROR, CRITICAL)
    file_handler.setLevel(logging.INFO)

    # 4. Filter để tự động thêm User Code vào log (để biết ai gây lỗi)
    class UserFilter(logging.Filter):
        def filter(self, record):
            try:
                record.user_code = session.get('user_code', 'System/Anon')
            except:
                record.user_code = 'System'
            return True

    file_handler.addFilter(UserFilter())

    # 5. Gắn Handler vào Flask App
    app.logger.addHandler(file_handler)
    app.logger.setLevel(logging.INFO)
    
    # Xóa default handler để tránh trùng lặp log ra console (tùy chọn)
    # app.logger.removeHandler(default_handler)

    app.logger.info("Titan OS Startup: Hệ thống Logging đã kích hoạt.")