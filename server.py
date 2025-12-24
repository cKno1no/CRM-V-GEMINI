# server.py
# --- TITAN OS PRODUCTION SERVER ---

import logging
from datetime import datetime
import os
import schedule
import time
import threading # [QUAN TRỌNG] Để chạy song song Scheduler và Server

# Import ứng dụng Flask
from app import app
from waitress import serve

# =========================================================================
# 1. CẤU HÌNH LOGGING (Giữ nguyên từ hệ thống cũ)
# =========================================================================
def logger_setup():
    # Tạo thư mục logs nếu chưa có
    if not os.path.exists('logs'):
        os.makedirs('logs')

    # Cấu hình logging cơ bản
    logging.basicConfig(
        level=logging.INFO,
        format='[%(asctime)s] %(levelname)s in %(module)s: %(message)s',
        handlers=[
            logging.FileHandler(f"logs/titan_server_{datetime.now().strftime('%Y%m%d')}.log", encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    logging.info("Titan OS Startup: Hệ thống Logging đã kích hoạt.")

# =========================================================================
# 2. CÁC JOB CHẠY NGẦM (SCHEDULER JOBS)
# =========================================================================
def run_daily_gamification():
    """
    Job chạy định kỳ (20:00 hàng ngày) để tổng kết điểm thưởng và gửi thư.
    """
    with app.app_context():
        try:
            print(f">>> [Job Scheduler] Bắt đầu tổng kết Gamification: {datetime.now().strftime('%H:%M:%S')}")
            
            if hasattr(app, 'gamification_service'):
                # Gọi service xử lý logic tính điểm
                app.gamification_service.process_daily_rewards()
                logging.info("[Job Scheduler] Đã chạy xong process_daily_rewards.")
            else:
                err_msg = "[Job Scheduler] Lỗi: Gamification Service chưa được khởi tạo trong app."
                print(err_msg)
                logging.error(err_msg)
                
            print(">>> [Job Scheduler] Hoàn tất tổng kết.")
            
        except Exception as e:
            err_msg = f"[Job Scheduler] Lỗi nghiêm trọng khi chạy job: {e}"
            print(err_msg)
            logging.error(err_msg)

def run_schedule_loop():
    """
    Vòng lặp vô tận để kiểm tra và kích hoạt các job đã lên lịch.
    Chạy trên một luồng (Thread) riêng biệt.
    """
    while True:
        try:
            schedule.run_pending()
            # Ngủ 20 giây để tiết kiệm CPU, độ trễ tối đa chỉ 20s
            time.sleep(120) 
        except Exception as e:
            logging.error(f"Lỗi trong luồng Scheduler Loop: {e}")
            time.sleep(120) # Nghỉ 5s rồi thử lại nếu lỗi

# =========================================================================
# 3. MAIN ENTRY POINT
# =========================================================================
if __name__ == '__main__':
    # A. Khởi tạo Logging
    logger_setup()
    
    # B. Cấu hình Lịch chạy (Scheduler)
    # Chạy vào 20:00 mỗi ngày
    schedule.every().day.at("20:20").do(run_daily_gamification)
    
    # [DEV ONLY] Bỏ comment dòng dưới để test chạy mỗi phút
    # schedule.every(1).minutes.do(run_daily_gamification)

    # C. Khởi động Luồng Scheduler (Daemon Thread)
    # Daemon=True nghĩa là khi tắt Server chính, luồng này cũng tự tắt theo
    scheduler_thread = threading.Thread(target=run_schedule_loop, daemon=True)
    scheduler_thread.start()
    
    print(f">>> Titan OS Scheduler đã khởi động song song (Check mỗi 20s)...")
    logging.info("Titan OS Scheduler started in background thread.")

    # D. Khởi động Web Server (Waitress) - Đây là Luồng chính (Blocking)
    print("-------------------------------------------------------")
    print("TITAN OS - PRODUCTION SERVER (WAITRESS)")
    print("System CPU: 8 logical cores") 
    print("Worker Threads: 12")
    print("Server is running at: http://0.0.0.0:5000")
    print("Press Ctrl+C to stop.")
    print("-------------------------------------------------------")
    
    # Waitress sẽ chiếm giữ luồng chính tại đây
    serve(app, host='0.0.0.0', port=5000, threads=12)