from waitress import serve
from app import app
import logging
import os

# Cấu hình logging cho Waitress
logger = logging.getLogger('waitress')
logger.setLevel(logging.INFO)

if __name__ == "__main__":
    # Lấy số luồng CPU (Logical Processors)
    cpu_count = os.cpu_count() or 4
    
    # Tính toán số threads tối ưu cho Waitress
    # Với Web App Database-bound (nhiều truy vấn SQL), nên tăng thread cao hơn số CPU.
    # Công thức an toàn: cpu_count * 1.5
    optimal_threads = int(cpu_count * 1.5) 
    
    # Giới hạn trần để tránh quá tải Context Switching nếu CPU quá nhiều luồng
    if optimal_threads > 32: optimal_threads = 32
    if optimal_threads < 8: optimal_threads = 8 # Đảm bảo tối thiểu 8 luồng

    print("-------------------------------------------------------")
    print("TITAN OS - PRODUCTION SERVER (WAITRESS)")
    print(f"System CPU: {cpu_count} logical cores")
    print(f"Worker Threads: {optimal_threads}")
    print("Server is running at: http://0.0.0.0:5000")
    print("Press Ctrl+C to stop.")
    print("-------------------------------------------------------")
    
    serve(
        app, 
        host='0.0.0.0', 
        port=5000,
        
        # --- CẤU HÌNH TỐI ƯU ---
        threads=optimal_threads,  # Số luồng xử lý song song (Khoảng 24 với máy của bạn)
        
        # Tăng giới hạn Backlog (Hàng chờ kết nối) cho lúc cao điểm
        backlog=1024,
        
        # Thời gian timeout (quan trọng cho các báo cáo chạy lâu)
        channel_timeout=300, # 5 phút (Cho phép báo cáo nặng chạy lâu mà không bị ngắt)
        
        # Giới hạn kích thước Request (Tránh upload file quá lớn làm treo)
        max_request_body_size=1073741824, # 1GB (Cho phép upload file lớn)
        
        # Giữ kết nối (Keep-Alive) để user lướt web mượt hơn
        connection_limit=1000, # Cho phép 1000 kết nối đồng thời
        cleanup_interval=30    # Dọn dẹp kết nối rác mỗi 30s
    )