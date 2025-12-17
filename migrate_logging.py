import os
import re

# Cấu hình
TARGET_EXTENSIONS = {".py"}
IGNORE_DIRS = {".git", "__pycache__", "venv", "env", "logs", "migrations"}

# Regex patterns
PRINT_PATTERN = re.compile(r'^\s*print\s*\((.*)\)', re.MULTILINE)
ERROR_KEYWORDS = ["Lỗi", "Error", "Exception", "fail", "CẢNH BÁO"]

def should_use_error_level(content):
    """Kiểm tra xem nội dung print có phải là lỗi không"""
    return any(kw.lower() in content.lower() for kw in ERROR_KEYWORDS)

def process_file(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        lines = f.readlines()

    new_lines = []
    modified = False
    has_flask_import = False
    
    # Kiểm tra xem file đã import current_app chưa
    content_str = "".join(lines)
    if "from flask import current_app" in content_str or "import current_app" in content_str:
        has_flask_import = True

    for line in lines:
        # Bỏ qua dòng comment
        if line.strip().startswith("#"):
            new_lines.append(line)
            continue

        match = PRINT_PATTERN.search(line)
        if match:
            # Lấy nội dung bên trong print(...)
            # Lưu ý: Regex này xử lý print đơn giản trên 1 dòng. 
            # Print nhiều dòng phức tạp có thể cần check tay.
            indent = line[:line.find("print")] # Giữ nguyên thụt đầu dòng
            content = match.group(1)
            
            if should_use_error_level(content):
                new_line = f"{indent}current_app.logger.error({content})\n"
            else:
                new_line = f"{indent}current_app.logger.info({content})\n"
            
            new_lines.append(new_line)
            modified = True
        else:
            new_lines.append(line)

    if modified:
        # Nếu có sửa đổi, cần đảm bảo import current_app
        if not has_flask_import:
            # Tìm vị trí thích hợp để chèn import (sau các import khác hoặc đầu file)
            insert_idx = 0
            for i, l in enumerate(new_lines):
                if l.startswith("import ") or l.startswith("from "):
                    insert_idx = i
                    break
            
            new_lines.insert(insert_idx, "from flask import current_app\n")
            print(f"[AUTO-IMPORT] Đã thêm import vào: {file_path}")

        # Ghi lại file
        with open(file_path, "w", encoding="utf-8") as f:
            f.writelines(new_lines)
        print(f"[UPDATED] Đã sửa file: {file_path}")

def main():
    root_dir = os.getcwd()
    print(f"Đang quét thư mục: {root_dir}")

    for root, dirs, files in os.walk(root_dir):
        # Bỏ qua thư mục rác
        dirs[:] = [d for d in dirs if d not in IGNORE_DIRS]

        for file in files:
            if os.path.splitext(file)[1] in TARGET_EXTENSIONS:
                # Bỏ qua chính file script này
                if file == "migrate_logging.py":
                    continue
                    
                file_path = os.path.join(root, file)
                try:
                    process_file(file_path)
                except Exception as e:
                    print(f"Lỗi khi xử lý file {file_path}: {e}")

if __name__ == "__main__":
    main()