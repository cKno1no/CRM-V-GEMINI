import os
import json
import random
import pyodbc
import google.generativeai as genai
from dotenv import load_dotenv
import re

# --- CẤU HÌNH ---
# --- CẤU HÌNH ---
load_dotenv()
API_KEY = "AIzaSyCC_qWqKqqupwwUT7mOR_Z75M9eKv8Vil4" # Hoặc lấy từ env

db_server = os.getenv('DB_SERVER')
db_name = os.getenv('DB_NAME')
db_uid = os.getenv('DB_UID')
db_pwd = os.getenv('DB_PWD')

CONN_STR = (
    r'DRIVER={ODBC Driver 17 for SQL Server};'
    f'SERVER={db_server};'
    f'DATABASE={db_name};'
    f'UID={db_uid};PWD={db_pwd}'
)



genai.configure(api_key=API_KEY)
model = genai.GenerativeModel('gemini-2.5-flash')

IMG_BASE_DIR = "static/img/3d_assets"



def get_db_connection():
    return pyodbc.connect(CONN_STR)

# 1. QUÉT KHO ẢNH HIỆN CÓ
def scan_local_images():
    """Trả về dictionary: { 'tên_folder': ['ảnh1.png', 'ảnh2.png'] }"""
    image_inventory = {}
    
    if not os.path.exists(IMG_BASE_DIR):
        print(f"❌ Không tìm thấy thư mục {IMG_BASE_DIR}. Hãy chạy script tải ảnh trước!")
        return {}

    for root, dirs, files in os.walk(IMG_BASE_DIR):
        folder_name = os.path.basename(root)
        if folder_name == '3d_assets': continue # Bỏ qua root
        
        valid_images = [f for f in files if f.lower().endswith(('.png', '.jpg', '.jpeg'))]
        if valid_images:
            image_inventory[folder_name] = valid_images
            
    return image_inventory

# 2. HÀM CHỌN ẢNH NGẪU NHIÊN (FALLBACK)
def get_random_fallback(category, inventory):
    """Chọn ảnh ngẫu nhiên theo Category nếu AI bó tay"""
    # Map Category DB sang Folder ảnh
    cat_lower = category.lower() if category else ""
    target_folders = []
    
    if 'kỹ thuật' in cat_lower:
        target_folders = ['maintenance', 'parts_oil', 'factory', 'industry_40']
    elif 'kinh doanh' in cat_lower or 'bán hàng' in cat_lower:
        target_folders = ['productivity', 'culture']
    elif 'kỹ năng' in cat_lower or 'lãnh đạo' in cat_lower:
        target_folders = ['productivity', 'culture']
    else:
        target_folders = list(inventory.keys()) # Random all

    # Chọn folder
    # Lọc những folder có tồn tại trong inventory
    available_folders = [f for f in target_folders if f in inventory]
    if not available_folders: 
        available_folders = list(inventory.keys())
        
    chosen_folder = random.choice(available_folders)
    chosen_img = random.choice(inventory[chosen_folder])
    
    return f"/{IMG_BASE_DIR}/{chosen_folder}/{chosen_img}"

# 3. AI MATCHING
def ai_assign_images(courses, inventory):
    print(f"🤖 Đang nhờ AI chọn ảnh cho {len(courses)} khóa học...")
    
    # Chuẩn bị dữ liệu gửi AI
    # Chỉ gửi ID và Tên để tiết kiệm token
    course_list_min = [{"id": c.CourseID, "title": c.Title, "cat": c.Category} for c in courses]
    
    prompt = f"""
    Bạn là chuyên gia thiết kế UI. Tôi có danh sách khóa học và kho ảnh 3D.
    Hãy chọn ảnh phù hợp nhất cho từng khóa học dựa trên Tên và Danh mục.
    
    KHO ẢNH (Phân loại theo folder):
    {json.dumps(inventory, ensure_ascii=False)}
    
    DANH SÁCH KHÓA HỌC:
    {json.dumps(course_list_min, ensure_ascii=False)}
    
    YÊU CẦU:
    1. Trả về JSON mapping: [ {{ "id": 1, "folder": "tên_folder", "file": "tên_file.png" }}, ... ]
    2. Nếu khóa học về Kỹ thuật/Bảo trì -> Ưu tiên folder 'maintenance', 'parts_oil', 'factory'.
    3. Nếu khóa học về Kinh doanh/Kỹ năng -> Ưu tiên 'productivity', 'culture'.
    4. Nếu khóa về Công nghệ/Số -> Ưu tiên 'industry_40'.
    5. Đảm bảo mọi ID đều được gán ảnh.
    """
    
    try:
        response = model.generate_content(prompt)
        json_str = response.text.replace('```json', '').replace('```', '').strip()
        # Vá lỗi JSON nếu có
        if not json_str.endswith(']'): json_str += ']'
        
        assignments = json.loads(json_str)
        return assignments
    except Exception as e:
        print(f"❌ Lỗi AI: {e}. Sẽ dùng chế độ Random Fallback.")
        return []

def main():
    # 1. Quét ảnh
    inventory = scan_local_images()
    if not inventory: return
    print(f"📸 Đã tìm thấy {sum(len(v) for v in inventory.values())} ảnh trong {len(inventory)} chủ đề.")

    # 2. Lấy khóa học
    conn = get_db_connection()
    cursor = conn.cursor()
    courses = cursor.execute("SELECT CourseID, Title, Category FROM TRAINING_COURSES").fetchall()
    
    # 3. Gọi AI
    ai_results = ai_assign_images(courses, inventory)
    
    # Map kết quả AI vào dict để dễ tra cứu
    ai_map = {item['id']: f"/{IMG_BASE_DIR}/{item['folder']}/{item['file']}" for item in ai_results}
    
    # 4. Cập nhật DB
    print("\n🔄 Đang cập nhật Database...")
    count = 0
    for c in courses:
        final_path = ""
        
        # Ưu tiên lấy từ AI
        if c.CourseID in ai_map:
            final_path = ai_map[c.CourseID]
            # Kiểm tra file có tồn tại thật không (phòng khi AI bịa tên file)
            real_check_path = final_path.lstrip('/')
            if not os.path.exists(real_check_path):
                # Nếu AI bịa tên file -> Fallback
                final_path = get_random_fallback(c.Category, inventory)
        else:
            # Nếu AI bỏ sót -> Fallback
            final_path = get_random_fallback(c.Category, inventory)
            
        cursor.execute("UPDATE TRAINING_COURSES SET ThumbnailUrl = ? WHERE CourseID = ?", (final_path, c.CourseID))
        count += 1
        print(f"   ✅ [{c.Category}] {c.Title} \n      -> {final_path}")

    conn.commit()
    conn.close()
    print(f"\n🎉 HOÀN TẤT! Đã gán ảnh 3D cho {count} khóa học.")

if __name__ == "__main__":
    main()