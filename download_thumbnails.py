import os
import requests
import time
from urllib.parse import quote

# 1. Cấu hình thư mục lưu ảnh
SAVE_DIR = "static/img/thumbnails"
if not os.path.exists(SAVE_DIR):
    os.makedirs(SAVE_DIR)

# 2. Định nghĩa phong cách chung (Style Prompts)
# Tông màu xanh dương (Blue), phong cách 3D nổi, nền trắng sạch sẽ
STYLE = "3d render, isometric view, cute 3d icon style, minimalist, industrial blue and white color palette, soft lighting, white background, high quality, unreal engine 5 render"

# 3. Danh sách từ khóa cho 30 khóa học (Map theo Sub-categories)
# Format: (Tên file lưu, Từ khóa nội dung)
courses_prompts = [
    # --- Nhóm: Vòng bi & Truyền động ---
    ("bearing_struct.jpg", "industrial ball bearing structure cross section"),
    ("bearing_install.jpg", "mechanic installing metal bearing with tools"),
    ("lubrication.jpg", "oil drop lubrication industrial gears"),
    ("transmission_belt.jpg", "industrial conveyor belt system"),
    ("motor_drive.jpg", "electric motor engine industrial"),

    # --- Nhóm: Hệ thống Cơ khí & Thiết bị ---
    ("pump_system.jpg", "industrial water pump system"),
    ("fan_blower.jpg", "industrial ventilation fan blower"),
    ("hydraulics.jpg", "hydraulic cylinder and pipes"),
    ("pneumatics.jpg", "pneumatic air compressor machine"),
    ("valves.jpg", "industrial pipeline valve metal"),

    # --- Nhóm: Làm kín & Bảo vệ (Sealing) ---
    ("gaskets.jpg", "rubber o-ring and gasket sealing"),
    ("mechanical_seal.jpg", "mechanical seal component"),
    ("corrosion.jpg", "rusty metal vs shiny metal protection shield"),
    
    # --- Nhóm: Quản lý Bảo trì (MRO) ---
    ("maintenance_tools.jpg", "toolbox with wrench and screwdriver"),
    ("mro_checklist.jpg", "clipboard with checklist and gear icon"),
    ("predictive_maint.jpg", "graph chart monitoring machine health"),
    
    # --- Nhóm: Tự động hóa & IoT ---
    ("smart_factory.jpg", "smart factory building with wifi signal"),
    ("iot_sensor.jpg", "digital sensor connected to cloud"),
    ("digital_twin.jpg", "hologram of a machine digital twin"),
    ("automation_arm.jpg", "robotic arm assembly line"),
    
    # --- Nhóm: Kỹ năng mềm & Kinh doanh ---
    ("sales_growth.jpg", "rising arrow profit chart business"),
    ("negotiation.jpg", "two 3d characters shaking hands business"),
    ("leadership.jpg", "chess king piece leading pawns"),
    ("time_mgmt.jpg", "alarm clock and calendar schedule"),
    ("presentation.jpg", "3d character pointing at whiteboard presentation"),
    
    # --- Nhóm: Văn hóa & Quy định ---
    ("company_culture.jpg", "teamwork puzzle pieces connecting"),
    ("safety_first.jpg", "industrial safety helmet yellow hardhat"),
    ("regulations.jpg", "document book with law scale icon"),
    ("new_hire.jpg", "welcome badge for new employee"),
    ("csr_sustain.jpg", "green leaf growing from gear sustainability")
]

def download_image(filename, prompt):
    # Tạo full prompt
    full_prompt = f"{prompt}, {STYLE}"
    # Encode URL
    encoded_prompt = quote(full_prompt)
    # URL API (Pollinations.ai - Free, No Key required)
    url = f"https://image.pollinations.ai/prompt/{encoded_prompt}?width=800&height=600&nologo=true&seed={int(time.time())}"
    
    print(f"⬇️ Đang tạo và tải: {filename}...")
    try:
        response = requests.get(url, timeout=30)
        if response.status_code == 200:
            file_path = os.path.join(SAVE_DIR, filename)
            with open(file_path, 'wb') as f:
                f.write(response.content)
            print(f"✅ Đã lưu: {file_path}")
        else:
            print(f"❌ Lỗi tải {filename}: Status {response.status_code}")
    except Exception as e:
        print(f"❌ Lỗi kết nối: {e}")

def main():
    print("🚀 Bắt đầu tải 30 ảnh mẫu 3D Isometric...")
    print(f"📂 Thư mục lưu: {SAVE_DIR}")
    print("-" * 50)
    
    for filename, prompt in courses_prompts:
        download_image(filename, prompt)
        # Nghỉ 1 xíu để không spam server
        time.sleep(1.5) 
        
    print("-" * 50)
    print("✨ Hoàn tất! Hãy kiểm tra thư mục ảnh.")

if __name__ == "__main__":
    main()