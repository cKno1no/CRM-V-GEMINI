import os
import requests
import time

# --- CẤU HÌNH ---
BASE_DIR = "static/img/3d_assets"

# Tạo các thư mục nếu chưa có
for cat in ['factory', 'maintenance', 'industry_40', 'productivity', 'culture', 'parts_oil']:
    path = os.path.join(BASE_DIR, cat)
    if not os.path.exists(path):
        os.makedirs(path)

# --- DANH SÁCH 60+ ẢNH (Tuyển chọn từ GitHub Raw) ---
MEGA_ASSETS = {
    # 1. NHÀ MÁY & CÔNG NGHIỆP (Factory)
    'factory': [
        ('crane_hook.png', 'https://raw.githubusercontent.com/microsoft/fluentui-emoji/main/assets/Hook/3D/hook_3d.png'),
        ('brick_wall.png', 'https://raw.githubusercontent.com/microsoft/fluentui-emoji/main/assets/Brick/3D/brick_3d.png'),
        ('ladder.png', 'https://raw.githubusercontent.com/microsoft/fluentui-emoji/main/assets/Ladder/3D/ladder_3d.png'),
        ('truck_delivery.png', 'https://raw.githubusercontent.com/microsoft/fluentui-emoji/main/assets/Delivery%20truck/3D/delivery_truck_3d.png'),
        ('shipping_box.png', 'https://raw.githubusercontent.com/microsoft/fluentui-emoji/main/assets/Package/3D/package_3d.png'),
        ('fuel_pump.png', 'https://raw.githubusercontent.com/microsoft/fluentui-emoji/main/assets/Fuel%20pump/3D/fuel_pump_3d.png'),
        ('high_voltage.png', 'https://raw.githubusercontent.com/microsoft/fluentui-emoji/main/assets/High%20voltage/3D/high_voltage_3d.png'),
        ('stop_sign.png', 'https://raw.githubusercontent.com/microsoft/fluentui-emoji/main/assets/Stop%20sign/3D/stop_sign_3d.png'),
        ('construction_sign.png', 'https://raw.githubusercontent.com/microsoft/fluentui-emoji/main/assets/Construction/3D/construction_3d.png'),
        ('helmet.png', 'https://raw.githubusercontent.com/microsoft/fluentui-emoji/main/assets/Rescue%20worker%E2%80%99s%20helmet/3D/rescue_worker%E2%80%99s_helmet_3d.png')
    ],

    # 2. BẢO DƯỠNG (Maintenance)
    'maintenance': [
        ('level_slider.png', 'https://raw.githubusercontent.com/microsoft/fluentui-emoji/main/assets/Level%20slider/3D/level_slider_3d.png'),
        ('control_knobs.png', 'https://raw.githubusercontent.com/microsoft/fluentui-emoji/main/assets/Control%20knobs/3D/control_knobs_3d.png'),
        ('battery.png', 'https://raw.githubusercontent.com/microsoft/fluentui-emoji/main/assets/Battery/3D/battery_3d.png'),
        ('flashlight.png', 'https://raw.githubusercontent.com/microsoft/fluentui-emoji/main/assets/Flashlight/3D/flashlight_3d.png'),
        ('magnet_tool.png', 'https://raw.githubusercontent.com/microsoft/fluentui-emoji/main/assets/Magnet/3D/magnet_3d.png'),
        ('microscope.png', 'https://raw.githubusercontent.com/microsoft/fluentui-emoji/main/assets/Microscope/3D/microscope_3d.png'), # Soi lỗi
        ('balance_scale.png', 'https://raw.githubusercontent.com/microsoft/fluentui-emoji/main/assets/Balance%20scale/3D/balance_scale_3d.png'),
        ('clipboard_check.png', 'https://raw.githubusercontent.com/microsoft/fluentui-emoji/main/assets/Clipboard/3D/clipboard_3d.png'),
        ('shield_check.png', 'https://raw.githubusercontent.com/microsoft/fluentui-emoji/main/assets/Shield/3D/shield_3d.png'), # An toàn
        ('fire_ext.png', 'https://raw.githubusercontent.com/microsoft/fluentui-emoji/main/assets/Fire%20extinguisher/3D/fire_extinguisher_3d.png')
    ],

    # 3. INTERNET 4.0 & CÔNG NGHỆ (Nguồn: 3dicons - Glass style)
    'industry_40': [
        ('server_glass.png', 'https://raw.githubusercontent.com/realvjy/3dicons/master/png/computer/server-front-color.png'),
        ('cloud_glass.png', 'https://raw.githubusercontent.com/realvjy/3dicons/master/png/computer/cloud-front-color.png'),
        ('wifi_glass.png', 'https://raw.githubusercontent.com/realvjy/3dicons/master/png/dynamic/wifi-dynamic-color.png'),
        ('lock_glass.png', 'https://raw.githubusercontent.com/realvjy/3dicons/master/png/dynamic/lock-dynamic-color.png'), # Security
        ('code_glass.png', 'https://raw.githubusercontent.com/realvjy/3dicons/master/png/dynamic/code-dynamic-color.png'),
        ('folder_glass.png', 'https://raw.githubusercontent.com/realvjy/3dicons/master/png/dynamic/folder-dynamic-color.png'),
        ('rocket_glass.png', 'https://raw.githubusercontent.com/realvjy/3dicons/master/png/dynamic/rocket-dynamic-color.png'),
        ('joystick.png', 'https://raw.githubusercontent.com/microsoft/fluentui-emoji/main/assets/Joystick/3D/joystick_3d.png'), # Điều khiển từ xa
        ('laptop_code.png', 'https://raw.githubusercontent.com/microsoft/fluentui-emoji/main/assets/Technologist/Default/3D/technologist_3d_default.png'),
        ('antenna.png', 'https://raw.githubusercontent.com/microsoft/fluentui-emoji/main/assets/Satellite%20antenna/3D/satellite_antenna_3d.png')
    ],

    # 4. LÀM VIỆC HIỆU QUẢ (Productivity)
    'productivity': [
        ('chart_glass.png', 'https://raw.githubusercontent.com/realvjy/3dicons/master/png/dynamic/chart-dynamic-color.png'),
        ('calendar_glass.png', 'https://raw.githubusercontent.com/realvjy/3dicons/master/png/dynamic/calender-dynamic-color.png'),
        ('target_glass.png', 'https://raw.githubusercontent.com/realvjy/3dicons/master/png/dynamic/target-dynamic-color.png'),
        ('notify_bell.png', 'https://raw.githubusercontent.com/realvjy/3dicons/master/png/dynamic/notify-dynamic-color.png'),
        ('medal_first.png', 'https://raw.githubusercontent.com/microsoft/fluentui-emoji/main/assets/1st%20place%20medal/3D/1st_place_medal_3d.png'),
        ('hourglass.png', 'https://raw.githubusercontent.com/microsoft/fluentui-emoji/main/assets/Hourglass%20done/3D/hourglass_done_3d.png'),
        ('gem_stone.png', 'https://raw.githubusercontent.com/microsoft/fluentui-emoji/main/assets/Gem%20stone/3D/gem_stone_3d.png'),
        ('pushpin.png', 'https://raw.githubusercontent.com/microsoft/fluentui-emoji/main/assets/Pushpin/3D/pushpin_3d.png'),
        ('bookmark.png', 'https://raw.githubusercontent.com/microsoft/fluentui-emoji/main/assets/Bookmark%20tabs/3D/bookmark_tabs_3d.png'),
        ('key.png', 'https://raw.githubusercontent.com/microsoft/fluentui-emoji/main/assets/Key/3D/key_3d.png') # Key success
    ],

    # 5. VĂN HÓA DOANH NGHIỆP (Culture)
    'culture': [
        ('hand_shake_glass.png', 'https://raw.githubusercontent.com/realvjy/3dicons/master/png/hands/hand-shake-front-color.png'),
        ('thumb_up_glass.png', 'https://raw.githubusercontent.com/realvjy/3dicons/master/png/hands/thumb-up-front-color.png'),
        ('heart_glass.png', 'https://raw.githubusercontent.com/realvjy/3dicons/master/png/dynamic/heart-dynamic-color.png'),
        ('chat_glass.png', 'https://raw.githubusercontent.com/realvjy/3dicons/master/png/dynamic/chat-dynamic-color.png'),
        ('star_glass.png', 'https://raw.githubusercontent.com/realvjy/3dicons/master/png/dynamic/star-dynamic-color.png'),
        ('megaphone.png', 'https://raw.githubusercontent.com/microsoft/fluentui-emoji/main/assets/Megaphone/3D/megaphone_3d.png'),
        ('grad_cap.png', 'https://raw.githubusercontent.com/microsoft/fluentui-emoji/main/assets/Graduation%20cap/3D/graduation_cap_3d.png'),
        ('crown.png', 'https://raw.githubusercontent.com/microsoft/fluentui-emoji/main/assets/Crown/3D/crown_3d.png'),
        ('busts_silhouette.png', 'https://raw.githubusercontent.com/microsoft/fluentui-emoji/main/assets/Busts%20in%20silhouette/3D/busts_in_silhouette_3d.png'), # Team
        ('sparkles.png', 'https://raw.githubusercontent.com/microsoft/fluentui-emoji/main/assets/Sparkles/3D/sparkles_3d.png')
    ],

    # 6. BẠC ĐẠN, DẦU MỠ, THỦY LỰC (Phụ tùng)
    'parts_oil': [
        ('oil_drum_new.png', 'https://raw.githubusercontent.com/microsoft/fluentui-emoji/main/assets/Oil%20drum/3D/oil_drum_3d.png'),
        ('water_drop.png', 'https://raw.githubusercontent.com/microsoft/fluentui-emoji/main/assets/Droplet/3D/droplet_3d.png'), # Dầu/Thủy lực
        ('dna_chain.png', 'https://raw.githubusercontent.com/microsoft/fluentui-emoji/main/assets/DNA/3D/dna_3d.png'), # Tượng trưng cho cấu trúc/xích
        ('link_chain.png', 'https://raw.githubusercontent.com/microsoft/fluentui-emoji/main/assets/Link/3D/link_3d.png'),
        ('test_tube.png', 'https://raw.githubusercontent.com/microsoft/fluentui-emoji/main/assets/Test%20tube/3D/test_tube_3d.png'), # Hóa chất
        ('compass.png', 'https://raw.githubusercontent.com/microsoft/fluentui-emoji/main/assets/Compass/3D/compass_3d.png'), # Độ chính xác
        ('abacus.png', 'https://raw.githubusercontent.com/microsoft/fluentui-emoji/main/assets/Abacus/3D/abacus_3d.png'), # Tính toán kỹ thuật
        ('clamp_vice.png', 'https://raw.githubusercontent.com/microsoft/fluentui-emoji/main/assets/Clamp/3D/clamp_3d.png'), # Kẹp
        ('nut_bolt_new.png', 'https://raw.githubusercontent.com/microsoft/fluentui-emoji/main/assets/Nut%20and%20bolt/3D/nut_and_bolt_3d.png'),
        ('wastebasket.png', 'https://raw.githubusercontent.com/microsoft/fluentui-emoji/main/assets/Wastebasket/3D/wastebasket_3d.png') # Xử lý thải
    ]
}

def main():
    print("🚀 Bắt đầu tải thêm 60+ ảnh 3D đa dạng nguồn...")
    
    total = 0
    for category, items in MEGA_ASSETS.items():
        cat_dir = os.path.join(BASE_DIR, category)
        if not os.path.exists(cat_dir):
            os.makedirs(cat_dir)
            
        print(f"\n📂 Chủ đề: {category.upper()}")
        
        for filename, url in items:
            save_path = os.path.join(cat_dir, filename)
            
            if os.path.exists(save_path):
                print(f"   ⏩ Đã có: {filename}")
                continue
                
            try:
                # Fake User-Agent để tránh bị GitHub chặn nếu tải nhanh
                headers = {'User-Agent': 'Mozilla/5.0'}
                r = requests.get(url, headers=headers, timeout=15)
                
                if r.status_code == 200:
                    with open(save_path, 'wb') as f:
                        f.write(r.content)
                    print(f"   ✅ Đã tải: {filename}")
                    total += 1
                else:
                    print(f"   ❌ Lỗi {r.status_code}: {filename}")
            except Exception as e:
                print(f"   ❌ Lỗi mạng: {filename} - {e}")
            
            # Nghỉ 0.2s để lịch sự với server
            time.sleep(0.2)

    print(f"\n🎉 HOÀN TẤT! Đã bổ sung {total} ảnh mới vào kho.")
    print("👉 Sếp hãy chạy lại script 'auto_assign_thumbnails.py' để AI có thêm nhiều lựa chọn mới nhé!")

if __name__ == "__main__":
    main()