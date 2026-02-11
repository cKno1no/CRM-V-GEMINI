from flask import Blueprint, render_template, jsonify, request, session, current_app, redirect, url_for, flash
from utils import login_required

training_bp = Blueprint('training_bp', __name__)

# ==============================================================================
# 1. DASHBOARD & COURSE NAVIGATION (GIAO DIỆN MỚI)
# ==============================================================================

@training_bp.route('/training', methods=['GET'])
@login_required
def training_dashboard():
    """Trang chủ Học tập (Dashboard V2)"""
    return render_template('training_dashboard_v2.html')

@training_bp.route('/api/training/dashboard_v2', methods=['GET'])
@login_required
def api_dashboard_v2():
    """API lấy dữ liệu Dashboard theo Category"""
    user_code = session.get('user_code')
    data = current_app.training_service.get_training_dashboard_v2(user_code)
    return jsonify(data)

@training_bp.route('/training/course/<int:course_id>', methods=['GET'])
@login_required
def course_detail_page(course_id):
    """Trang chi tiết Khóa học (Danh sách bài học)"""
    user_code = session.get('user_code')
    data = current_app.training_service.get_course_detail(course_id, user_code)
    
    if not data:
        flash("Khóa học không tồn tại hoặc đã bị ẩn.", "danger")
        return redirect(url_for('training_bp.training_dashboard'))
        
    return render_template('course_detail.html', course=data['info'], materials=data['materials'])

# ==============================================================================
# 2. STUDY ROOM (PHÒNG HỌC & AI TUTOR)
# ==============================================================================

@training_bp.route('/training/study-room/<int:material_id>', methods=['GET'])
@login_required
def study_room_detail(material_id):
    """Giao diện đọc tài liệu + Chat AI"""
    user_code = session.get('user_code')
    
    # Lấy thông tin tài liệu & tiến độ
    material = current_app.training_service.get_material_content(material_id, user_code)
    
    if not material:
        flash("Tài liệu không tồn tại.", "danger")
        return redirect(url_for('training_bp.training_dashboard'))
        
    return render_template('study_room.html', material=material)

@training_bp.route('/api/library/chat', methods=['POST'])
@login_required
def api_library_chat():
    """API Chat với tài liệu (AI Tutor)"""
    data = request.json
    material_id = data.get('material_id')
    question = data.get('question')
    
    # Gọi service xử lý (Live Read PDF)
    res = current_app.training_service.chat_with_document(material_id, question)
    return jsonify(res)

@training_bp.route('/api/training/progress', methods=['POST'])
@login_required
def api_update_progress():
    """API lưu trang đang đọc dở"""
    data = request.json
    res = current_app.training_service.update_reading_progress(
        session.get('user_code'), 
        data.get('material_id'), 
        data.get('page')
    )
    return jsonify({'success': res})

# ==============================================================================
# 3. QUIZ SYSTEM (BÀI KIỂM TRA CUỐI BÀI)
# ==============================================================================

@training_bp.route('/api/quiz/get', methods=['POST'])
@login_required
def api_get_quiz():
    """Lấy 5 câu hỏi (Giữ 4 cũ - Đổi 1 mới)"""
    user_code = session.get('user_code')
    mid = request.json.get('material_id')
    
    # Truyền thêm user_code vào hàm
    questions = current_app.training_service.get_material_quiz(mid, user_code)
    
    return jsonify(questions)

@training_bp.route('/api/quiz/submit', methods=['POST'])
@login_required
def api_submit_quiz():
    """Nộp bài kiểm tra & Chấm điểm"""
    data = request.json
    res = current_app.training_service.submit_material_quiz(
        session.get('user_code'), 
        data.get('material_id'), 
        data.get('answers')
    )
    return jsonify(res)

# ==============================================================================
# 4. DAILY CHALLENGE (GAME MỖI NGÀY)
# ==============================================================================

@training_bp.route('/training/daily-challenge', methods=['GET'])
@login_required
def daily_challenge_page():
    """Trang Game"""
    return render_template('daily_challenge.html')

@training_bp.route('/api/challenge/status', methods=['GET'])
@login_required
def get_game_status():
    """Kiểm tra trạng thái (Chờ / Làm bài / Đã xong)"""
    user_code = session.get('user_code')
    status = current_app.training_service.get_current_challenge_status(user_code)
    return jsonify(status)

@training_bp.route('/api/challenge/submit', methods=['POST'])
@login_required
def submit_game_answer():
    """Nộp bài Daily Challenge"""
    user_code = session.get('user_code')
    data = request.json
    result = current_app.training_service.submit_answer(
        user_code, 
        data.get('session_id'), 
        data.get('answer')
    )
    return jsonify(result)

@training_bp.route('/api/training/search', methods=['GET'])
@login_required
def api_search_courses():
    query = request.args.get('q', '').strip()
    if not query: return jsonify([])

    # Tìm kiếm theo Tên khóa học hoặc Tên bài học
    # Kết quả trả về danh sách Khóa học
    res = current_app.training_service.search_courses_and_materials(query)
    return jsonify(res)

@training_bp.route('/training/category/<path:category_name>', methods=['GET']) # Dùng <path:> để bắt được cả ký tự đặc biệt
@login_required
def category_detail_page(category_name):
    """Trang chi tiết danh sách khóa học theo chủ đề"""
    user_code = session.get('user_code')
    
    # 1. Decode tên category từ URL (VD: Kỹ%20thuật -> Kỹ thuật)
    try:
        cat_decoded = urllib.parse.unquote(category_name)
    except:
        cat_decoded = category_name

    # 2. Lấy dữ liệu từ Service
    all_data = current_app.training_service.get_training_dashboard_v2(user_code)
    
    # 3. Tìm category tương ứng trong data (So sánh tương đối)
    target_courses = {}
    found_key = None
    
    # Chuẩn hóa chuỗi để so sánh (lowercase, strip)
    search_key = cat_decoded.lower().strip()
    
    for key, val in all_data.items():
        if key.lower().strip() == search_key:
            found_key = key
            target_courses = val
            break
            
    # Fallback: Nếu không tìm thấy chính xác, thử tìm gần đúng (contains)
    if not found_key:
        for key, val in all_data.items():
            if search_key in key.lower() or key.lower() in search_key:
                found_key = key
                target_courses = val
                break
    
    # 4. Flatten danh sách khóa học (Gộp các sub-category lại để hiển thị grid)
    courses_flat = []
    sub_categories = []
    
    if target_courses:
        sub_categories = list(target_courses.keys())
        for sub, list_c in target_courses.items():
            courses_flat.extend(list_c)
            
    # Nếu không có dữ liệu, vẫn render trang nhưng list rỗng
    return render_template(
        'training_category_detail.html', 
        category_name=found_key if found_key else cat_decoded, 
        courses=courses_flat,
        sub_categories=sub_categories
    )