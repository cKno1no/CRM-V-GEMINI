import pandas as pd
import pyodbc
import google.generativeai as genai
import time
import os
from collections import defaultdict

# --- CẤU HÌNH ---
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

def get_ai_clean_answer(question, answers_list):
    """
    Prompt được thiết kế lại để bám sát yêu cầu nghiệp vụ
    """
    prompt = f"""
    Bạn là Trưởng phòng đào tạo đang chuẩn hóa đáp án thi.
    
    CÂU HỎI: {question}
    
    DƯỚI ĐÂY LÀ TỔNG HỢP CÂU TRẢ LỜI TỪ NHÂN VIÊN (Kèm điểm tin cậy):
    {answers_list}
    
    NHIỆM VỤ VÀ YÊU CẦU BẮT BUỘC:
    1. NGUỒN DỮ LIỆU: Phải dựa 90% vào các câu trả lời có điểm tin cậy cao ở trên. Chỉ chỉnh sửa câu từ cho gãy gọn, chuyên nghiệp. KHÔNG TỰ BỊA RA KIẾN THỨC MỚI nếu nhân viên đã trả lời đúng ý.
    2. KIỂM TRA LOGIC: 
       - Nếu các câu trả lời trên đều SAI HOÀN TOÀN về mặt kiến thức/kỹ thuật thực tế: Hãy tự viết đáp án đúng và bắt đầu bằng cụm từ "[AIG]".
       - Nếu có ý đúng, ý sai: Hãy lọc lấy ý đúng, bỏ ý sai.
    3. ĐỊNH DẠNG:
       - Ngắn gọn, súc tích (Tối đa 150 từ).
       - Bắt buộc phải chia đoạn, xuống dòng (dùng dấu gạch đầu dòng - ) nếu có nhiều ý. Không viết thành 1 khối văn bản dày đặc.
    """
    
    try:
        response = model.generate_content(prompt, request_options={'timeout': 600})
        return response.text.strip()
    except Exception as e:
        print(f"Lỗi AI: {e}")
        return None

def main():
    try:
        conn = pyodbc.connect(CONN_STR)
    except Exception as e:
        print(f"Lỗi kết nối SQL: {e}")
        return

    # [FIX JOIN] Join theo QuestionCode (INT) = QuestionID_Ref (INT)
    # Lấy TOP 500 câu hỏi mới nhất (ID cao nhất) chưa có đáp án
    sql = """
    SELECT TOP 500 
        Q.ID as RecordID,      -- ID tự tăng (để update)
        Q.QuestionCode,        -- Mã câu hỏi (để hiển thị/log)
        Q.Content as QuestionContent,
        H.UserCode, 
        H.Answer1, H.Answer2, H.Answer3, H.Answer4, H.Answer5, H.Answer6
    FROM TRAINING_QUESTION_BANK Q
    JOIN TRAINING_HISTORY_RAW H ON Q.QuestionCode = H.QuestionID_Ref
    WHERE Q.CorrectAnswer IS NULL
    ORDER BY Q.QuestionCode DESC
    """
    
    print("Đang đọc dữ liệu từ SQL (Join Corrected)...")
    df = pd.read_sql(sql, conn)
    
    if df.empty:
        print("Không tìm thấy dữ liệu khớp! Hãy kiểm tra lại cột QuestionCode và QuestionID_Ref.")
        return

    # Gom nhóm theo RecordID (Khóa chính bảng Bank)
    grouped = df.groupby(['RecordID', 'QuestionCode', 'QuestionContent'])
    
    updates = []
    EXPERTS = ['GD001', 'KD004']
    
    print(f"--- BẮT ĐẦU CHẠY AI TRÊN {len(grouped)} CÂU HỎI ---")

    for (rec_id, q_code, q_content), group in grouped:
        print(f"\nĐang xử lý câu Mã: {q_code} (ID: {rec_id})...")
        
        weighted_counts = defaultdict(int)
        has_data = False
        
        for _, row in group.iterrows():
            # Gom cột trả lời
            parts = [str(row[f'Answer{i}']).strip() for i in range(1, 7) if pd.notnull(row[f'Answer{i}']) and str(row[f'Answer{i}']).strip() != '']
            
            if not parts: continue
            
            # Tạo chuỗi trả lời gộp
            ans_str = " | ".join(parts)
            has_data = True
            
            # Tính trọng số
            weight = 5 if row['UserCode'] in EXPERTS else 1
            weighted_counts[ans_str] += weight
            
        if not has_data:
            print("-> Bỏ qua: Không có dữ liệu trả lời.")
            continue
            
        # Sắp xếp theo điểm trọng số
        sorted_answers = sorted(weighted_counts.items(), key=lambda x: x[1], reverse=True)
        
        # Chỉ lấy Top 5 câu trả lời khác biệt nhất để tiết kiệm token và tránh nhiễu
        ai_input_list = "\n".join([f"- {ans} (Điểm tin cậy: {score})" for ans, score in sorted_answers[:5]])
        
        # Gọi AI
        clean_answer = get_ai_clean_answer(q_content, ai_input_list)
        
        if clean_answer:
            print(f"-> AI chốt:\n{clean_answer[:100]}...") 
            updates.append((clean_answer, int(rec_id)))
            
        time.sleep(1.5)

    # Update vào SQL
    if updates:
        print(f"\nĐang cập nhật {len(updates)} câu vào Database...")
        cursor = conn.cursor()
        try:
            # Update theo RecordID (ID tự tăng) cho chính xác tuyệt đối
            cursor.executemany("UPDATE TRAINING_QUESTION_BANK SET CorrectAnswer = ? WHERE ID = ?", updates)
            conn.commit()
            print("✅ HOÀN TẤT CẬP NHẬT!")
        except Exception as e:
            print(f"❌ Lỗi Update SQL: {e}")
    
    conn.close()

if __name__ == "__main__":
    main()