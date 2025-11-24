# services/commission_service.py
from db_manager import DBManager, safe_float
from datetime import datetime
import config
import pyodbc

class CommissionService:
    def __init__(self, db_manager: DBManager):
        self.db = db_manager

    def create_proposal(self, user_code, customer_id, date_from, date_to, commission_rate_percent):
        """
        Tạo phiếu dùng kết nối Transaction thủ công để đảm bảo Commit thành công.
        """
        conn = None
        try:
            # 1. Mở kết nối thủ công (Raw Connection)
            conn = self.db.get_transaction_connection()
            cursor = conn.cursor()
            
            # 2. Chạy lệnh SQL
            sql = "EXEC dbo.sp_CreateCommissionProposal ?, ?, ?, ?, ?"
            params = (user_code, customer_id, date_from, date_to, float(commission_rate_percent))
            
            cursor.execute(sql, params)
            
            # 3. Lấy ID trả về
            row = cursor.fetchone()
            new_voucher_id = None
            if row:
                new_voucher_id = row[0]
            
            # 4. QUAN TRỌNG: COMMIT GIAO DỊCH
            conn.commit()
            
            return new_voucher_id

        except Exception as e:
            print(f"Lỗi tạo phiếu hoa hồng: {e}")
            # Rollback nếu có lỗi
            if conn: 
                conn.rollback()
            return None
        finally:
            # Đóng kết nối
            if conn:
                conn.close()

    # --- Các hàm khác giữ nguyên logic, chỉ đảm bảo dùng đúng DBManager ---
    def recalculate_proposal(self, ma_so):
        query = """
            UPDATE M
            SET 
                DOANH_SO_CHON = ISNULL(D.TotalSelected, 0),
                GIA_TRI_CHI = ISNULL(D.TotalSelected, 0) * (ISNULL(M.MUC_CHI_PERCENT, 0) / 100.0)
            FROM dbo.[DE XUAT BAO HANH_MASTER] M
            OUTER APPLY (
                SELECT SUM(DOANH_SO) as TotalSelected 
                FROM dbo.[DE XUAT BAO HANH_DS] 
                WHERE MA_SO = M.MA_SO AND CHON = 1
            ) D
            WHERE M.MA_SO = ?
        """
        self.db.execute_non_query(query, (ma_so,))

    def toggle_invoice(self, detail_id, is_checked):
        self.db.execute_non_query(
            "UPDATE dbo.[DE XUAT BAO HANH_DS] SET CHON = ? WHERE VoucherID = ?", 
            (1 if is_checked else 0, detail_id)
        )
        row = self.db.get_data("SELECT MA_SO FROM dbo.[DE XUAT BAO HANH_DS] WHERE VoucherID = ?", (detail_id,))
        if row:
            self.recalculate_proposal(row[0]['MA_SO'])
            return True
        return False

    def submit_to_payment_request(self, ma_so, user_code):
        master_data = self.db.get_data("SELECT * FROM dbo.[DE XUAT BAO HANH_MASTER] WHERE MA_SO = ?", (ma_so,))
        if not master_data:
            return {'success': False, 'message': 'Không tìm thấy phiếu đề xuất.'}
        
        master = master_data[0]
        if master['TRANG_THAI'] != 'DRAFT':
            return {'success': False, 'message': 'Phiếu này đã được gửi hoặc xử lý rồi.'}

        self.db.execute_non_query(
            "DELETE FROM dbo.[DE XUAT BAO HANH_DS] WHERE MA_SO = ? AND CHON = 0", 
            (ma_so,)
        )
        self.recalculate_proposal(ma_so)
        
        # Lấy lại master sau khi recalculate
        master = self.db.get_data("SELECT * FROM dbo.[DE XUAT BAO HANH_MASTER] WHERE MA_SO = ?", (ma_so,))[0]
        amount = safe_float(master['GIA_TRI_CHI'])
        
        if amount <= 0:
             return {'success': False, 'message': 'Giá trị chi bằng 0, không thể gửi.'}

        from services.budget_service import BudgetService
        budget_service = BudgetService(self.db)
        
        reason = f"Thanh toán hoa hồng KH {master['KHACH_HANG']} theo đề xuất {ma_so}."
        result = budget_service.create_expense_request(
            user_code=user_code, dept_code='KD', budget_code='V01A', amount=amount, reason=reason
        )

        if result['success']:
            self.db.execute_non_query("UPDATE dbo.[DE XUAT BAO HANH_MASTER] SET TRANG_THAI = 'SUBMITTED' WHERE MA_SO = ?", (ma_so,))
            return {'success': True, 'message': f"Đã chuyển sang đề nghị thanh toán: {result['request_id']}"}
        else:
            return result