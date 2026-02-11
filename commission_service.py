# services/commission_service.py

from flask import current_app
from db_manager import DBManager, safe_float
from datetime import datetime
import config
import pyodbc
import os 

class CommissionService:
    def __init__(self, db_manager: DBManager):
        self.db = db_manager
        self.TABLE_RECIPIENTS = getattr(config, 'TABLE_COMMISSION_RECIPIENTS', '[dbo].[DE XUAT BAO HANH_DETAIL]')

    def create_proposal(self, user_code, customer_id, date_from, date_to, commission_rate_percent, note=""):
        conn = None
        try:
            conn = self.db.get_transaction_connection()
            cursor = conn.cursor()
            
            sql = f"EXEC {config.SP_CREATE_COMMISSION} ?, ?, ?, ?, ?"
            params = (user_code, customer_id, date_from, date_to, float(commission_rate_percent))
            
            cursor.execute(sql, params)
            
            row = cursor.fetchone()
            new_voucher_id = None
            if row:
                new_voucher_id = row[0]
                if note:
                    update_sql = f"UPDATE {config.TABLE_COMMISSION_MASTER} SET GHI_CHU = ? WHERE MA_SO = ?"
                    cursor.execute(update_sql, (note, new_voucher_id))
            
            conn.commit()
            return new_voucher_id

        except Exception as e:
            current_app.logger.error(f"Lỗi tạo phiếu hoa hồng: {e}")
            if conn: conn.rollback()
            return None
        finally:
            if conn: conn.close()

    def recalculate_proposal(self, ma_so):
        query = f"""
            UPDATE M
            SET 
                DOANH_SO_CHON = ISNULL(D.TotalSelected, 0),
                GIA_TRI_CHI = ISNULL(D.TotalSelected, 0) * (ISNULL(M.MUC_CHI_PERCENT, 0) / 100.0)
            FROM {config.TABLE_COMMISSION_MASTER} M
            OUTER APPLY (
                SELECT SUM(DOANH_SO) as TotalSelected 
                FROM {config.TABLE_COMMISSION_DETAIL} 
                WHERE MA_SO = M.MA_SO AND CHON = 1
            ) D
            WHERE M.MA_SO = ?
        """
        self.db.execute_non_query(query, (ma_so,))

    def toggle_invoice(self, detail_id, is_checked):
        self.db.execute_non_query(
            f"UPDATE {config.TABLE_COMMISSION_DETAIL} SET CHON = ? WHERE VoucherID = ?", 
            (1 if is_checked else 0, detail_id)
        )
        row = self.db.get_data(f"SELECT MA_SO FROM {config.TABLE_COMMISSION_DETAIL} WHERE VoucherID = ?", (detail_id,))
        if row:
            self.recalculate_proposal(row[0]['MA_SO'])
            return True
        return False

    def add_manual_detail(self, ma_so, contact_name, bank_name, bank_account, amount):
        try:
            query = f"""
                INSERT INTO {self.TABLE_RECIPIENTS} 
                ([MA SO], [NHAN SU], [MUC CHI], [NGAN HANG], [SO TAI KHOAN], [NGAY LAM], [GHI CHU])
                VALUES (?, ?, ?, ?, ?, GETDATE(), N'Chi trực tiếp')
            """
            self.db.execute_non_query(query, (ma_so, contact_name, float(amount), bank_name, bank_account))
            return True
        except Exception as e:
            current_app.logger.error(f"Lỗi add_manual_detail: {e}")
            return False

    def get_proposal_recipients(self, ma_so):
        query = f"""
            SELECT [ID], [NHAN SU], [MUC CHI], [NGAN HANG], [SO TAI KHOAN]
            FROM {self.TABLE_RECIPIENTS}
            WHERE [MA SO] = ?
        """
        return self.db.get_data(query, (ma_so,))

    # --- HÀM SINH PHIẾU IN (ĐÃ FIX KEYERROR USERCODE) ---
    # --- HÀM SINH PHIẾU IN (NEW DESIGN: MODERN LANDSCAPE) ---
    def generate_commission_voucher_html(self, ma_so):
        try:
            print(f">>> BẮT ĐẦU TẠO HTML (MODERN UI) CHO PHIẾU: {ma_so}") 
            
            upload_path = config.UPLOAD_FOLDER_PATH
            if not os.path.exists(upload_path): os.makedirs(upload_path)
            
            # 1. LẤY DỮ LIỆU (Giữ nguyên logic cũ)
            query_master = f"""
                SELECT 
                    M.*, 
                    ISNULL(C.ObjectName, M.KHACH_HANG) as FullObjectName,
                    ISNULL(C.ShortObjectName, M.KHACH_HANG) as ShortName
                FROM {config.TABLE_COMMISSION_MASTER} M
                LEFT JOIN {config.ERP_IT1202} C ON M.KHACH_HANG = C.ObjectID
                WHERE M.MA_SO = ?
            """
            master_data = self.db.get_data(query_master, (ma_so,))
            if not master_data: return None
            master = master_data[0]
            customer_id = master.get('KHACH_HANG', '')

            # Lấy tên nhân viên
            creator_code = master.get('NGUOI_LAM', 'N/A')
            creator_name = creator_code
            try:
                user_table = getattr(config, 'TEN_BANG_NGUOI_DUNG', '[GD - NGUOI DUNG]')
                u_data = self.db.get_data(f"SELECT SHORTNAME FROM {user_table} WHERE USERCODE = ?", (creator_code,))
                if u_data: creator_name = u_data[0]['SHORTNAME']
            except: pass
            creator_display = f"{creator_name} ({creator_code})"

            # Số liệu AR & Sales
            q_debt = f"SELECT TotalDebt, TotalOverdueDebt FROM {config.CRM_AR_AGING_SUMMARY} WHERE ObjectID = ?"
            debt_data = self.db.get_data(q_debt, (customer_id,))
            cong_no_qua_han = safe_float(debt_data[0]['TotalOverdueDebt']) if debt_data else 0
            cong_no_hien_tai = safe_float(debt_data[0]['TotalDebt']) if debt_data else 0 
            
            current_year = datetime.now().year
            q_sales = f"""
                SELECT SUM(ConvertedAmount) as SalesYTD FROM {config.ERP_GIAO_DICH}
                WHERE ObjectID = ? AND TranYear = ? AND CreditAccountID LIKE '{config.ACC_DOANH_THU}'
            """
            sales_data = self.db.get_data(q_sales, (customer_id, current_year))
            doanh_so_ky = safe_float(sales_data[0]['SalesYTD']) if sales_data else 0
            doanh_so_chi = safe_float(master.get('DOANH_SO_CHON', 0))
            tong_tien_chi = safe_float(master.get('GIA_TRI_CHI', 0))
            ty_le_chi = (tong_tien_chi / doanh_so_chi * 100) if doanh_so_chi > 0 else 0

            # Người nhận
            query_recipients = f"""
                SELECT D.[NHAN SU], D.[MUC CHI], D.[NGAN HANG], D.[SO TAI KHOAN], D.[GHI CHU],
                    ISNULL(C.[SO DTDD 1], '') as Phone, ISNULL(C.[CHUC VU], '') as ChucVu
                FROM {self.TABLE_RECIPIENTS} D
                LEFT JOIN [dbo].[HD_NHAN SU LIEN HE] C ON D.[NHAN SU] = C.[TEN HO] + ' ' + C.[TEN THUONG GOI] AND C.[CONG TY] = ? 
                WHERE D.[MA SO] = ?
            """
            recipients = self.db.get_data(query_recipients, (customer_id, ma_so))
            
            # --- RENDER ROWS (MODERN STYLE) ---
            rows_html = ""
            # Cố định tối thiểu 5 dòng cho đẹp trang A4
            total_rows = max(len(recipients), 5) 
            
            for i in range(total_rows):
                if i < len(recipients):
                    r = recipients[i]
                    amt = "{:,.0f}".format(safe_float(r['MUC CHI']))
                    rows_html += f"""
                    <tr>
                        <td class="fw-bold">{r.get('NHAN SU','')}</td>
                        <td>{r.get('ChucVu','')}</td>
                        <td class="text-end text-primary fw-bold">{amt}</td>
                        <td>{r.get('NHAN SU','')}</td>
                        <td class="font-monospace">{r.get('SO TAI KHOAN','')}</td>
                        <td>{r.get('NGAN HANG','')}</td>
                        <td>{r.get('GHI_CHU') or r.get('GHI CHU', '')}</td>
                        <td>{r.get('Phone','')}</td>
                    </tr>
                    """
                else:
                    # Dòng kẻ mờ cho đẹp
                    rows_html += '<tr><td colspan="8" style="height: 35px;"></td></tr>'

            # --- FORMAT DỮ LIỆU HIỂN THỊ ---
            def fmt_date(d): return d.strftime('%d/%m/%Y') if isinstance(d, datetime) else '...'
            date_from = fmt_date(master.get('TU_NGAY'))
            date_to = fmt_date(master.get('DEN_NGAY'))
            
            days_diff = 0
            if master.get('DEN_NGAY') and master.get('TU_NGAY'):
                try: days_diff = (master['DEN_NGAY'] - master['TU_NGAY']).days
                except: pass
            
            print_time = datetime.now().strftime("Ngày %d tháng %m năm %Y, %H:%M")

            # --- 2. HTML TEMPLATE (MODERN DASHBOARD STYLE) ---
            html_content = f"""
            <!DOCTYPE html>
            <html lang="vi">
            <head>
                <meta charset="UTF-8">
                <title>Phieu Chi {ma_so}</title>
                <style>
                    /* CẤU HÌNH TRANG IN A4 NGANG */
                    @page {{ size: A4 landscape; margin: 10mm; }}
                    @media print {{ 
                        body {{ -webkit-print-color-adjust: exact; print-color-adjust: exact; }} 
                    }}

                    :root {{ --primary: #2563eb; --secondary: #64748b; --dark: #1e293b; --light: #f8fafc; --border: #e2e8f0; }}
                    
                    body {{ 
                        font-family: 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; 
                        color: var(--dark); margin: 0; padding: 0; background: #fff; font-size: 13px; line-height: 1.4;
                    }}

                    /* HEADER */
                    .header {{ display: flex; justify-content: space-between; align-items: center; border-bottom: 2px solid var(--primary); padding-bottom: 15px; margin-bottom: 20px; }}
                    .brand {{ display: flex; align-items: center; gap: 15px; }}
                    .logo {{ height: 45px; }} 
                    .doc-title {{ font-size: 22px; font-weight: 800; color: var(--primary); text-transform: uppercase; letter-spacing: 0.5px; }}
                    .meta-info {{ text-align: right; font-size: 11px; color: var(--secondary); }}
                    .voucher-badge {{ background: var(--primary); color: white; padding: 4px 10px; border-radius: 4px; font-weight: bold; font-size: 14px; margin-top: 5px; display: inline-block; }}

                    /* DASHBOARD GRID */
                    .dashboard {{ display: grid; grid-template-columns: 1.2fr 1.8fr 1fr; gap: 20px; margin-bottom: 25px; }}
                    
                    /* CARD STYLES */
                    .card {{ border: 1px solid var(--border); border-radius: 8px; padding: 15px; background: var(--light); }}
                    .card-header {{ font-size: 11px; text-transform: uppercase; color: var(--secondary); font-weight: 700; margin-bottom: 10px; border-bottom: 1px solid #ddd; padding-bottom: 5px; }}
                    
                    /* Card 1: Khách hàng */
                    .customer-name {{ font-size: 16px; font-weight: 700; color: var(--dark); margin-bottom: 5px; }}
                    .info-row {{ display: flex; justify-content: space-between; margin-bottom: 3px; font-size: 12px; }}
                    .note-box {{ background: #fff3cd; color: #856404; padding: 8px; border-radius: 4px; margin-top: 8px; font-style: italic; border: 1px solid #ffeeba; }}

                    /* Card 2: Metrics (Số liệu) */
                    .metrics-grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 15px; }}
                    .metric-item {{ background: #fff; padding: 8px; border-radius: 4px; border: 1px solid var(--border); }}
                    .metric-label {{ font-size: 10px; color: var(--secondary); text-transform: uppercase; }}
                    .metric-value {{ font-size: 14px; font-weight: 700; color: var(--dark); margin-top: 2px; }}
                    .text-danger {{ color: #dc2626; }} .text-success {{ color: #16a34a; }}

                    /* Card 3: Total (Tiền chi) */
                    .total-card {{ background: var(--primary); color: white; text-align: center; display: flex; flex-direction: column; justify-content: center; }}
                    .total-card .card-header {{ color: rgba(255,255,255,0.8); border-color: rgba(255,255,255,0.3); }}
                    .total-amount {{ font-size: 28px; font-weight: 800; letter-spacing: -0.5px; }}
                    .total-percent {{ font-size: 14px; background: rgba(255,255,255,0.2); padding: 2px 8px; border-radius: 10px; display: inline-block; margin-top: 5px; }}

                    /* TABLE */
                    table {{ width: 100%; border-collapse: collapse; font-size: 12px; }}
                    th {{ background: var(--dark); color: #fff; padding: 10px; text-align: left; font-weight: 600; text-transform: uppercase; font-size: 11px; }}
                    td {{ border-bottom: 1px solid var(--border); padding: 10px; vertical-align: middle; }}
                    tr:nth-child(even) {{ background-color: #f8fafc; }}
                    .text-end {{ text-align: right; }}
                    .fw-bold {{ font-weight: 600; }}
                    .font-monospace {{ font-family: 'Consolas', monospace; letter-spacing: -0.5px; }}

                    /* FOOTER */
                    .footer {{ display: grid; grid-template-columns: 1fr 1fr 1fr; margin-top: 40px; text-align: center; page-break-inside: avoid; }}
                    .sign-box {{ padding: 20px; }}
                    .sign-title {{ font-weight: 700; text-transform: uppercase; font-size: 12px; color: var(--dark); margin-bottom: 60px; }}
                    .sign-placeholder {{ border-top: 1px dashed var(--secondary); width: 60%; margin: 0 auto; padding-top: 5px; font-size: 11px; color: var(--secondary); font-style: italic; }}

                </style>
            </head>
            <body>
                <div class="header">
                    <div class="brand">
                        <img src="https://crm.stdd.vn/static/img/logo_std.png" class="logo" alt="STD Logo">
                        <div>
                            <div class="doc-title">Phiếu Đề Xuất Chi Bảo Hành</div>
                            <div class="voucher-badge">{ma_so}</div>
                        </div>
                    </div>
                    <div class="meta-info">
                        <div>Thời gian in: {print_time}</div>
                        <div>Hệ thống: Titan OS Finance</div>
                    </div>
                </div>

                <div class="dashboard">
                    <div class="card">
                        <div class="card-header"><i class="fas fa-building"></i> Thông tin chung</div>
                        <div class="customer-name">{master['FullObjectName']}</div>
                        <div class="info-row">
                            <span>Người đề xuất:</span> <strong>{creator_display}</strong>
                        </div>
                        <div class="info-row">
                            <span>Giai đoạn:</span> <span>{date_from} - {date_to} ({days_diff} ngày)</span>
                        </div>
                        <div class="note-box">
                            <strong>Ghi chú:</strong> {master.get('GHI_CHU') or 'Không có ghi chú'}
                        </div>
                    </div>

                    <div class="card">
                        <div class="card-header">Cơ sở tính toán</div>
                        <div class="metrics-grid">
                            <div class="metric-item">
                                <div class="metric-label">Doanh số Tính Chi</div>
                                <div class="metric-value">{doanh_so_chi:,.0f}</div>
                            </div>
                            <div class="metric-item">
                                <div class="metric-label">Doanh số Kỳ (YTD)</div>
                                <div class="metric-value">{doanh_so_ky:,.0f}</div>
                            </div>
                            <div class="metric-item">
                                <div class="metric-label">Công nợ Hiện tại</div>
                                <div class="metric-value text-primary">{cong_no_hien_tai:,.0f}</div>
                            </div>
                            <div class="metric-item">
                                <div class="metric-label">Công nợ Quá hạn</div>
                                <div class="metric-value text-danger">{cong_no_qua_han:,.0f}</div>
                            </div>
                        </div>
                    </div>

                    <div class="card total-card">
                        <div class="card-header" style="color: white;">Tổng giá trị đề xuất</div>
                        <div class="total-amount">{tong_tien_chi:,.0f} <small style="font-size: 14px; font-weight: normal;">VND</small></div>
                        <div><span class="total-percent">Tỷ lệ: {ty_le_chi:.1f}%</span></div>
                    </div>
                </div>

                <table>
                    <thead>
                        <tr>
                            <th width="18%">Người thụ hưởng</th>
                            <th width="12%">Chức vụ</th>
                            <th width="12%" class="text-end">Số tiền</th>
                            <th width="15%">Tên TK Ngân hàng</th>
                            <th width="12%">Số TK</th>
                            <th width="10%">Ngân hàng</th>
                            <th width="10%">Ghi chú</th>
                            <th width="11%">SĐT</th>
                        </tr>
                    </thead>
                    <tbody>
                        {rows_html}
                    </tbody>
                </table>

                <div class="footer">
                    <div class="sign-box">
                        <div class="sign-title">Nhân sự lập phiếu</div>
                        <div class="sign-placeholder">Ký và ghi rõ họ tên</div>
                    </div>
                    <div class="sign-box">
                        <div class="sign-title">Kế toán trưởng</div>
                        <div class="sign-placeholder">Ký và ghi rõ họ tên</div>
                    </div>
                    <div class="sign-box">
                        <div class="sign-title">Giám đốc phê duyệt</div>
                        <div class="sign-placeholder">Ký và ghi rõ họ tên</div>
                    </div>
                </div>

            </body>
            </html>
            """
            
            # 5. LƯU FILE
            filename = f"WARRANTY_{ma_so}_{datetime.now().strftime('%Y%m%d%H%M')}.html"
            save_path = os.path.join(upload_path, filename)
            
            with open(save_path, "w", encoding="utf-8") as f:
                f.write(html_content)
            
            print(f"✅ ĐÃ TẠO FILE HTML: {save_path}")
            return filename
            
        except Exception as e:
            print(f"❌❌❌ ERROR Generating HTML: {str(e)}") 
            import traceback
            traceback.print_exc()
            return None

    def submit_to_payment_request(self, ma_so, user_code):
        print(f"-> Submit Payment Request cho {ma_so}...")
        master_data = self.db.get_data(f"SELECT * FROM {config.TABLE_COMMISSION_MASTER} WHERE MA_SO = ?", (ma_so,))
        if not master_data: return {'success': False, 'message': 'Không tìm thấy phiếu.'}
        
        master = master_data[0]
        if master['TRANG_THAI'] != 'DRAFT': return {'success': False, 'message': 'Phiếu đã xử lý.'}

        # Dọn dẹp & Tính lại
        self.db.execute_non_query(f"DELETE FROM {config.TABLE_COMMISSION_DETAIL} WHERE MA_SO = ? AND CHON = 0", (ma_so,))
        self.recalculate_proposal(ma_so)
        
        master = self.db.get_data(f"SELECT * FROM {config.TABLE_COMMISSION_MASTER} WHERE MA_SO = ?", (ma_so,))[0]
        amount = safe_float(master['GIA_TRI_CHI'])
        
        if amount <= 0: return {'success': False, 'message': 'Giá trị chi bằng 0.'}

        # [GỌI HÀM TẠO FILE VÀ KIỂM TRA]
        attached_file = self.generate_commission_voucher_html(ma_so)
        if attached_file:
            print(f"-> File đính kèm: {attached_file}")
        else:
            print("-> ⚠️ Cảnh báo: Không tạo được file đính kèm!")

        # Lấy người nhận để tạo Reason
        recipients = self.get_proposal_recipients(ma_so)
        rec_txt = "; ".join([f"{r['NHAN SU']} ({safe_float(r['MUC CHI']):,.0f})" for r in recipients])
        
        reason = f"TT Bảo hành/HH KH {master['KHACH_HANG']} ({ma_so}). {master.get('GHI_CHU','')}. Nhận: {rec_txt}"

        from services.budget_service import BudgetService
        budget_service = BudgetService(self.db)
        
        result = budget_service.create_expense_request(
            user_code=user_code, 
            dept_code='KD', 
            budget_code='V01A', 
            amount=amount, 
            reason=reason,
            object_id=master['KHACH_HANG'],
            attachments=attached_file 
        )

        if result['success']:
            self.db.execute_non_query(f"UPDATE {config.TABLE_COMMISSION_MASTER} SET TRANG_THAI = 'SUBMITTED' WHERE MA_SO = ?", (ma_so,))
            msg = f"Đã chuyển đề nghị: {result['request_id']}."
            if attached_file: msg += " (Đã đính kèm phiếu in)"
            return {'success': True, 'message': msg}
        else:
            return result