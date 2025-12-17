# forms.py
from flask_wtf import FlaskForm
from wtforms import StringField, DateField, SelectField, PasswordField
from wtforms.validators import DataRequired, Optional, Length, Regexp, EqualTo

# --- 1. FORM TRA CỨU (Đã làm) ---
class SalesLookupForm(FlaskForm):
    class Meta:
        csrf = False
    
    item_search = StringField('Từ khóa', validators=[
        Optional(),
        Length(max=50, message="Quá dài"),
        Regexp(r'^[\w\s\-\.\,]*$', message="Ký tự không hợp lệ")
    ])
    object_id = StringField('Mã KH', validators=[Optional(), Length(max=20)])

# --- 2. FORM ĐĂNG NHẬP (QUAN TRỌNG) ---
class LoginForm(FlaskForm):
    # Tự động bật CSRF Protection
    username = StringField('Tên đăng nhập', validators=[
        DataRequired(message="Vui lòng nhập Mã NV"),
        Length(max=50),
        Regexp(r'^[a-zA-Z0-9_\.]+$', message="Mã NV không chứa ký tự đặc biệt")
    ])
    password = PasswordField('Mật khẩu', validators=[DataRequired(message="Thiếu mật khẩu")])

# --- 3. FORM ĐỔI MẬT KHẨU ---
class ChangePasswordForm(FlaskForm):
    old_password = PasswordField('Mật khẩu hiện tại', validators=[DataRequired()])
    new_password = PasswordField('Mật khẩu mới', validators=[
        DataRequired(),
        Length(min=6, message="Mật khẩu tối thiểu 6 ký tự")
    ])
    confirm_password = PasswordField('Nhập lại mật khẩu', validators=[
        DataRequired(),
        EqualTo('new_password', message="Mật khẩu xác nhận không khớp")
    ])

# --- 4. FORM LỌC NGÀY THÁNG (CHO DASHBOARD) ---
class DateFilterForm(FlaskForm):
    class Meta:
        csrf = False # Form GET dùng cho Filter
    
    date_from = DateField('Từ ngày', format='%Y-%m-%d', validators=[Optional()])
    date_to = DateField('Đến ngày', format='%Y-%m-%d', validators=[Optional()])
    salesman_filter = StringField('Salesman', validators=[Optional(), Length(max=20)])