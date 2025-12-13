document.addEventListener("DOMContentLoaded", () => {
    loadUsers();
    
    // Search User Logic
    document.getElementById('userSearch').addEventListener('input', function(e) {
        const term = e.target.value.toLowerCase();
        const rows = document.querySelectorAll('#userTableBody tr');
        rows.forEach(row => {
            const text = row.innerText.toLowerCase();
            row.style.display = text.includes(term) ? '' : 'none';
        });
    });
});

// --- USER MANAGEMENT ---
function loadUsers() {
    fetch('/api/users/list')
        .then(r => r.json())
        .then(data => {
            const tbody = document.getElementById('userTableBody');
            tbody.innerHTML = '';
            data.forEach(u => {
                const roleClass = u.ROLE === 'ADMIN' ? 'bg-danger' : (u.ROLE === 'GM' ? 'bg-warning text-dark' : 'bg-secondary');
                const row = `<tr>
                    <td class="fw-bold font-monospace">${u.USERCODE}</td>
                    <td>${u.SHORTNAME || u.USERNAME}</td>
                    <td><span class="badge ${roleClass}">${u.ROLE || 'N/A'}</span></td>
                    <td>${u['BO PHAN'] || ''}</td>
                    <td>${u['CAP TREN'] || ''}</td>
                    <td class="text-center"><button class="btn btn-sm btn-outline-primary" onclick="openEditUser('${u.USERCODE}')"><i class="fas fa-edit"></i></button></td>
                </tr>`;
                tbody.insertAdjacentHTML('beforeend', row);
            });
        });
}

const editModal = new bootstrap.Modal(document.getElementById('editUserModal'));

function openEditUser(userCode) {
    fetch(`/api/users/detail/${userCode}`)
        .then(r => r.json())
        .then(u => {
            document.getElementById('md_usercode').value = u.USERCODE;
            document.getElementById('md_shortname').value = u.SHORTNAME;
            document.getElementById('md_role').value = u.ROLE;
            document.getElementById('md_captren').value = u['CAP TREN'];
            document.getElementById('md_bophan').value = u['BO PHAN'];
            document.getElementById('md_password').value = ''; // Không hiện pass cũ
            editModal.show();
        });
}

function submitEditUser() {
    if(!confirm("Xác nhận cập nhật thông tin user này?")) return;
    
    const data = {
        user_code: document.getElementById('md_usercode').value,
        shortname: document.getElementById('md_shortname').value,
        role: document.getElementById('md_role').value.toUpperCase(),
        cap_tren: document.getElementById('md_captren').value.toUpperCase(),
        bo_phan: document.getElementById('md_bophan').value,
        password: document.getElementById('md_password').value
    };
    
    fetch('/api/users/update', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify(data)
    }).then(r => r.json()).then(res => {
        if(res.success) {
            alert("Cập nhật thành công!");
            editModal.hide();
            loadUsers();
        } else {
            alert("Lỗi cập nhật!");
        }
    });
}

// --- PERMISSIONS MANAGEMENT ---
let currentRole = null;
let permissionMatrix = {};

function loadPermissions() {
    fetch('/api/permissions/matrix')
        .then(r => r.json())
        .then(data => {
            permissionMatrix = data.matrix;
            const roleList = document.getElementById('roleList');
            roleList.innerHTML = '';
            
            data.roles.forEach(role => {
                // Không cho sửa quyền ADMIN (vì mặc định full quyền)
                if(role === 'ADMIN') return; 
                
                const btn = document.createElement('button');
                btn.className = 'list-group-item list-group-item-action d-flex justify-content-between align-items-center';
                btn.innerHTML = `<span>${role}</span> <i class="fas fa-chevron-right small text-muted"></i>`;
                btn.onclick = () => selectRole(role, btn);
                roleList.appendChild(btn);
            });
        });
}

function selectRole(role, btn) {
    currentRole = role;
    document.getElementById('selectedRoleName').textContent = role;
    
    // UI Active
    document.querySelectorAll('#roleList button').forEach(b => {
        b.classList.remove('role-active');
        b.querySelector('i').className = 'fas fa-chevron-right small text-muted';
    });
    btn.classList.add('role-active');
    btn.querySelector('i').className = 'fas fa-check small text-white';
    
    // Reset Checks
    document.querySelectorAll('.feature-check').forEach(chk => chk.checked = false);
    
    // Load Checked
    if (permissionMatrix[role]) {
        permissionMatrix[role].forEach(feat => {
            const chk = document.getElementById(`feat_${feat}`);
            if(chk) chk.checked = true;
        });
    }
}

function savePermissions() {
    if (!currentRole) { alert("Vui lòng chọn một Role trước."); return; }
    
    const selectedFeatures = [];
    document.querySelectorAll('.feature-check:checked').forEach(chk => {
        selectedFeatures.push(chk.value);
    });
    
    fetch('/api/permissions/save', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({ role_id: currentRole, features: selectedFeatures })
    }).then(r => r.json()).then(res => {
        if(res.success) {
            alert(`Đã lưu ${selectedFeatures.length} quyền cho Role: ${currentRole}`);
            permissionMatrix[currentRole] = selectedFeatures; // Update local cache
        } else {
            alert("Lỗi khi lưu!");
        }
    });
}