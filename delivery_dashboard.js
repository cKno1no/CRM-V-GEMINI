/**
 * static/js/delivery_dashboard.js
 * Logic xử lý cho Bảng điều phối giao vận
 */

// Biến toàn cục để lưu cấu hình và dữ liệu
let deliveryConfig = {
    groupedTasks: [],
    ungroupedTasks: [],
    todayStr: '',
    canEditPlanner: false,
    canEditDispatch: false
};

let confirmModal = null;

document.addEventListener('DOMContentLoaded', function() {
    // 1. KHỞI TẠO DỮ LIỆU TỪ DOM
    const contextEl = document.getElementById('delivery-context');
    if (!contextEl) {
        console.error("Không tìm thấy delivery-context!");
        return;
    }

    // Parse dữ liệu từ data attributes
    try {
        deliveryConfig.groupedTasks = JSON.parse(contextEl.dataset.groupedTasks || '[]');
        deliveryConfig.ungroupedTasks = JSON.parse(contextEl.dataset.ungroupedTasks || '[]');
        deliveryConfig.todayStr = contextEl.dataset.todayStr || '';
        
        // Parse boolean (Lưu ý: Python True/False sang JSON là true/false, nhưng data attr là chuỗi)
        // Cách an toàn nhất là so sánh chuỗi hoặc parse JSON nếu backend trả về true/false chuẩn
        deliveryConfig.canEditPlanner = JSON.parse(contextEl.dataset.canEditPlanner || 'false');
        deliveryConfig.canEditDispatch = JSON.parse(contextEl.dataset.canEditDispatch || 'false');
    } catch (e) {
        console.error("Lỗi parse dữ liệu JSON:", e);
    }

    // 2. KHỞI TẠO CÁC COMPONENT
    confirmModal = new bootstrap.Modal(document.getElementById('confirmDeliveryModal'));

    // 3. RENDER GIAO DIỆN BAN ĐẦU
    populatePlannerBoard();
    populateDispatchBoard();

    // 4. ĐĂNG KÝ SỰ KIỆN
    
    // Tìm kiếm
    $('#dispatchSearchInput').on('input', function() {
        const searchTerm = $(this).val().toLowerCase();
        $('#dispatch-view .delivery-card').each(function() {
            if ($(this).text().toLowerCase().includes(searchTerm)) $(this).show(); else $(this).hide();
        });
    });

    // Sắp xếp cột mặc định
    sortCardsInColumn('#col-hom-nay', 'plannedDate', 'asc');
    sortCardsInColumn('#col-sap-xep', 'plannedDate', 'asc');
    sortCardsInColumn('#col-trong-tuan', 'plannedDate', 'asc');
    sortCardsInColumn('#col-da-giao', 'actualDeliveryDate', 'desc');

    // Chặn in ấn/copy
    document.addEventListener('keydown', function(e) { 
        if ((e.ctrlKey || e.metaKey) && e.key === 'p') { 
            e.preventDefault(); alert("Chức năng in đã bị khóa."); 
        } 
    }, true);
    window.print = function() { alert("Chức năng in đã bị khóa."); };
    document.addEventListener('contextmenu', e => e.preventDefault());
});

// --- CÁC HÀM LOGIC ---

function openConfirmationModal(element) {
    // Kiểm tra quyền
    if (!deliveryConfig.canEditDispatch && element.closest('#dispatch-view')) {
         return;
    }
    const voucherId = element.dataset.voucherId;
    const voucherNo = element.querySelector('.item-title').textContent.split('(')[0].trim();
    const objectName = element.querySelector('.item-customer').textContent;
    
    document.getElementById('modalVoucherID').value = voucherId;
    document.getElementById('modalVoucherNo').textContent = voucherNo;
    document.getElementById('modalObjectName').textContent = objectName;
    
    const itemTableBody = document.getElementById('modalItemTableBody');
    itemTableBody.innerHTML = '<tr><td colspan="3" class="text-center"><i class="fas fa-spinner fa-spin"></i> Đang tải...</td></tr>';
    
    fetch(`/api/delivery/get_items/${voucherId}`)
        .then(response => response.json())
        .then(data => {
            itemTableBody.innerHTML = ''; 
            if (data && data.length > 0) {
                data.forEach(item => { 
                    itemTableBody.innerHTML += `<tr><td>${item.InventoryID}</td><td>${item.InventoryName}</td><td class="text-end">${item.ActualQuantity}</td></tr>`; 
                });
            } else { 
                itemTableBody.innerHTML = '<tr><td colspan="3" class="text-center text-danger">Không tìm thấy chi tiết.</td></tr>'; 
            }
        })
        .catch(e => { itemTableBody.innerHTML = `<tr><td colspan="3" class="text-center text-danger">Lỗi: ${e.message}</td></tr>`; });
    
    confirmModal.show();
}

function updateStatus(newStatus) {
    if (!deliveryConfig.canEditDispatch) { alert('Bạn không có quyền thực hiện thao tác này.'); return; }
    const voucherId = document.getElementById('modalVoucherID').value;
    if (!voucherId) return;
    
    fetch('/api/delivery/set_status', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ voucher_id: voucherId, new_status: newStatus })
    })
    .then(response => response.json())
    .then(data => { 
        if (data.success) { 
            alert(`Đã cập nhật trạng thái: ${newStatus}`); 
            confirmModal.hide(); 
            location.reload(); 
        } else { 
            alert('Lỗi: ' + data.message); 
        } 
    })
    .catch(e => alert('Lỗi kết nối: ' + e.message));
}

// --- LOGIC KÉO THẢ (DRAG & DROP) ---
function allowDrop(ev) { 
    if (!deliveryConfig.canEditPlanner) return; 
    ev.preventDefault(); 
    ev.currentTarget.classList.add('drag-over'); 
}

// Gán event listener cho các cột (đã có trong HTML ondrop/ondragover, ở đây xử lý dragleave)
// Lưu ý: Các hàm drag/drop được gọi trực tiếp từ HTML attribute nên phải expose ra global scope (window)
// Hoặc gán lại event bằng JS thuần bên trong DOMContentLoaded nếu muốn bỏ ondrop trong HTML.
// Ở đây giữ nguyên ondrop trong HTML để tương thích code cũ, nên cần gán hàm vào window.

window.allowDrop = allowDrop;

// Xử lý dragleave bằng JS
document.querySelectorAll('#planner-board .kanban-column').forEach(col => { 
    col.addEventListener('dragleave', (ev) => { ev.currentTarget.classList.remove('drag-over'); }); 
});

window.drag = function(ev) {
    if (!deliveryConfig.canEditPlanner) { ev.preventDefault(); return false; }
    ev.dataTransfer.setData("text/plain", ev.target.id);
    ev.dataTransfer.setData("voucher-id", ev.target.dataset.voucherId || "");
    ev.dataTransfer.setData("object-id", ev.target.dataset.objectId || "");
    ev.dataTransfer.setData("original-day", ev.target.dataset.originalDay || "POOL");
}

window.drop = function(ev) {
    if (!deliveryConfig.canEditPlanner) return; 
    ev.preventDefault(); 
    ev.currentTarget.classList.remove('drag-over');
    
    const elementId = ev.dataTransfer.getData("text/plain");
    const voucherId = ev.dataTransfer.getData("voucher-id");
    const objectId = ev.dataTransfer.getData("object-id");
    const oldDay = ev.dataTransfer.getData("original-day");
    
    const targetColumn = ev.currentTarget;
    const newDay = targetColumn.id; 
    const draggedElement = document.getElementById(elementId);
    
    if (!draggedElement) return;
    savePlannedDay(voucherId, objectId, newDay, oldDay, draggedElement, targetColumn);
}

function savePlannedDay(voucherId, objectId, newDay, oldDay, element, targetColumn) {
    if (!deliveryConfig.canEditPlanner) return; 
    let payload = { new_day: newDay, old_day: oldDay };
    if (objectId) payload.object_id = objectId; else payload.voucher_id = voucherId;
    
    fetch('/api/delivery/set_day', { 
        method: 'POST', 
        headers: { 'Content-Type': 'application/json' }, 
        body: JSON.stringify(payload) 
    })
    .then(response => response.json())
    .then(data => {
        if (data.success) { 
            targetColumn.querySelector('.task-list').appendChild(element); 
            element.dataset.originalDay = newDay; 
        } else { 
            alert('Lỗi khi lưu kế hoạch: ' + data.message); 
            location.reload(); 
        }
    }).catch(error => { 
        alert('Lỗi kết nối API: ' + error.message); 
        location.reload(); 
    });
}

// --- HÀM TẠO THẺ (CARD) ---
function formatCurrency(numStr) { 
    const num = parseFloat(numStr || 0); 
    return new Intl.NumberFormat('vi-VN').format(num); 
}

function createDeliveryCard(item) {
    let status_class = `status-${item.DeliveryStatus.toLowerCase()}`;
    const item_date_str = item.EarliestRequestDate_str || '9999-12-31';
    
    // Logic quá hạn
    const is_overdue = (item_date_str != '—' && item_date_str < deliveryConfig.todayStr) && (item.DeliveryStatus == 'Open');
    if (is_overdue) status_class += ' overdue';
    
    const draggable = deliveryConfig.canEditPlanner ? 'true' : 'false';
    const card_class = deliveryConfig.canEditPlanner ? '' : 'no-drag';
    const is_completed = item.DeliveryStatus == 'Da Giao';
    
    const plannedDateISO = item.Planned_Date_ISO || '9999-12-31';
    const actualDateISO = item.ActualDeliveryDate_ISO || '1900-01-01';

    let deliveryDateInfo = '';
    if (item.DeliveryStatus == 'Da Giao') {
         deliveryDateInfo = `<strong class="text-success" style="font-size: 0.85rem;"><i class="fas fa-check-circle me-1"></i> Đã Giao: ${item.ActualDeliveryDate_str}</strong><br>`;
    } else if (item.Planned_Day_Display) {
         deliveryDateInfo = `<strong class="text-primary" style="font-size: 0.85rem;"><i class="fas fa-calendar-check me-1"></i> Giao: ${item.Planned_Day_Display}</strong><br>`;
    }

    return `<div class="delivery-card ${status_class} ${card_class}" draggable="${is_completed ? 'false' : draggable}" ondragstart="drag(event)" 
             id="lxh-${item.VoucherID}" data-voucher-id="${item.VoucherID}" data-original-day="${item.Planned_Day || 'POOL'}" 
             data-planned-date="${plannedDateISO}" data-actual-delivery-date="${actualDateISO}">
            <div class="item-title">${item.VoucherNo} (${item.ItemCount} MH)</div>
            <div class="item-customer">${item.ObjectName}</div>
            <div class="item-details">
                ${deliveryDateInfo}
                Ngày LXH: ${item.VoucherDate_str} <br>
                RefNo02: ${item.RefNo02 || '—'}
                ${is_overdue ? `<br><strong class="text-danger">QUÁ HẠN (Y/C: ${item.EarliestRequestDate_str})</strong>` : ''}
            </div></div>`;
}

function createGroupCard(group) {
    let status_class = 'group-card'; 
    const item_date_str = group.EarliestRequestDate_str || '9999-12-31';
    const is_overdue = (item_date_str != '—' && item_date_str < deliveryConfig.todayStr);
    if (is_overdue) status_class += ' overdue';
    
    const draggable = deliveryConfig.canEditPlanner ? 'true' : 'false';
    const card_class = deliveryConfig.canEditPlanner ? '' : 'no-drag';
    const plannedDateISO = group.Planned_Date_ISO || '9999-12-31';

    let deliveryDateInfo = '';
    if (group.Planned_Day_Display) {
         deliveryDateInfo = `<strong class="text-primary" style="font-size: 0.85rem;"><i class="fas fa-calendar-check me-1"></i> Giao: ${group.Planned_Day_Display}</strong><br>`;
    }

    return `<div class="delivery-card ${status_class} ${card_class}" draggable="${draggable}" ondragstart="drag(event)" 
             id="group-${group.ObjectID}-${group.Planned_Day}" data-object-id="${group.ObjectID}" data-original-day="${group.Planned_Day || 'POOL'}" data-planned-date="${plannedDateISO}">
            <div class="item-title">${group.ObjectName}</div>
            <div class="item-customer">(${group.LXH_Count} Lệnh Xuất Hàng)</div>
            <div class="item-details">
                ${deliveryDateInfo}
                Trạng thái: <strong>${group.Status_Summary}</strong><br>
                Giá trị: ${formatCurrency(group.TotalValue)} <br>Y/C Giao sớm nhất: ${group.EarliestRequestDate_str} <br>
                RefNo02: ${group.RefNo02_str || '—'}
                ${is_overdue ? `<br><strong class="text-danger">CÓ PHIẾU QUÁ HẠN</strong>` : ''}
            </div></div>`;
}

// Nạp dữ liệu cho Tab 1
function populatePlannerBoard() {
    document.querySelectorAll('#planner-board .task-list').forEach(list => { list.innerHTML = ''; });
    
    deliveryConfig.groupedTasks.forEach(group => {
        const plannedDay = group.Planned_Day || 'POOL'; 
        const column = document.getElementById(`task-list-${plannedDay}`);
        if (column) column.innerHTML += createGroupCard(group);
    });
    
    deliveryConfig.ungroupedTasks.forEach(item => {
        let plannedDay;
        if (item.DeliveryStatus == 'Da Giao') plannedDay = 'COMPLETED'; 
        else if (item.DeliveryStatus == 'Da Soan') plannedDay = item.Planned_Day || 'POOL';
        
        if (plannedDay) {
            const column = document.getElementById(`task-list-${plannedDay}`);
            if (column) column.innerHTML += createDeliveryCard(item);
        }
    });
}

// Gán sự kiện cho Tab 2
function populateDispatchBoard() {
    document.querySelectorAll('#dispatch-view .delivery-card').forEach(card => {
        card.addEventListener('click', function() { if (deliveryConfig.canEditDispatch) openConfirmationModal(this); });
        card.draggable = false; 
    });
}

function sortCardsInColumn(columnId, sortField, sortDir = 'asc') {
    const list = document.querySelector(`${columnId} .task-list`);
    if (!list) return;
    const cards = Array.from(list.querySelectorAll('.delivery-card'));
    
    cards.sort((a, b) => {
        const valA = a.dataset[sortField] || (sortDir === 'asc' ? '9999-12-31' : '1900-01-01');
        const valB = b.dataset[sortField] || (sortDir === 'asc' ? '9999-12-31' : '1900-01-01');
        
        if (sortDir === 'asc') {
            return valA.localeCompare(valB); // ASC (Cũ -> Mới)
        } else {
            return valB.localeCompare(valA); // DESC (Mới -> Cũ)
        }
    });
    
    cards.forEach(card => list.appendChild(card));
}