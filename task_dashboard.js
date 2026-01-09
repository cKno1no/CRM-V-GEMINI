/**
 * static/js/task_dashboard.js
 * Logic xử lý cho Dashboard Quản lý Đầu việc
 */

// Biến toàn cục lưu cấu hình
let taskConfig = {
    dailyTasks: [],
    historyTasks: [],
    isAdmin: false,
    baseUrl: '/task_dashboard', 
    viewMode: 'USER',
    activeFilter: 'ALL',
    searchTerm: ''
};

let taskManagerInstance = null;

// --- HÀM HELPER PARSE JSON AN TOÀN ---
function safeJsonParse(jsonString) {
    if (!jsonString) return [];
    try {
        const safeString = jsonString.replace(/\bNaN\b/g, "null");
        return JSON.parse(safeString);
    } catch (e) {
        console.error("Lỗi parse JSON:", e);
        return [];
    }
}

// Lớp quản lý Task
class TaskManager {
    constructor(tasks) {
        this.tasks = (tasks.daily_tasks || []).concat(tasks.history_tasks || []);
        this.tasksMap = this.tasks.reduce((map, task) => {
            if (task && task.TaskID) {
                map[task.TaskID] = task;
            }
            return map;
        }, {});
    }

    getTask(id) {
        return this.tasksMap[id];
    }

    openUpdateModal(taskId) {
        loadEligibleHelpers(); 
        const task = this.getTask(taskId);
        
        if (!task) { 
            console.error("Không tìm thấy task ID:", taskId, "trong cache.");
            alert('Task không tồn tại hoặc chưa được tải!'); 
            return; 
        }

        $('#updateTaskIdDisplay').text(taskId);
        $('#update_task_id').val(taskId);
        $('#updateTaskTitleDisplay').text(task.Title || 'N/A'); 
        
        const objectIdInput = $('#update_object_id');
        const isReadOnly = task.DetailContent || task.CompletedDate; 
        
        if (isReadOnly) {
            objectIdInput.attr('readonly', true).css('background-color', '#e9ecef');
        } else {
            objectIdInput.attr('readonly', false).css('background-color', '');
        }

        if (task.ClientName) {
             objectIdInput.val(task.ObjectID + ' - ' + task.ClientName);
        } else {
             objectIdInput.val(task.ObjectID || '');
        }

        let progress = task.ProgressPercentage;
        if (progress === null || isNaN(progress)) progress = 0;
        $('#progress_percent').val(progress); 
        
        $('#update_detail_content').val(''); 
        
        $('#log_type_select').show().val('PROGRESS');
        $('#helpCallDisplay').css('display', 'none');
        $('#helperSelectionBox').hide(); 

        $('#logHistoryContainer').html('<p class="text-center text-muted">Đang tải lịch sử...</p>');
        
        // Gọi API lấy lịch sử
        fetch(`/api/task/history/${taskId}`)
            .then(r => r.json())
            .then(logs => {
                renderLogHistory(logs, taskConfig.isAdmin);
            })
            .catch(e => {
                $('#logHistoryContainer').html('<p class="alert alert-danger">Lỗi tải lịch sử Log.</p>');
            });
                
        $('#updateTaskModal').modal('show'); 
    }
    
    openSupervisorNoteModal(logId) {
         $('#noteLogIdDisplay').text(logId);
         $('#note_log_id').val(logId);
         // Reset nội dung để người dùng nhập mới, không bị dính nội dung cũ
         $('#note_content').val(''); 
         $('#supervisorNoteModal').modal('show');
    }
}

// --- INIT ---
document.addEventListener('DOMContentLoaded', function() {
    const contextEl = document.getElementById('task-context');
    if (!contextEl) {
        console.error("Lỗi: Không tìm thấy phần tử #task-context");
        return;
    }

    try {
        taskConfig.dailyTasks = safeJsonParse(contextEl.dataset.dailyTasks);
        taskConfig.historyTasks = safeJsonParse(contextEl.dataset.historyTasks);
        
        taskConfig.isAdmin = (contextEl.dataset.isAdmin === 'True' || contextEl.dataset.isAdmin === 'true');
        taskConfig.baseUrl = contextEl.dataset.baseUrl || '/task_dashboard';
        taskConfig.viewMode = contextEl.dataset.viewMode || 'USER';
        taskConfig.activeFilter = contextEl.dataset.activeFilter || 'ALL';
        taskConfig.searchTerm = contextEl.dataset.searchTerm || '';

    } catch(e) {
        console.error("Lỗi khởi tạo cấu hình Task:", e);
    }

    // Khởi tạo Manager
    taskManagerInstance = new TaskManager({
        daily_tasks: taskConfig.dailyTasks,
        history_tasks: taskConfig.historyTasks
    });

    // DataTables
    if ($('#task-history-table').length) {
        $('#task-history-table').DataTable({
            "paging": true, "pageLength": 15, "lengthChange": true, "searching": false, 
            "ordering": true, "info": true, "order": [[1, 'desc']], "dom": 'lfrtip',
            "language": {
                "zeroRecords": "Không tìm thấy dữ liệu.",
                "info": "Hiển thị _START_ - _END_ / _TOTAL_",
                "paginate": { "next": "Sau", "previous": "Trước" }
            }
        });
    }

    startPolling();
    // [BỔ SUNG] Tự động set 100% khi chọn Hoàn thành
    $('#log_type_select').on('change', function() {
        if ($(this).val() === 'REQUEST_CLOSE') {
            $('#progress_percent').val(100);
        }
    });
    // Event Listeners
    $('#create_object_id_input').on('input', debounce(function() {
         const inputElement = $('#create_object_id_input');
         const dropdown = $('#create_kh_search_results');
         timKhachHangAutocomplete(inputElement.val(), dropdown);
    }, 300));
    
    $('#create_kh_search_results').on('change', function() {
        selectClient('create_kh_search_results', 'create_object_id_input', 'create_object_id_selected');
    });

    $('#helpCallBtn').on('click', function() {
        $('#log_type_select').hide();
        $('#helpCallDisplay').css('display', 'flex'); 
        if ($('#log_type_select option[value="HELP_CALL"]').length === 0) {
            $('#log_type_select').append('<option value="HELP_CALL">HELP</option>');
        }
        $('#log_type_select').val('HELP_CALL');
        $('#helperSelectionBox').slideDown(); 
        loadEligibleHelpers();
        setTimeout(() => { $('#helper_code_select').focus(); }, 100);
    });

    // Submit form cập nhật chính
    $('#updateTaskForm').on('submit', function(e) {
        e.preventDefault();
        const form = $(this);
        const selectedHelpers = form.find('#helper_code_select').val() || [];
        const logType = form.find('#log_type_select').val();

        if (logType === 'HELP_CALL' && selectedHelpers.length === 0) {
             alert('Vui lòng chọn ít nhất 1 người hoặc bộ phận để gửi yêu cầu.'); return;
        }
        
        const payload = {
            task_id: form.find('#update_task_id').val(),
            object_id: form.find('#update_object_id').val().split(' - ')[0], 
            content: form.find('#update_detail_content').val(), 
            progress_percent: parseInt(form.find('#progress_percent').val()) || 0,
            log_type: logType,
            helper_codes: selectedHelpers
        };
        
        fetch('/api/task/log_progress', {
            method: 'POST', headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        })
        .then(r => r.json()).then(result => {
            alert(result.message);
            if (result.success) window.location.reload(); 
        })
        .catch(error => { alert('Lỗi kết nối.'); });
    });

    // Submit form phản hồi cấp trên
    $('#supervisorNoteForm').on('submit', function(e) {
        e.preventDefault();
        const logId = $('#note_log_id').val();
        const feedback = $('#note_content').val();

        if (!feedback.trim()) {
            alert("Vui lòng nhập nội dung phản hồi.");
            return;
        }

        fetch('/api/task/add_feedback', {
            method: 'POST', headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ log_id: logId, feedback: feedback })
        })
        .then(r => r.json()).then(result => {
            alert(result.message);
            if (result.success) { $('#supervisorNoteModal').modal('hide'); window.location.reload(); }
        })
        .catch(error => alert('Lỗi khi lưu phản hồi.'));
    });
    
    document.addEventListener('keydown', function(e) { 
        if ((e.ctrlKey || e.metaKey) && e.key === 'p') { 
            e.preventDefault(); alert("Chức năng in đã bị khóa."); 
        } 
    }, true);
});

// --- EXPOSED FUNCTIONS ---

window.filterTasks = function(filterType) {
    window.location.href = `${taskConfig.baseUrl}?view=${taskConfig.viewMode}&filter=${filterType}&search=${taskConfig.searchTerm}`;
}

window.openCreateTaskModal = function() { $('#createTaskModal').modal('show'); };

window.openUpdateModal = function(taskId) { 
    if (taskManagerInstance) {
        taskManagerInstance.openUpdateModal(taskId); 
    } else {
        console.error("Task Manager chưa sẵn sàng.");
    }
};

window.openSupervisorNoteModal = function(logId, content) { 
    if (taskManagerInstance) taskManagerInstance.openSupervisorNoteModal(logId); 
};

window.togglePriority = function(taskId, btn) {
    fetch(`/api/task/toggle_priority/${taskId}`, { method: 'POST' })
        .then(r => r.json()).then(data => {
            if (data.success) {
                const icon = btn.querySelector('i');
                const newPriority = data.new_priority;
                const color = (newPriority === 'HIGH' || newPriority === 'ALERT') ? '#ffc107' : '#ccc';
                icon.style.color = color;
            } else {
                alert(data.message);
            }
        }).catch(e => console.error(e));
};

function debounce(func, timeout = 300) {
    let timer;
    return (...args) => { clearTimeout(timer); timer = setTimeout(() => { func.apply(this, args); }, timeout); };
}

let eligibleHelpers = []; 
function loadEligibleHelpers() {
    if (eligibleHelpers.length > 0) return;
    fetch('/api/get_eligible_helpers').then(r => r.json()).then(data => {
        eligibleHelpers = data;
        const ddl = $('#helper_code_select'); ddl.empty();
        ddl.append('<optgroup label="--- GỬI CẢ BỘ PHẬN ---"><option value="DEPT_2. KINH DOANH">TOÀN BỘ KINH DOANH</option><option value="DEPT_5. KHO">TOÀN BỘ KHO</option><option value="DEPT_3. THU KY">TOÀN BỘ THƯ KÝ</option></optgroup>');
        ddl.append('<optgroup label="--- NHÂN VIÊN ---">');
        data.forEach(h => { ddl.append(`<option value="${h.code}">${h.name}</option>`); });
        ddl.append('</optgroup>');
    }).catch(e => console.error(e));
}

function renderLogHistory(logs, isAdmin) {
    const container = $('#logHistoryContainer'); container.empty();
    if (!logs || logs.length === 0) { container.html('<p class="alert alert-info">Chưa có lịch sử.</p>'); return; }

    logs.forEach(log => {
        let logClass = 'bg-light border-secondary';
        let pct = (log.ProgressPercentage === null || isNaN(log.ProgressPercentage)) ? 0 : log.ProgressPercentage;
        let icon = `<i class="fas fa-chart-line text-primary"></i> ${pct}%`;
        let actionBtn = '';

        if(log.TaskLogType === 'BLOCKED') { logClass = 'bg-danger-subtle border-danger'; icon = '<i class="fas fa-lock text-danger"></i> BLOCKED'; }
        else if(log.TaskLogType === 'HELP_CALL') { logClass = 'bg-warning-subtle border-warning'; icon = `<i class="fas fa-headset text-warning"></i> HỖ TRỢ (${log.HelperRequestCode})`; }
        else if(log.TaskLogType === 'REQUEST_CLOSE') { logClass = 'bg-success-subtle border-success'; icon = '<i class="fas fa-check-circle text-success"></i> Y/C ĐÓNG'; }
        else if(log.TaskLogType === 'SUPERVISOR_NOTE') { logClass = 'bg-info-subtle border-info'; icon = '<i class="fas fa-user-tie text-info"></i> FEEDBACK'; }

        if (isAdmin && log.TaskLogType !== 'SUPERVISOR_NOTE') {
             const safeContent = (log.UpdateContent || "").replace(/'/g, "\\'");
             // --- FIX LỖI Ở ĐÂY: Thêm type="button" ---
             actionBtn = `<button type="button" class="btn btn-sm btn-link p-0 float-end text-info" onclick="openSupervisorNoteModal(${log.LogID}, '${safeContent}')"><i class="fas fa-comment-dots"></i></button>`;
        }

        let feedbackHtml = log.SupervisorFeedback ? `<div class="mt-2 p-2 small border-top border-info"><i class="fas fa-reply me-1 text-info"></i> <strong>Cấp trên:</strong> ${log.SupervisorFeedback}</div>` : '';
        
        container.append(`
            <div class="card mb-2 ${logClass}">
                <div class="card-body p-3">
                    ${actionBtn}
                    <div class="small fw-bold mb-1">${icon} <span class="text-muted ms-3">${new Date(log.UpdateDate).toLocaleString('vi-VN')}</span></div>
                    <p class="mb-0 ms-2 small">${log.UpdateContent || ''}</p>
                    ${feedbackHtml}
                </div>
            </div>
        `);
    });
}

function timKhachHangAutocomplete(term, resultsDropdown) {
     const cleanedTerm = String(term || '').trim();
     if (cleanedTerm.length < 2) { resultsDropdown.hide().empty(); return; }
     resultsDropdown.empty().show();
     fetch(`/sales/api/khachhang/${cleanedTerm}`).then(r => r.json()).then(data => {
         if (data && data.length > 0) {
             data.forEach(kh => { resultsDropdown.append(`<option value="${kh.ID}">${kh.FullName}</option>`); });
             resultsDropdown.attr('size', Math.min(data.length, 5));
         } else { resultsDropdown.hide(); }
     }).catch(e => resultsDropdown.hide());
}

window.selectClient = function(dropdownId, inputId, hiddenId) {
     const dropdown = $(`#${dropdownId}`);
     const selectedOption = dropdown.find('option:selected');
     if (selectedOption.length) {
         $(`#${hiddenId}`).val(selectedOption.val());
         $(`#${inputId}`).val(selectedOption.text());
         dropdown.hide();
     }
}

function checkRecentUpdates() {
    fetch(`/api/task/recent_updates?view=${taskConfig.viewMode}&minutes=15`)
        .then(r => r.json()).then(updatedTasks => {
            document.querySelectorAll('.new-update-marker').forEach(m => m.remove());
            if (updatedTasks && updatedTasks.length > 0) {
                updatedTasks.forEach(task => {
                    let card = document.querySelector(`.task-card[onclick*="${task.TaskID}"]`) || document.querySelector(`tr[data-task-id="${task.TaskID}"] td`);
                    if (card) {
                        const marker = document.createElement('div'); marker.className = 'new-update-marker';
                        if (card.tagName === 'TD') card.style.position = 'relative';
                        card.appendChild(marker);
                    }
                });
            }
        }).catch(console.error);
}
function startPolling() { checkRecentUpdates(); setInterval(checkRecentUpdates, 15 * 60 * 1000); }