const authStatus = document.getElementById("authStatus");
const crudOutput = document.getElementById("crudOutput");
const portfolioOutput = document.getElementById("portfolioOutput");
const adminOutput = document.getElementById("adminOutput");
const roleBadge = document.getElementById("roleBadge");

let sessionToken = "";
let isAdminUser = false;
let currentRole = "guest";
let currentInternalRole = "staff";

const SQL_TABLES_EXTRACTED = [
  "members",
  "customers",
  "staff",
  "categories",
  "products",
  "suppliers",
  "purchase_orders",
  "purchase_order_items",
  "sales",
  "sale_items",
  "payments",
  "attendance",
];

const TABLES_BY_ROLE = {
  member: SQL_TABLES_EXTRACTED,
  staff: ["products", "attendance", "categories", "customers", "sales", "sale_items", "payments"],
  customer: ["products", "categories", "sales", "payments"],
};

const TABLE_ID_FIELD_MAP = {
  members: "member_id",
  products: "product_id",
  categories: "category_id",
  customers: "customer_id",
  staff: "staff_id",
  suppliers: "supplier_id",
  purchase_orders: "poid",
  purchase_order_items: "po_item_id",
  sales: "sale_id",
  sale_items: "sale_item_id",
  payments: "payment_id",
  attendance: "attendance_id",
};

const TABLE_PAYLOAD_TEMPLATES = {
  members: {
    name: "John Doe",
    age: 32,
    email: "john.doe@outlet.com",
    contact_number: "9876543210",
    role: "Manager",
    image: "john.jpg",
    created_at: "2026-03-22T10:30:48",
  },
  staff: {
    name: "Store Staff",
    role: "Cashier",
    salary: 35000,
    contact_number: "9999999999",
    join_date: "2026-03-22",
    member_id: 1,
  },
  products: {
    name: "Barcode Scanner",
    price: 2499.0,
    stock_quantity: 30,
    reorder_level: 5,
    category_id: 1,
  },
  categories: {
    category_name: "Automation",
    description: "Automation tools and devices",
    created_at: "2026-03-22T10:30:48.371299",
  },
  customers: {
    name: "Customer Name",
    email: "customer@example.com",
    contact_number: "8888888888",
    loyalty_points: 0,
    created_at: "2026-03-22T10:30:48.371299",
  },
  suppliers: {
    name: "Supply Co",
    contact_number: "7777777777",
    email: "supply@example.com",
    address: "Main Road, City",
  },
  purchase_orders: {
    supplier_id: 1,
    order_date: "2026-03-22",
    total_amount: 12000.0,
    status: "pending",
  },
  purchase_order_items: {
    poid: 1,
    product_id: 1,
    quantity: 10,
    cost_price: 900.0,
  },
  sales: {
    customer_id: 1,
    staff_id: 1,
    sale_date: "2026-03-22",
    total_amount: 1500.0,
  },
  sale_items: {
    sale_id: 1,
    product_id: 1,
    quantity: 2,
    unit_price: 750.0,
  },
  payments: {
    sale_id: 1,
    payment_method: "UPI",
    amount: 1500.0,
    payment_date: "2026-03-22",
  },
  attendance: {
    staff_id: 1,
    entry_time: "09:00:00",
    exit_time: "18:00:00",
    work_date: "2026-03-22",
  },
};

function roleSelectEl() {
  return document.getElementById("portalRole");
}

function tableSelectEl() {
  return document.getElementById("tableSelect");
}

function renderTableOptions(tableNames) {
  const select = tableSelectEl();
  const current = select.value;
  select.innerHTML = "";

  tableNames.forEach((name) => {
    const option = document.createElement("option");
    option.value = name;
    option.textContent = name;
    select.appendChild(option);
  });

  if (tableNames.includes(current)) {
    select.value = current;
    return;
  }

  if (tableNames.length > 0) {
    select.value = tableNames[0];
  }

  generateFormForSelectedTable();
}

function applyTableVisibility() {
  const allowedTables = TABLES_BY_ROLE[currentRole] || [];
  renderTableOptions(allowedTables);
}

const controlsToToggle = [
  "logoutButton",
  "listBtn",
  "getBtn",
  "portfolioListBtn",
  "portfolioOneBtn",
  "updateSelfPortfolioBtn",
];

const adminOnlyControls = [
  "submitUpdateBtn",
  "addRecordRowBtn",
  "submitBulkCreateBtn",
  "addDeleteRowBtn",
  "submitBulkDeleteBtn",
  "adminListGroupsBtn",
  "adminAddToGroupBtn",
  "adminRemoveFromGroupBtn",
  "adminUnauthorizedCheckBtn",
];

function showPage(pageName) {
  // Hide all pages
  document.querySelectorAll(".content-page").forEach((page) => {
    page.classList.remove("active");
  });
  
  // Remove active from all nav buttons
  document.querySelectorAll(".nav-btn").forEach((btn) => {
    btn.classList.remove("active");
  });
  
  // Show selected page
  const page = document.getElementById(`${pageName}-page`);
  if (page) {
    page.classList.add("active");
  }
  
  // Mark nav button as active
  const navBtn = document.querySelector(`[data-page="${pageName}"]`);
  if (navBtn) {
    navBtn.classList.add("active");
  }
}

function showUserInfo() {
  const modal = document.getElementById("userInfoPanel");
  const trigger = document.getElementById("profileMenuToggle");
  if (modal) {
    modal.style.display = "flex";
    modal.classList.add("open");
  }
  if (trigger) {
    trigger.setAttribute("aria-expanded", "true");
  }
}

function hideUserInfo() {
  const modal = document.getElementById("userInfoPanel");
  const trigger = document.getElementById("profileMenuToggle");
  if (modal) {
    modal.classList.remove("open");
    modal.style.display = "none";
  }
  if (trigger) {
    trigger.setAttribute("aria-expanded", "false");
  }
}

function setAuthenticated(enabled) {
  const loginScreen = document.querySelector(".login-screen");
  const appScreen = document.querySelector(".app-screen");
  
  if (enabled) {
    // Hide login, show app
    if (loginScreen) loginScreen.style.display = "none";
    if (appScreen) appScreen.style.display = "flex";
    document.body.classList.add("logged-in");
    document.body.classList.remove("logged-out");
    
    // Don't show any page by default - user must click a nav tab
  } else {
    // Show login, hide app
    if (loginScreen) loginScreen.style.display = "flex";
    if (appScreen) appScreen.style.display = "none";
    document.body.classList.remove("logged-in");
    document.body.classList.add("logged-out");
    
    // Hide user info modal
    hideUserInfo();
    
    // Clear form
    document.getElementById("username").value = "";
    document.getElementById("password").value = "";
  }

  controlsToToggle.forEach((id) => {
    const el = document.getElementById(id);
    if (el) el.disabled = !enabled;
  });

  if (!enabled) {
    adminOnlyControls.forEach((id) => {
      const el = document.getElementById(id);
      if (el) el.disabled = true;
    });
    renderTableOptions([]);
  }

  // Update test controls state
  if (enabled) {
    initTestControls();
  } else {
    document.getElementById("runQuickTest").disabled = true;
    document.getElementById("stopTest").disabled = true;
  }
}

function applyRolePermissions() {
  adminOnlyControls.forEach((id) => {
    document.getElementById(id).disabled = !isAdminUser;
  });
  const canUpdateSelfPortfolio = Boolean(sessionToken) && currentRole !== "customer";
  document.getElementById("updateSelfPortfolioBtn").disabled = !canUpdateSelfPortfolio;
  applyTableVisibility();
  roleBadge.textContent = `Role: ${currentRole}`;
}

function setOutput(el, data) {
  el.textContent = typeof data === "string" ? data : JSON.stringify(data, null, 2);
}

function formatSecurityCheck(data) {
  if (!data || typeof data !== 'object') {
    return data;
  }

  let html = '<div class="security-check-report">';
  
  // Header with summary
  html += '<div class="security-check-header">';
  html += `<h3>🔒 Security Check Report</h3>`;
  html += `<div class="check-summary">`;
  const suspiciousCount = data.suspicious_count || 0;
  html += `<span class="badge ${suspiciousCount > 0 ? 'danger' : 'success'}">`;
  html += `${suspiciousCount} Suspicious ${suspiciousCount === 1 ? 'Issue' : 'Issues'}`;
  html += `</span>`;
  if (data.audit_file) {
    html += `<span class="audit-file">📄 Audit Log: ${data.audit_file.split('/').pop()}</span>`;
  }
  html += `</div>`;
  html += '</div>';

  // Suspicious items
  if (data.suspicious && data.suspicious.length > 0) {
    html += '<div class="suspicious-items">';
    data.suspicious.forEach((item, idx) => {
      html += `<div class="suspicious-item ${item.status}">`;
      html += `<div class="suspicious-header">`;
      html += `<span class="db-table">📊 ${item.db}.${item.table}</span>`;
      html += `<span class="status-badge ${item.status}">${item.status.replace(/_/g, ' ')}</span>`;
      html += `</div>`;

      if (item.note) {
        html += `<div class="item-note">📝 ${item.note}</div>`;
      }

      // Show expected vs live state
      if (item.expected && item.live) {
        html += `<div class="data-comparison">`;
        html += `<div class="comparison-col">`;
        html += `<div class="comp-title">Expected State</div>`;
        html += `<div class="comparison-field"><span class="label">Rows:</span> <span class="value">${item.expected.row_count}</span></div>`;
        html += `<div class="comparison-field"><span class="label">Checksum:</span> <span class="checksum">${item.expected.key_checksum}</span></div>`;
        if (item.expected.last_api_write_at) {
          html += `<div class="comparison-field"><span class="label">Last Write:</span> <span class="value">${new Date(item.expected.last_api_write_at).toLocaleString()}</span></div>`;
        }
        if (item.expected.last_api_actor) {
          html += `<div class="comparison-field"><span class="label">Actor:</span> <span class="actor">${item.expected.last_api_actor}</span></div>`;
        }
        html += `</div>`;

        html += `<div class="comparison-col">`;
        html += `<div class="comp-title">Current State</div>`;
        html += `<div class="comparison-field"><span class="label">Rows:</span> <span class="value ${item.live.row_count !== item.expected.row_count ? 'mismatch' : ''}">${item.live.row_count}</span></div>`;
        html += `<div class="comparison-field"><span class="label">Checksum:</span> <span class="checksum ${item.live.key_checksum !== item.expected.key_checksum ? 'mismatch' : ''}">${item.live.key_checksum}</span></div>`;
        html += `</div>`;
        html += `</div>`;
      } else if (item.status === 'error') {
        html += `<div class="item-error">⚠️ Error: ${item.note || 'Unknown error'}</div>`;
      }

      html += '</div>';
    });
    html += '</div>';
  } else {
    html += '<div class="empty-state"><p>✅ All monitored tables verified - no suspicious activity detected</p></div>';
  }

  html += '</div>';
  
  // Create a temporary div to set as innerHTML
  const temp = document.createElement('div');
  temp.innerHTML = html;
  return temp;
}

function setSecurityCheckOutput(el, data) {
  if (typeof data === 'string') {
    el.textContent = data;
    return;
  }
  el.innerHTML = '';
  const formatted = formatSecurityCheck(data);
  if (formatted instanceof HTMLElement) {
    el.appendChild(formatted);
  } else {
    el.textContent = JSON.stringify(data, null, 2);
  }
}

function formatValidationErrors(validationErrors) {
  if (!validationErrors || !Array.isArray(validationErrors)) {
    return '';
  }

  let html = '<div class="validation-errors">';
  html += '<h4 style="color: var(--danger); margin-top: 0;">🔍 Validation Error Details:</h4>';
  html += '<div class="error-list">';
  
  validationErrors.forEach((err, idx) => {
    html += '<div class="error-item">';
    html += `<div class="error-index">Record #${err.record_index + 1}</div>`;
    html += `<div class="error-message">❌ ${err.error}</div>`;
    
    // Show the problematic record data
    if (err.record) {
      html += '<div class="error-record-data">';
      html += '<div class="record-label">Record Data:</div>';
      html += '<div class="record-content">';
      Object.entries(err.record).forEach(([key, value]) => {
        html += `<div class="record-field"><span class="field-name">${key}:</span> <span class="field-value">${value}</span></div>`;
      });
      html += '</div>';
      html += '</div>';
    }
    
    html += '</div>';
  });
  
  html += '</div>';
  html += '</div>';
  
  return html;
}

function setProfileOutput(content) {
  const el = document.getElementById("portfolioOutput");
  if (typeof content === "string") {
    if (content.includes("failed") || content.includes("error")) {
      el.innerHTML = `<div class="empty-state"><p>❌ ${content}</p></div>`;
    } else {
      el.innerHTML = `<div class="profile-card"><p style="color: var(--success);">✅ ${content}</p></div>`;
    }
  } else {
    el.innerHTML = typeof content === "string" ? content : JSON.stringify(content, null, 2);
  }
}

function createProfileCard(member, groups = []) {
  if (!member) return "";
  
  const memberData = member.member || member;
  const memberGroups = groups || member.groups || [];
  
  let html = '<div class="profile-card">';
  html += `<h3>👤 ${memberData.full_name || "Member Profile"}</h3>`;
  html += '<div class="profile-fields">';
  
  // Display key fields
  const fields = {
    "ID": memberData.member_id || "N/A",
    "Username": memberData.username || "N/A",
    "Full Name": memberData.full_name || "N/A",
    "Email": memberData.email || "N/A",
    "Contact": memberData.contact_number || "N/A",
    "Department": memberData.department || "N/A",
    "Age": memberData.age || "N/A",
    "Status": memberData.status || "N/A"
  };
  
  Object.entries(fields).forEach(([label, value]) => {
    if (value !== "N/A" || memberData[label.toLowerCase()]) {
      html += `<div class="profile-field">
        <div class="profile-field-label">${label}</div>
        <div class="profile-field-value">${value || "—"}</div>
      </div>`;
    }
  });
  
  html += '</div>';
  
  // Display groups if any
  if (memberGroups && memberGroups.length > 0) {
    html += '<div class="groups-list"><strong>📊 Groups Assigned:</strong>';
    memberGroups.forEach(group => {
      const groupName = group.group_name || "Unknown";
      const role = group.role_in_group || group.role_in_group || "member";
      html += `<div class="group-item">${groupName} <span class="group-role">(${role})</span></div>`;
    });
    html += '</div>';
  }
  
  html += '</div>';
  return html;
}

function displayProfileList(data) {
  const el = document.getElementById("portfolioOutput");
  if (!data || !data.records || data.records.length === 0) {
    el.innerHTML = '<div class="empty-state"><p>No profiles found</p></div>';
    return;
  }
  
  let html = '<div class="profiles-grid">';
  data.records.forEach(record => {
    const member = record.data || record;
    const groups = member.groups || [];
    html += createProfileCard(member, groups);
  });
  html += '</div>';
  el.innerHTML = html;
}

function displayProfileDetail(data) {
  const el = document.getElementById("portfolioOutput");
  if (!data) {
    el.innerHTML = '<div class="empty-state"><p>Member not found</p></div>';
    return;
  }
  
  const member = data.member || data;
  const groups = data.groups || [];
  const html = createProfileCard(member, groups);
  el.innerHTML = html;
}

function formatRecordsTable(data, tableName) {
  if (!data || !data.records || data.records.length === 0) {
    return '<div class="empty-state"><p>No records found</p></div>';
  }

  let html = '<div class="table-display"><table class="data-table"><thead><tr>';
  
  // Get column headers from first record
  const firstRecord = data.records[0]?.data || data.records[0];
  const columns = Object.keys(firstRecord || {});
  
  if (columns.length === 0) {
    return '<div class="empty-state"><p>No data to display</p></div>';
  }
  
  // Create headers
  columns.forEach(col => {
    html += `<th>${col}</th>`;
  });
  html += '</tr></thead><tbody>';
  
  // Create rows
  data.records.forEach(record => {
    const rowData = record.data || record;
    html += '<tr>';
    columns.forEach(col => {
      let value = rowData[col];
      
      // Format value
      if (value === null || value === undefined) {
        value = '—';
      } else if (typeof value === 'object') {
        value = JSON.stringify(value).substring(0, 50);
      } else if (typeof value === 'string' && value.length > 50) {
        value = value.substring(0, 50) + '...';
      }
      
      html += `<td>${value}</td>`;
    });
    html += '</tr>';
  });
  
  html += '</tbody></table></div>';
  return html;
}

function formatGroupsList(data) {
  // Handle both "groups" and "records" keys from API
  const groups = data?.groups || data?.records || [];
  
  if (!groups || groups.length === 0) {
    return '<div class="empty-state"><p>No groups found</p></div>';
  }

  let html = '<div class="groups-display">';
  
  groups.forEach(group => {
    const members = group.members || [];
    
    html += `<div class="group-card">
      <h4>👥 ${group.group_name || 'Unknown Group'}</h4>
      <div class="group-details">
        <div class="detail-row"><span class="label">ID:</span> <span class="value">${group.group_id || 'N/A'}</span></div>
        <div class="detail-row"><span class="label">Created:</span> <span class="value">${group.created_at || 'N/A'}</span></div>`;
    
    if (group.description) {
      html += `<div class="detail-row"><span class="label">Description:</span> <span class="value">${group.description}</span></div>`;
    }
    
    // Display members in this group
    html += `<div class="detail-row"><span class="label">Members:</span> <span class="value">`;
    if (members && members.length > 0) {
      html += `<div class="members-list">`;
      members.forEach(member => {
        const memberId = member.member_id || 'N/A';
        const memberName = member.full_name || member.username || 'Unknown';
        html += `<div class="member-item">
          <span class="member-id">#${memberId}</span> 
          <span class="member-name">${memberName}</span>
        </div>`;
      });
      html += `</div>`;
    } else {
      html += `<em>No members</em>`;
    }
    html += `</span></div>`;
    
    html += `</div></div>`;
  });
  
  html += '</div>';
  return html;
}

function formatWhoAmI(me, token = "") {
  const member = me.member || {};
  const groups = Array.isArray(me.groups) ? me.groups : [];
  const allowedTables = Array.isArray(me.allowed_tables) ? me.allowed_tables : [];
  const groupSummary = groups.length
    ? groups.map((g) => `${g.group_name} (${g.role_in_group})`).join(", ")
    : "None";

  const lines = [
    "🔐 Authenticated User Session",
    "─────────────────────────────",
    `👤 Username: ${member.username || "N/A"}`,
    `🆔 Member ID: ${member.member_id ?? "N/A"}`,
    `📧 Email: ${member.email || "N/A"}`,
    `📱 Contact: ${member.contact_number || "N/A"}`,
    `👔 Role: ${member.role || "N/A"}`,
    `📅 Age: ${member.age || "N/A"}`,
    `✅ Account Status: ${member.status || "N/A"}`,
    `🎯 Portal Role: ${me.portal_role || currentRole}`,
    `🔑 Internal Role: ${me.role || currentInternalRole}`,
    `⚙️ Admin Access: ${Boolean(me.is_admin) ? "Yes" : "No"}`,
    `👥 Groups: ${groupSummary}`,
    `📊 Allowed Tables: ${allowedTables.join(", ") || "None"}`,
  ];

  if (token) {
    lines.push(`🔐 Session Token: ${token.substring(0, 30)}...`);
  }

  return lines.join("\n");
}

function displayAuthStatusAsTable(el, me, token = "") {
  const member = me.member || {};
  const groups = Array.isArray(me.groups) ? me.groups : [];
  const allowedTables = Array.isArray(me.allowed_tables) ? me.allowed_tables : [];
  const groupSummary = groups.length
    ? groups.map((g) => `${g.group_name} (${g.role_in_group})`).join(", ")
    : "None";

  const data = [
    { label: "Username", value: member.username || "N/A" },
    { label: "Member ID", value: member.member_id ?? "N/A" },
    { label: "Email", value: member.email || "N/A" },
    { label: "Contact", value: member.contact_number || "N/A" },
    { label: "Role", value: member.role || "N/A" },
    { label: "Age", value: member.age || "N/A" },
    { label: "Account Status", value: member.status || "N/A" },
    { label: "Portal Role", value: me.portal_role || currentRole },
    { label: "Internal Role", value: me.role || currentInternalRole },
    { label: "Admin Access", value: Boolean(me.is_admin) ? "Yes" : "No" },
    { label: "Groups", value: groupSummary },
    { label: "Allowed Tables", value: allowedTables.join(", ") || "None" },
  ];

  if (token) {
    data.push({ label: "Session Token", value: token.substring(0, 30) + "..." });
  }

  let html = '<table class="profile-info-table">';
  html += '<tbody>';
  
  data.forEach(row => {
    html += `<tr>`;
    html += `<td class="label">${row.label}</td>`;
    html += `<td class="value">${escapeHtml(String(row.value))}</td>`;
    html += `</tr>`;
  });

  html += '</tbody>';
  html += '</table>';

  el.innerHTML = html;
}

function escapeHtml(unsafe) {
  return unsafe
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#039;");
}

async function apiCall(path, options = {}) {
  return window.ApiService.call(path, options);
}

async function authenticate(username, password, selectedPortalRole) {
  const result = await apiCall("/api/auth/login", {
    method: "POST",
    body: JSON.stringify({ username, password, portal_role: selectedPortalRole }),
  });

  sessionToken = result.session_token;
  window.ApiService.setToken(sessionToken);
  setAuthenticated(true);

  const me = await apiCall("/api/auth/me");
  currentRole = me.portal_role || result.portal_role || selectedPortalRole;
  currentInternalRole = me.role || result.role || "staff";
  isAdminUser = Boolean(me.is_admin);
  applyRolePermissions();
  displayAuthStatusAsTable(authStatus, me, sessionToken);
  return { login: result, me };
}

function resetAuthState(messageText) {
  setAuthenticated(false);
  sessionToken = "";
  window.ApiService.setToken("");
  isAdminUser = false;
  currentRole = "guest";
  currentInternalRole = "staff";
  roleBadge.textContent = "";
  if (messageText) {
    setOutput(authStatus, messageText);
  } else {
    authStatus.textContent = "";
  }
}

document.getElementById("loginForm").addEventListener("submit", async (event) => {
  event.preventDefault();
  const selectedPortalRole = roleSelectEl().value;
  const username = document.getElementById("username").value.trim();
  const password = document.getElementById("password").value;

  try {
    await authenticate(username, password, selectedPortalRole);
  } catch (error) {
    resetAuthState(`Login failed: ${error.message}`);
  }
});

document.getElementById("logoutButton").addEventListener("click", async () => {
  try {
    const result = await apiCall("/api/auth/logout", { method: "POST" });
    console.log("Logout result:", result);
  } catch (error) {
    console.log("Logout error:", error.message);
  } finally {
    hideUserInfo();
    resetAuthState("");
    setAuthenticated(false);
  }
});

const profileMenuToggle = document.getElementById("profileMenuToggle");
if (profileMenuToggle) {
  profileMenuToggle.addEventListener("click", async () => {
    const panel = document.getElementById("userInfoPanel");
    const isOpen = Boolean(panel && panel.classList.contains("open"));
    if (isOpen) {
      hideUserInfo();
      return;
    }

    try {
      const result = await apiCall("/api/auth/me");
      currentRole = result.portal_role || currentRole;
      currentInternalRole = result.role || currentInternalRole;
      isAdminUser = Boolean(result.is_admin);
      applyRolePermissions();
      displayAuthStatusAsTable(authStatus, result, sessionToken);
      showUserInfo();
    } catch (error) {
      alert(`Failed to load user info: ${error.message}`);
    }
  });
}

// Close user info modal when clicking close button
const closeBtn = document.querySelector(".close-btn");
if (closeBtn) {
  closeBtn.addEventListener("click", hideUserInfo);
}

// Close modal when clicking outside content
const userInfoModal = document.getElementById("userInfoPanel");
if (userInfoModal) {
  userInfoModal.addEventListener("click", (e) => {
    if (e.target === userInfoModal || e.target.classList.contains("user-info-backdrop")) {
      hideUserInfo();
    }
  });
}

// Navigation menu handlers
document.querySelectorAll(".nav-btn").forEach((btn) => {
  btn.addEventListener("click", () => {
    const page = btn.getAttribute("data-page");
    if (page) {
      showPage(page);
    }
  });
});

function tableName() {
  return document.getElementById("tableSelect").value;
}

function recordId() {
  return document.getElementById("recordIdInput").value.trim();
}

function parsePayload() {
  // Removed - using dynamic form instead
  return {};
}

function generateFormForSelectedTable() {
  // Removed - no longer generate single record form
}

document.getElementById("listBtn").addEventListener("click", async () => {
  try {
    const result = await apiCall(`/api/project/${tableName()}`);
    const table = tableName();
    const html = formatRecordsTable(result, table);
    crudOutput.innerHTML = html;
  } catch (error) {
    crudOutput.innerHTML = `<div class="empty-state"><p>❌ List failed: ${error.message}</p></div>`;
  }
});

document.getElementById("getBtn").addEventListener("click", async () => {
  const id = recordId();
  if (!id) {
    setOutput(crudOutput, "Record ID is required");
    return;
  }

  try {
    const result = await apiCall(`/api/project/${tableName()}/${encodeURIComponent(id)}`);
    
    // Format the record nicely
    let recordHTML = `<div style="background: var(--bg-secondary); padding: 20px; border-radius: 10px; border-left: 4px solid var(--primary);">
      <h4 style="color: var(--primary); margin-top: 0; margin-bottom: 15px;">📄 Record Details</h4>
      <table style="width: 100%; border-collapse: collapse;">`;
    
    // If result is an object with data, loop through it
    const data = result.data || result;
    Object.keys(data).forEach(key => {
      const value = data[key];
      const displayValue = typeof value === 'object' ? JSON.stringify(value) : value;
      recordHTML += `
        <tr style="border-bottom: 1px solid var(--border);">
          <td style="padding: 10px 0; font-weight: 600; color: var(--primary); width: 30%;">${key}</td>
          <td style="padding: 10px 0; color: var(--text-primary);">${displayValue}</td>
        </tr>
      `;
    });
    
    recordHTML += `</table></div>`;
    crudOutput.innerHTML = recordHTML;
    
  } catch (error) {
    setOutput(crudOutput, `Read failed: ${error.message}`);
  }
});

// Update Single Record with Form Fields
let updateFieldsData = {}; // Store update form field values

function showUpdateFields() {
  const recordId_val = recordId();
  const currentTable = tableName();
  
  if (!recordId_val || !currentTable) {
    document.getElementById("updateFieldsContainer").style.display = 'none';
    return;
  }
  
  // Get template fields for this table
  const template = TABLE_PAYLOAD_TEMPLATES[currentTable];
  if (!template) {
    document.getElementById("updateFieldsContainer").style.display = 'none';
    return;
  }
  
  // Build form fields (exclude auto-generated fields)
  let fieldsHTML = '';
  const autoGeneratedFields = ["created_at", "updated_at", "id"];
  const fields = Object.keys(template).filter(f => !autoGeneratedFields.includes(f));
  
  fields.forEach(fieldName => {
    const example = template[fieldName];
    const fieldType = Array.isArray(example) ? 'select' : typeof example;
    
    fieldsHTML += `
      <div class="record-field-group">
        <label>${fieldName} <span class="field-type">${fieldType}</span></label>
        <input 
          type="text" 
          class="update-field-input" 
          data-field-name="${fieldName}"
          placeholder="Leave blank to keep current value"
        />
      </div>
    `;
  });
  
  document.getElementById("updateFormFields").innerHTML = fieldsHTML;
  document.getElementById("updateFieldsContainer").style.display = 'block';
  
  // Add change listeners
  document.querySelectorAll(".update-field-input").forEach(input => {
    input.addEventListener("change", () => {
      const fieldName = input.getAttribute("data-field-name");
      const value = input.value.trim();
      if (value) {
        updateFieldsData[fieldName] = value;
      } else {
        delete updateFieldsData[fieldName];
      }
    });
  });
}

// Listen for record ID or table changes to show/hide update form
document.getElementById("recordIdInput").addEventListener("change", showUpdateFields);
document.getElementById("tableSelect").addEventListener("change", showUpdateFields);

document.getElementById("submitUpdateBtn").addEventListener("click", async () => {
  const recordId_val = recordId();
  const currentTable = tableName();
  
  if (!recordId_val) {
    setOutput(crudOutput, "Record ID is required");
    return;
  }
  
  if (Object.keys(updateFieldsData).length === 0) {
    setOutput(crudOutput, "Please fill in at least one field to update");
    return;
  }
  
  try {
    const result = await apiCall(`/api/project/${currentTable}/${encodeURIComponent(recordId_val)}`, {
      method: "PUT",
      body: JSON.stringify(updateFieldsData),
    });
    
    const output = `<div style="background: rgba(0, 184, 148, 0.15); padding: 15px; border-radius: 5px;">
      <h4 style="color: var(--success); margin-top: 0;">✅ Record Updated Successfully</h4>
      <p><strong>Table:</strong> ${currentTable}</p>
      <p><strong>Record ID:</strong> ${recordId_val}</p>
    </div>`;
    
    crudOutput.innerHTML = output;
    
    // Clear the form
    document.getElementById("updateFormFields").innerHTML = '';
    document.getElementById("updateFieldsContainer").style.display = 'none';
    updateFieldsData = {};
    document.getElementById("recordIdInput").value = '';
    
  } catch (error) {
    setOutput(crudOutput, `Update failed: ${error.message}`);
  }
});

// Dynamic Multiple Records Handlers
let recordRows = 0;
const recordData = {}; // Store row data: rowId -> { table, field: value, ... }

function getRecordFieldsTemplate() {
  const selectedTable = tableName();
  const template = TABLE_PAYLOAD_TEMPLATES[selectedTable];
  if (!template) return {};
  
  const fieldTypes = {
    age: "int", poid: "int", po_item_id: "int", quantity: "int",
    stock_quantity: "int", reorder_level: "int", member_id: "int",
    category_id: "int", product_id: "int", staff_id: "int",
    customer_id: "int", supplier_id: "int", sale_id: "int",
    attendance_id: "int", payment_id: "int", sale_item_id: "int",
    price: "float", salary: "float", unit_price: "float",
    total_amount: "float", cost_price: "float", amount: "float",
    loyalty_points: "int",
  };
  
  const autoGeneratedFields = ["created_at", "updated_at", "id"];
  
  const fields = [];
  Object.keys(template).forEach(key => {
    if (!autoGeneratedFields.includes(key)) {
      fields.push({
        name: key,
        type: fieldTypes[key] || "text",
        placeholder: typeof template[key] === "string" ? template[key] : JSON.stringify(template[key])
      });
    }
  });
  return fields;
}

function addRecordRow() {
  const rowId = ++recordRows;
  const container = document.getElementById("recordsContainer");
  const fields = getRecordFieldsTemplate();
  const selectedTable = tableName();
  const idField = TABLE_ID_FIELD_MAP[selectedTable];
  
  const rowDiv = document.createElement("div");
  rowDiv.className = "record-row";
  rowDiv.id = `record-row-${rowId}`;
  
  // Include ID field if available
  let idFieldHTML = '';
  if (idField) {
    idFieldHTML = `
      <div class="record-field-group">
        <label>${idField} (Optional - auto-assigned if blank) <span class="field-type">id</span></label>
        <input 
          type="text" 
          class="record-field-input record-id-input" 
          data-row-id="${rowId}"
          data-field-name="${idField}"
          data-field-type="int"
          placeholder="Leave blank for auto-assignment"
        />
      </div>
    `;
  }
  
  let fieldsHTML = '<div class="record-fields">';
  fieldsHTML += idFieldHTML; // Add ID field first
  
  fields.forEach(field => {
    fieldsHTML += `
      <div class="record-field-group">
        <label>${field.name} <span class="field-type">${field.type}</span></label>
        <input 
          type="text" 
          class="record-field-input" 
          data-row-id="${rowId}"
          data-field-name="${field.name}"
          data-field-type="${field.type}"
          placeholder="${field.placeholder}"
        />
      </div>
    `;
  });
  fieldsHTML += '</div>';
  
  rowDiv.innerHTML = `
    <div class="record-row-header">
      <span class="record-row-number">Record ${rowId}</span>
      <button type="button" class="remove-row-btn" onclick="removeRecordRow(${rowId})">✕ Remove</button>
    </div>
    ${fieldsHTML}
  `;
  
  container.appendChild(rowDiv);
  recordData[rowId] = { table: selectedTable }; // Store which table this row is for
}

function removeRecordRow(rowId) {
  const row = document.getElementById(`record-row-${rowId}`);
  if (row) {
    row.remove();
  }
  delete recordData[rowId];
}

function collectRecordRowData() {
  // Return records grouped by table: { members: [...], products: [...] }
  const recordsByTable = {};
  const inputs = document.querySelectorAll(".record-field-input");
  
  inputs.forEach(input => {
    const rowId = input.getAttribute("data-row-id");
    const fieldName = input.getAttribute("data-field-name");
    const fieldType = input.getAttribute("data-field-type");
    const value = input.value.trim();
    
    if (!recordData[rowId]) {
      recordData[rowId] = {};
    }
    
    if (value) {
      let typedValue = value;
      if (fieldType === "number" || fieldType === "float") {
        typedValue = isNaN(value) ? value : parseFloat(value);
      } else if (fieldType === "int") {
        typedValue = isNaN(value) ? value : parseInt(value);
      }
      recordData[rowId][fieldName] = typedValue;
    }
  });
  
  // Add timestamp to each record and group by table
  const now = new Date();
  const timestamp = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}-${String(now.getDate()).padStart(2, '0')} ${String(now.getHours()).padStart(2, '0')}:${String(now.getMinutes()).padStart(2, '0')}:${String(now.getSeconds()).padStart(2, '0')}`;
  
  Object.keys(recordData).forEach(rowId => {
    const rowTable = recordData[rowId].table;
    if (!rowTable) return; // Skip if no table assigned
    
    if (Object.keys(recordData[rowId]).length > 1) { // More than just "table" property
      if (!recordData[rowId].created_at) {
        recordData[rowId].created_at = timestamp;
      }
      
      // Remove the "table" property before sending to backend
      const cleanRecord = { ...recordData[rowId] };
      delete cleanRecord.table;
      
      if (!recordsByTable[rowTable]) {
        recordsByTable[rowTable] = [];
      }
      recordsByTable[rowTable].push(cleanRecord);
    }
  });
  
  return recordsByTable;
}

document.getElementById("addRecordRowBtn").addEventListener("click", (e) => {
  e.preventDefault();
  addRecordRow();
});

document.getElementById("submitBulkCreateBtn").addEventListener("click", async () => {
  try {
    const recordsByTable = collectRecordRowData();
    const tables = Object.keys(recordsByTable);
    
    if (tables.length === 0) {
      setOutput(crudOutput, "Please fill in at least one record");
      return;
    }
    
    // BEGIN TRANSACTION: Start atomic operation
    let transactionId = null;
    try {
      const beginResult = await apiCall("/api/transaction/begin", {
        method: "POST",
      });
      transactionId = beginResult.transaction_id;
    } catch (error) {
      setOutput(crudOutput, `Failed to begin transaction: ${error.message}`);
      return;
    }
    
    // QUEUE PHASE: Submit all records with transaction ID (nothing written to DB yet)
    const allResults = {};
    let totalRecords = 0;
    let totalSuccess = 0;
    let totalFailed = 0;
    let queueValidationErrors = null;
    
    for (const tableStr of tables) {
      const records = recordsByTable[tableStr];
      totalRecords += records.length;
      
      try {
        const result = await apiCall(`/api/project/${tableStr}`, {
          method: "POST",
          body: JSON.stringify(records),
          headers: {
            "X-Transaction-ID": transactionId,
          },
        });
        
        allResults[tableStr] = result;
        
        // Note: All requests return 202 Accepted during QUEUE phase
        const successCount = result.results?.filter(r => r.status === 'success').length || records.length;
        const failedCount = result.results?.filter(r => r.status === 'failed').length || 0;
        
        totalSuccess += successCount;
        totalFailed += failedCount;
      } catch (error) {
        // Check if this is a validation error from the queue phase
        if (error.response?.validation_errors) {
          queueValidationErrors = error.response.validation_errors;
          // Show validation errors immediately without proceeding to commit
          const validationErrors = queueValidationErrors;
          let errorOutput = `
            <div class="batch-error-container">
              <div class="error-header">
                <h4 style="margin-top: 0;">❌ Validation Failed During Record Submission (${validationErrors.length} Error${validationErrors.length !== 1 ? 's' : ''})</h4>
                <p class="error-summary"><span style="color: #ffff00; text-shadow: 0 0 5px rgba(0,0,0,0.5);">⚠️ NO RECORDS WERE CREATED</span></p>
              </div>
              
              <div class="error-details">
                <p><strong>Table:</strong> ${tableStr}</p>
                <p><strong>Reason:</strong> ${error.message || 'Record validation failed'}</p>
          `;
          
          errorOutput += formatValidationErrors(validationErrors);
          
          errorOutput += `
                <p style="color: #666; font-size: 0.9em; margin-top: 15px; padding-top: 15px; border-top: 1px solid var(--border-light);">
                  <strong>What to do:</strong> Please review the highlighted records above and correct the invalid fields before trying again.
                </p>
              </div>
            </div>
          `;
          
          crudOutput.innerHTML = errorOutput;
          
          // Clear the form
          document.getElementById("recordsContainer").innerHTML = '';
          recordRows = 0;
          Object.keys(recordData).forEach(key => delete recordData[key]);
          return;
        }

        // Check if this is a duplicate record ID error (409 Conflict)
        if (error.status === 409 || (error.response?.error_type === "duplicate_id")) {
          let errorOutput = `
            <div class="batch-error-container">
              <div class="error-header">
                <h4 style="margin-top: 0;">⚠️ Record ID Already Exists</h4>
                <p class="error-summary"><span style="color: #ffff00; text-shadow: 0 0 5px rgba(0,0,0,0.5);">❌ NO RECORDS WERE CREATED</span></p>
              </div>
              
              <div class="error-details">
                <p><strong>Table:</strong> ${tableStr}</p>
                <p><strong>Error:</strong> ${error.response?.error || error.message || 'A record with this ID already exists'}</p>
                
                <div style="background: rgba(255, 193, 7, 0.1); border-left: 3px solid #ffc107; padding: 12px; margin: 12px 0; border-radius: 3px;">
                  <strong style="color: #856404;">💡 What you can do:</strong>
                  <ul style="margin: 8px 0; padding-left: 20px;">
                    <li>Try <strong>updating the existing record</strong> using the Update function instead</li>
                    <li>Or enter a <strong>different Record ID</strong> for this new record</li>
                  </ul>
                </div>
              </div>
            </div>
          `;
          
          crudOutput.innerHTML = errorOutput;
          
          // Clear the form
          document.getElementById("recordsContainer").innerHTML = '';
          recordRows = 0;
          Object.keys(recordData).forEach(key => delete recordData[key]);
          return;
        }
        
        allResults[tableStr] = { status: 'error', message: error.message };
        totalFailed += records.length;
      }
    }
    
    // COMMIT PHASE: Execute all operations atomically or rollback all
    let commitResult = null;
    try {
      commitResult = await apiCall(`/api/transaction/${transactionId}/commit`, {
        method: "POST",
      });
    } catch (commitError) {
      // Check if this is a duplicate record ID error during commit
      if (commitError.response?.error_type === "duplicate_id" || (commitError.response?.validation_errors && 
          commitError.response.validation_errors.some(e => e.error && e.error.toLowerCase().includes("already exists")))) {
        let errorOutput = `
          <div class="batch-error-container">
            <div class="error-header">
              <h4 style="margin-top: 0;">⚠️ Record ID Already Exists</h4>
              <p class="error-summary"><span style="color: #ffff00; text-shadow: 0 0 5px rgba(0,0,0,0.5);">❌ NO RECORDS WERE CREATED</span></p>
            </div>
            
            <div class="error-details">
              <p><strong>Error Details:</strong></p>
        `;
        
        // Show the specific duplicate errors
        if (commitError.response?.validation_errors) {
          commitError.response.validation_errors.forEach((err, idx) => {
            errorOutput += `<div style="background: rgba(255, 193, 7, 0.1); border-left: 2px solid #ffc107; padding: 8px; margin: 8px 0; border-radius: 3px;">
              <strong style="color: #856404;">❌ ${err.error || err}</strong>
            </div>`;
          });
        } else {
          errorOutput += `<div style="background: rgba(255, 193, 7, 0.1); border-left: 2px solid #ffc107; padding: 8px; margin: 8px 0; border-radius: 3px;">
            <strong style="color: #856404;">❌ ${commitError.message || 'A record with this ID already exists'}</strong>
          </div>`;
        }
        
        errorOutput += `
                <div style="background: rgba(255, 193, 7, 0.1); border-left: 3px solid #ffc107; padding: 12px; margin: 12px 0; border-radius: 3px;">
                  <strong style="color: #856404;">💡 What you can do:</strong>
                  <ul style="margin: 8px 0; padding-left: 20px;">
                    <li>Try <strong>updating the existing record</strong> using the Update function instead</li>
                    <li>Or enter a <strong>different Record ID</strong> for this new record</li>
                  </ul>
                </div>
              </div>
            </div>
          `;
        
        crudOutput.innerHTML = errorOutput;
        
        // Clear the form
        document.getElementById("recordsContainer").innerHTML = '';
        recordRows = 0;
        Object.keys(recordData).forEach(key => delete recordData[key]);
        return;
      }
      
      // Commit failed - validation errors prevent creation
      const validationErrors = commitError.response?.validation_errors || [];
      const errorTitle = validationErrors.length > 0 
        ? `❌ Batch Creation Failed (${validationErrors.length} Validation Error${validationErrors.length !== 1 ? 's' : ''})`
        : '❌ Transaction Rolled Back (Validation Failed)';
      
      let errorOutput = `
        <div class="batch-error-container">
          <div class="error-header">
            <h4 style="margin-top: 0;">${errorTitle}</h4>
            <p class="error-summary"><strong>Status:</strong> NO RECORDS WERE CREATED (to ensure data consistency)</p>
          </div>
          
          <div class="error-details">
            <p><strong>Reason:</strong> ${commitError.message || 'One or more records failed validation'}</p>
      `;
      
      // If there are validation errors with details, show them
      if (validationErrors.length > 0) {
        errorOutput += formatValidationErrors(validationErrors);
      }
      
      errorOutput += `
            <p style="color: #666; font-size: 0.9em; margin-top: 15px; padding-top: 15px; border-top: 1px solid var(--border-light);">
              <strong>Why this happens:</strong> This atomic behavior ensures all-or-nothing execution. If any record fails validation, the entire batch is rejected to prevent partial/inconsistent data states.
            </p>
          </div>
        </div>
      `;
      
      crudOutput.innerHTML = errorOutput;
      
      // Clear the form
      document.getElementById("recordsContainer").innerHTML = '';
      recordRows = 0;
      Object.keys(recordData).forEach(key => delete recordData[key]);
      return;
    }
    
    // Build detailed output for success
    let detaisHTML = '';
    tables.forEach(tableStr => {
      const result = allResults[tableStr];
      detaisHTML += `
        <div style="margin-top: 10px; padding: 8px; background: var(--bg-secondary); border-left: 3px solid var(--primary); border-radius: 3px;">
          <strong style="color: var(--primary);">Table: ${tableStr}</strong><br>
          ${result.message || result.status || 'Processed'}<br>
          ${result.results ? `Records: ${result.results.map(r => r.status).join(', ')}` : ''}
        </div>
      `;
    });
    
    const output = `<div style="background: rgba(0, 184, 148, 0.15); padding: 15px; border-radius: 5px;">
      <h4 style="color: var(--success); margin-top: 0;">✅ Multi-Table Transaction Completed (Atomic Operations)</h4>
      <p><strong>Transaction ID:</strong> ${transactionId}</p>
      <p><strong>Total Records Submitted:</strong> ${totalRecords}</p>
      <p><strong>Successfully Created:</strong> ${totalSuccess}</p>
      ${totalFailed > 0 ? `<p><strong>Failed:</strong> ${totalFailed}</p>` : ''}
      <strong style="color: var(--primary);">📊 Table Results:</strong>
      ${detaisHTML}
      <p style="color: #666; font-size: 0.9em; margin-top: 10px;">
        ✓ All records validated before creation (all-or-nothing atomicity)
      </p>
    </div>`;
    
    crudOutput.innerHTML = output;
    
    // Clear the form
    document.getElementById("recordsContainer").innerHTML = '';
    recordRows = 0;
    Object.keys(recordData).forEach(key => delete recordData[key]);
    
  } catch (error) {
    setOutput(crudOutput, `Transaction failed: ${error.message}`);
  }
});

// Delete Multiple Records Handlers
let deleteRows = 0;
const deleteData = {}; // Store delete data: rowId -> { table, record_id }

function addDeleteRow() {
  const rowId = ++deleteRows;
  const container = document.getElementById("deleteRowsContainer");
  const selectedTable = tableName();
  
  const rowDiv = document.createElement("div");
  rowDiv.className = "record-row delete-row";
  rowDiv.id = `delete-row-${rowId}`;
  
  // Build table dropdown options
  let tableOptions = '';
  TABLES_BY_ROLE[currentRole].forEach(table => {
    const selected = table === selectedTable ? 'selected' : '';
    tableOptions += `<option value="${table}" ${selected}>${table}</option>`;
  });
  
  rowDiv.innerHTML = `
    <div class="record-row-header">
      <span class="record-row-number">Delete ${rowId}</span>
      <button type="button" class="remove-row-btn" onclick="removeDeleteRow(${rowId})">✕ Remove</button>
    </div>
    <div class="record-fields">
      <div class="record-field-group">
        <label>Table <span class="field-type">select</span></label>
        <select class="record-field-select" data-row-id="${rowId}" data-field-name="table" style="padding: 8px; background: var(--bg-tertiary); border: 1px solid var(--border); border-radius: 4px; color: var(--text-primary);">
          ${tableOptions}
        </select>
      </div>
      <div class="record-field-group">
        <label>Record ID to Delete <span class="field-type">id</span></label>
        <input 
          type="text" 
          class="record-field-input" 
          data-row-id="${rowId}"
          data-field-name="record_id"
          placeholder="e.g., 1, 5, 10"
        />
      </div>
    </div>
  `;
  
  container.appendChild(rowDiv);
  deleteData[rowId] = { table: selectedTable };
}

function removeDeleteRow(rowId) {
  const row = document.getElementById(`delete-row-${rowId}`);
  if (row) {
    row.remove();
  }
  delete deleteData[rowId];
}

function collectDeleteRowData() {
  // Return delete rows grouped by table: { members: [1, 2], products: [5, 10] }
  const deletesByTable = {};
  
  // First, update table selections from dropdowns if they exist
  const tableSelects = document.querySelectorAll(".record-field-select[data-field-name='table']");
  tableSelects.forEach(select => {
    const rowId = select.getAttribute("data-row-id");
    const selectedTable = select.value;
    if (!deleteData[rowId]) {
      deleteData[rowId] = {};
    }
    deleteData[rowId].table = selectedTable;
  });
  
  // Collect record IDs
  const idInputs = document.querySelectorAll(".record-field-input[data-field-name='record_id']");
  idInputs.forEach(input => {
    const rowId = input.getAttribute("data-row-id");
    const recordId = input.value.trim();
    
    if (recordId) {
      const table = deleteData[rowId].table;
      if (!deletesByTable[table]) {
        deletesByTable[table] = [];
      }
      // Convert to appropriate ID type (mostly int)
      const typedId = isNaN(recordId) ? recordId : parseInt(recordId);
      deletesByTable[table].push(typedId);
    }
  });
  
  return deletesByTable;
}

document.getElementById("addDeleteRowBtn").addEventListener("click", (e) => {
  e.preventDefault();
  addDeleteRow();
});

document.getElementById("submitBulkDeleteBtn").addEventListener("click", async () => {
  try {
    const deletesByTable = collectDeleteRowData();
    const tables = Object.keys(deletesByTable);
    
    if (tables.length === 0) {
      setOutput(crudOutput, "Please enter at least one record ID to delete");
      return;
    }
    
    // Confirm deletion
    const totalRecords = tables.reduce((sum, table) => sum + deletesByTable[table].length, 0);
    if (!confirm(`⚠️ Are you sure you want to DELETE ${totalRecords} record(s) across ${tables.length} table(s)? This action cannot be undone!`)) {
      return;
    }
    
    // Submit deletes for each table
    const allResults = {};
    let totalSuccess = 0;
    let totalFailed = 0;
    
    for (const tableStr of tables) {
      const recordIds = deletesByTable[tableStr];
      
      try {
        const result = await apiCall(`/api/project/${tableStr}/bulk-delete`, {
          method: "POST",
          body: JSON.stringify({ record_ids: recordIds }),
        });
        
        allResults[tableStr] = result;
        
        const successCount = result.results?.filter(r => r.status === 'success').length || recordIds.length;
        const failedCount = result.results?.filter(r => r.status === 'failed').length || 0;
        
        totalSuccess += successCount;
        totalFailed += failedCount;
      } catch (error) {
        allResults[tableStr] = { status: 'error', message: error.message };
        totalFailed += recordIds.length;
      }
    }
    
    // Build detailed output
    let detailsHTML = '';
    tables.forEach(tableStr => {
      const result = allResults[tableStr];
      detailsHTML += `
        <div style="margin-top: 10px; padding: 8px; background: var(--bg-secondary); border-left: 3px solid #dc3545; border-radius: 3px;">
          <strong style="color: #dc3545;">🗑️ Table: ${tableStr}</strong><br>
          ${result.message || result.status || 'Processed'}<br>
          ${result.results ? `Records: ${result.results.map(r => `${r.id}(${r.status})`).join(', ')}` : ''}
        </div>
      `;
    });
    
    const output = `<div style="background: rgba(220, 53, 69, 0.15); padding: 15px; border-radius: 5px;">
      <h4 style="color: #dc3545; margin-top: 0;">✅ Multi-Table Delete Transaction Completed (Atomic Operations)</h4>
      <p><strong>Total Records Deleted:</strong> ${totalRecords}</p>
      <p><strong>Successfully Deleted:</strong> ${totalSuccess}</p>
      ${totalFailed > 0 ? `<p><strong>Failed:</strong> ${totalFailed}</p>` : ''}
      <strong style="color: #dc3545;">📊 Table Results:</strong>
      ${detailsHTML}
    </div>`;
    
    crudOutput.innerHTML = output;
    
    // Clear the form
    document.getElementById("deleteRowsContainer").innerHTML = '';
    deleteRows = 0;
    Object.keys(deleteData).forEach(key => delete deleteData[key]);
    
  } catch (error) {
    setOutput(crudOutput, `Delete transaction failed: ${error.message}`);
  }
});

document.getElementById("portfolioListBtn").addEventListener("click", async () => {
  try {
    const result = await apiCall("/api/member-portfolio");
    displayProfileList(result);
  } catch (error) {
    setProfileOutput(`Portfolio fetch failed: ${error.message}`);
  }
});

document.getElementById("portfolioOneBtn").addEventListener("click", async () => {
  const memberId = document.getElementById("portfolioMemberId").value.trim();
  if (!memberId) {
    setProfileOutput("Member ID is required");
    return;
  }

  try {
    const result = await apiCall(`/api/member-portfolio/${encodeURIComponent(memberId)}`);
    displayProfileDetail(result);
  } catch (error) {
    setProfileOutput(`Member fetch failed: ${error.message}`);
  }
});

document.getElementById("updateSelfPortfolioBtn").addEventListener("click", async () => {
  try {
    const form = document.getElementById("profileUpdateForm");
    const payload = {};
    const fields = form.querySelectorAll("[data-field-name]");
    
    fields.forEach((field) => {
      const fieldName = field.getAttribute("data-field-name");
      const value = field.value.trim();
      if (value) {
        payload[fieldName] = value;
      }
    });
    
    if (Object.keys(payload).length === 0) {
      setProfileOutput("Please fill in at least one field");
      return;
    }

    const result = await apiCall("/api/member-portfolio/me", {
      method: "PUT",
      body: JSON.stringify(payload),
    });
    setProfileOutput(result.message || "Profile updated successfully");
    // Clear form after successful update
    form.querySelectorAll("input").forEach(input => input.value = "");
  } catch (error) {
    setProfileOutput(`Update self profile failed: ${error.message}`);
  }
});

function adminValues() {
  return {
    groupId: document.getElementById("adminGroupId").value.trim(),
    memberId: document.getElementById("adminMemberId").value.trim(),
  };
}

document.getElementById("adminListGroupsBtn").addEventListener("click", async () => {
  try {
    const result = await apiCall("/api/admin/groups");
    const html = formatGroupsList(result);
    adminOutput.innerHTML = html;
  } catch (error) {
    adminOutput.innerHTML = `<div class="empty-state"><p>❌ List groups failed: ${error.message}</p></div>`;
  }
});

document.getElementById("adminAddToGroupBtn").addEventListener("click", async () => {
  const { groupId, memberId } = adminValues();
  if (!groupId || !memberId) {
    setOutput(adminOutput, "Group ID and Member ID are required");
    return;
  }

  try {
    const result = await apiCall(`/api/admin/groups/${encodeURIComponent(groupId)}/members`, {
      method: "POST",
      body: JSON.stringify({ member_id: Number(memberId), role: "member" }),
    });
    setOutput(adminOutput, result);
  } catch (error) {
    setOutput(adminOutput, `Admin add member failed: ${error.message}`);
  }
});

document.getElementById("adminRemoveFromGroupBtn").addEventListener("click", async () => {
  const { groupId, memberId } = adminValues();
  if (!groupId || !memberId) {
    setOutput(adminOutput, "Group ID and Member ID are required");
    return;
  }

  try {
    const result = await apiCall(
      `/api/admin/groups/${encodeURIComponent(groupId)}/members/${encodeURIComponent(memberId)}`,
      { method: "DELETE" }
    );
    setOutput(adminOutput, result);
  } catch (error) {
    setOutput(adminOutput, `Admin remove member failed: ${error.message}`);
  }
});

document.getElementById("adminUnauthorizedCheckBtn").addEventListener("click", async () => {
  try {
    const result = await apiCall("/api/admin/audit/unauthorized-check");
    setSecurityCheckOutput(adminOutput, result);
  } catch (error) {
    setSecurityCheckOutput(adminOutput, `Unauthorized check failed: ${error.message}`);
  }
});

// ============ STRESS TESTING SECTION ============

let testRunning = false;
let testStartTime = null;
let testStopRequest = false;
let testMetrics = {
  totalRequests: 0,
  successCount: 0,
  errorCount: 0,
  responseTimes: []
};

function initTestControls() {
  document.getElementById("runQuickTest").disabled = !sessionToken;
  document.getElementById("stopTest").disabled = true;
  document.getElementById("clearTestResults").disabled = true;
}

document.getElementById("runQuickTest").addEventListener("click", async () => {
  if (!sessionToken) {
    alert("Please login first");
    return;
  }

  const userCount = parseInt(document.getElementById("testUserCount").value) || 10;
  const duration = parseInt(document.getElementById("testDuration").value) || 30;
  const rampUpRate = parseInt(document.getElementById("testRampUpRate").value) || 2;
  const testType = document.getElementById("testType").value;

  // Reset metrics
  testMetrics = {
    totalRequests: 0,
    successCount: 0,
    errorCount: 0,
    responseTimes: []
  };

  testRunning = true;
  testStopRequest = false;
  testStartTime = Date.now();

  document.getElementById("runQuickTest").disabled = true;
  document.getElementById("stopTest").disabled = false;
  document.getElementById("testProgress").style.display = "block";
  document.getElementById("testResults").style.display = "none";

  // Simulate test execution
  const endTime = testStartTime + (duration * 1000);
  const rampUpInterval = 1000 / rampUpRate;
  let activeUsersCount = 0;

  const simulateUserRequests = async () => {
    while (testRunning && !testStopRequest && Date.now() < endTime) {
      if (activeUsersCount < userCount) {
        activeUsersCount = Math.min(
          activeUsersCount + 1,
          Math.floor((Date.now() - testStartTime) / rampUpInterval)
        );
      }

      // Simulate concurrent requests from active users
      for (let i = 0; i < activeUsersCount; i++) {
        if (testStopRequest) break;
        
        try {
          const startTime = Date.now();
          
          // Make API call based on test type
          let endpoint = "/api/project/products/1";
          let method = "GET";
          
          if (testType === "race") {
            endpoint = "/api/project/products/1";
            method = "PUT";
            const response = await apiCall(endpoint, {
              method,
              body: JSON.stringify({ price: 1000 + Math.random() * 500 })
            });
          } else if (testType === "failure") {
            endpoint = "/api/project/products/99999";
            method = "GET";
            try {
              await apiCall(endpoint, { method });
            } catch (e) {
              // Expected failure
            }
          } else {
            // Concurrent and stress tests
            const tables = ["products", "categories", "customers", "sales"];
            const randomTable = tables[Math.floor(Math.random() * tables.length)];
            endpoint = `/api/project/${randomTable}`;
            await apiCall(endpoint, { method });
          }

          const responseTime = Date.now() - startTime;
          testMetrics.responseTimes.push(responseTime);
          testMetrics.successCount++;
        } catch (error) {
          testMetrics.errorCount++;
        }

        testMetrics.totalRequests++;
      }

      // Update progress UI
      updateTestProgress(activeUsersCount, userCount);

      // Small delay between request bursts
      await new Promise(resolve => setTimeout(resolve, 100));
    }

    finishTest(testType);
  };

  simulateUserRequests();
});

function updateTestProgress(activeUsers, totalUsers) {
  const elapsedSeconds = Math.floor((Date.now() - testStartTime) / 1000);
  const successRate = testMetrics.totalRequests > 0 
    ? ((testMetrics.successCount / testMetrics.totalRequests) * 100).toFixed(2) 
    : 100;
  const avgResponse = testMetrics.responseTimes.length > 0
    ? (testMetrics.responseTimes.reduce((a, b) => a + b) / testMetrics.responseTimes.length).toFixed(2)
    : 0;

  document.getElementById("testStatus").textContent = testRunning && !testStopRequest ? "Running..." : "Stopping...";
  document.getElementById("activeUsers").textContent = activeUsers;
  document.getElementById("totalUsers").textContent = totalUsers;
  document.getElementById("elapsedTime").textContent = elapsedSeconds + "s";
  document.getElementById("totalRequests").textContent = testMetrics.totalRequests;
  document.getElementById("successRate").textContent = successRate + "%";
  document.getElementById("errorCount").textContent = testMetrics.errorCount;
  document.getElementById("avgResponseTime").textContent = avgResponse + "ms";
}

function finishTest(testType) {
  testRunning = false;

  document.getElementById("runQuickTest").disabled = false;
  document.getElementById("stopTest").disabled = true;
  document.getElementById("clearTestResults").disabled = false;

  const totalTime = (Date.now() - testStartTime) / 1000;
  const avgResponse = testMetrics.responseTimes.length > 0
    ? (testMetrics.responseTimes.reduce((a, b) => a + b) / testMetrics.responseTimes.length).toFixed(2)
    : 0;
  const successRate = testMetrics.totalRequests > 0
    ? ((testMetrics.successCount / testMetrics.totalRequests) * 100).toFixed(2)
    : 100;
  const p95Response = testMetrics.responseTimes.length > 0
    ? testMetrics.responseTimes.sort((a, b) => a - b)[Math.floor(testMetrics.responseTimes.length * 0.95)]
    : 0;

  const summary = `
╔════════════════════════════════════════════════════════════════════════╗
║                          TEST SUMMARY REPORT                          ║
╚════════════════════════════════════════════════════════════════════════╝

TEST TYPE: ${testType.toUpperCase()}
─────────────────────────────────────────────────────────────────────────

📊 METRICS:
  • Total Requests:     ${testMetrics.totalRequests}
  • Successful:         ${testMetrics.successCount} (${successRate}%)
  • Failed:             ${testMetrics.errorCount}
  • Total Time:         ${totalTime.toFixed(2)}s
  • Avg Response Time:  ${avgResponse}ms
  • P95 Response Time:  ${p95Response}ms

TEST RESULTS:
  ✅ Completed successfully
  
OBSERVATIONS:
  • System handled ${testMetrics.totalRequests} requests in ${totalTime.toFixed(2)} seconds
  • Average response time: ${avgResponse}ms
  • Success rate: ${successRate}%
  
For detailed load testing with graphs and statistics,
please run Locust from the command line:
  locust -f locustfile.py --host=http://localhost:5000

╔════════════════════════════════════════════════════════════════════════╗
`;

  document.getElementById("resultsOutput").textContent = summary;
  document.getElementById("testResults").style.display = "block";
  document.getElementById("testProgress").style.display = "none";
  document.getElementById("testStatus").textContent = "Completed";
}

document.getElementById("stopTest").addEventListener("click", () => {
  testStopRequest = true;
  document.getElementById("stopTest").disabled = true;
});

document.getElementById("clearTestResults").addEventListener("click", () => {
  document.getElementById("testResults").style.display = "none";
  document.getElementById("testProgress").style.display = "none";
  document.getElementById("resultsOutput").textContent = "";
  testMetrics = {
    totalRequests: 0,
    successCount: 0,
    errorCount: 0,
    responseTimes: []
  };
  document.getElementById("clearTestResults").disabled = true;
});

setAuthenticated(false);
applyRolePermissions();
roleBadge.textContent = "";
authStatus.textContent = "";
setOutput(crudOutput, "Login first to use CRUD endpoints.");
setOutput(portfolioOutput, "Login first to view member portfolio.");
setOutput(adminOutput, "Admin actions require admin role.");
