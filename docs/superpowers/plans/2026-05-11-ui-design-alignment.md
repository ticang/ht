# UI Design Alignment Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将 `ui/` 下现有静态 HTML 原型对齐 `docs/2026-05-11-contract-agent-design.md` 的前端 UI 要求，并让核心业务流程可浏览验证。

**Architecture:** 当前仓库只有静态 HTML 原型，没有 Next.js 工程脚手架。本计划采用最小影响路径：先在现有 `ui/*.html` 内补齐导航、页面入口和交互状态，不引入 React/Next 架构迁移；若后续确认要工程化，再单独制定迁移计划。

**Tech Stack:** HTML、CSS、少量原生 JavaScript、浏览器静态文件验证。

---

## Assumptions

- 本轮执行范围仅限 `ui/` 静态原型和计划文档，不创建 Next.js 项目。
- 设计文档第 9 章是 UI 对齐的主要验收依据。
- 所有改动应保持静态可打开，不依赖后端服务。
- 不实现真实上传、导出 PDF、WebSocket 通知或 API 调用，只补齐可理解、可跳转、可演示的原型状态。

## File Structure

- Modify: `ui/dashboard.html`，负责 Agent 对话首页、文件入口、合规结果摘要、底部服务状态。
- Modify: `ui/contracts.html`，负责合同列表、筛选、到期提醒入口、详情/对比入口。
- Modify: `ui/contract-detail.html`，负责合同详情、版本历史、条款详情、合规报告入口。
- Modify: `ui/contract-diff.html`，负责左右分栏合同差异、增删改颜色标记。
- Modify: `ui/compliance.html`，负责单份校验、批量校验、最近报告入口。
- Modify: `ui/compliance-report.html`，负责风险总览、风险项、法规引用、处理状态、导出入口。
- Modify: `ui/regulations.html`，负责法规类型筛选、语义/关键词搜索、同步/上传入口。
- Modify: `ui/settings.html`，负责模型配置、用户管理、API Key、审计日志标签页。
- Read-only reference: `docs/2026-05-11-contract-agent-design.md`，作为验收依据。

---

### Task 1: Build UI Coverage Matrix

**Files:**
- Read: `docs/2026-05-11-contract-agent-design.md`
- Read: `ui/dashboard.html`
- Read: `ui/contracts.html`
- Read: `ui/contract-detail.html`
- Read: `ui/contract-diff.html`
- Read: `ui/compliance.html`
- Read: `ui/compliance-report.html`
- Read: `ui/regulations.html`
- Read: `ui/settings.html`
- Modify: `tasks/todo.md`

- [ ] **Step 1: Confirm design requirements**

Run:

```powershell
rg -n "9\\.2|9\\.3|Agent 对话|合同管理|合同对比|合规校验|法规知识库|系统设置|通知" docs\2026-05-11-contract-agent-design.md
```

Expected: Output includes 第 9.2 核心页面 and 第 9.3 通知系统.

- [ ] **Step 2: Confirm existing UI files**

Run:

```powershell
rg --files ui
```

Expected: Output includes:

```text
ui\dashboard.html
ui\contracts.html
ui\contract-detail.html
ui\contract-diff.html
ui\compliance.html
ui\compliance-report.html
ui\regulations.html
ui\settings.html
ui\login.html
```

- [ ] **Step 3: Update task tracker with coverage result**

In `tasks/todo.md`, append this exact coverage block under `## Review` after implementation:

```markdown

### UI 覆盖矩阵

| 设计文档页面 | 原型文件 | 状态 |
|---|---|---|
| Agent 对话（首页） | `ui/dashboard.html` | 已覆盖 |
| 合同管理 | `ui/contracts.html`、`ui/contract-detail.html` | 已覆盖 |
| 合同对比 | `ui/contract-diff.html` | 已覆盖 |
| 合规校验 | `ui/compliance.html`、`ui/compliance-report.html` | 已覆盖 |
| 法规知识库 | `ui/regulations.html` | 已覆盖 |
| 系统设置 | `ui/settings.html` | 已覆盖 |
| 通知系统 | 多页面顶部铃铛 | 入口已覆盖，实时推送未实现 |
```

- [ ] **Step 4: Commit**

Run:

```powershell
git add tasks\todo.md docs\superpowers\plans\2026-05-11-ui-design-alignment.md
git commit -m "docs: plan ui design alignment"
```

Expected: Commit succeeds after user confirms committing is desired.

---

### Task 2: Normalize Navigation And Brand Text

**Files:**
- Modify: `ui/dashboard.html`
- Modify: `ui/contracts.html`
- Modify: `ui/contract-detail.html`
- Modify: `ui/contract-diff.html`
- Modify: `ui/compliance.html`
- Modify: `ui/compliance-report.html`
- Modify: `ui/regulations.html`
- Modify: `ui/settings.html`

- [ ] **Step 1: Find broken core links and brand variants**

Run:

```powershell
rg -n 'href="#"|合同管理系统|合同管理Agent' ui
```

Expected: Output shows current placeholder links and mixed brand names.

- [ ] **Step 2: Update core navigation links**

Apply these rules only to sidebar, breadcrumb, and primary flow links:

```text
工作台 -> dashboard.html
合同管理 -> contracts.html
合规校验 -> compliance.html
法规知识库 -> regulations.html
系统设置 -> settings.html
合同详情 -> contract-detail.html
合同对比 -> contract-diff.html
合规报告 -> compliance-report.html
```

Do not change decorative `href="#"` links such as forgot-password unless they are part of a core flow.

- [ ] **Step 3: Normalize visible brand text**

Use this visible brand text consistently in sidebar logos and page titles:

```text
合同管理 Agent
```

Do not change legal/business sample data unless it directly affects navigation.

- [ ] **Step 4: Verify navigation cleanup**

Run:

```powershell
rg -n 'href="#"|合同管理系统|合同管理Agent' ui
```

Expected:
- No sidebar or breadcrumb core navigation remains as `href="#"`.
- `合同管理系统` no longer appears in visible brand/sidebar text.
- Any remaining `href="#"` is limited to non-core placeholder actions.

- [ ] **Step 5: Commit**

Run:

```powershell
git add ui
git commit -m "fix: normalize ui prototype navigation"
```

Expected: Commit succeeds after user confirms committing is desired.

---

### Task 3: Make Agent Dashboard Flow Verifiable

**Files:**
- Modify: `ui/dashboard.html`

- [ ] **Step 1: Locate dashboard flow buttons**

Run:

```powershell
rg -n "上传文件|查看完整报告|导出PDF|输入消息|服务状态|ES|Milvus|Redis" ui\dashboard.html
```

Expected: Output includes the upload action, report action, export action, chat input, and service status.

- [ ] **Step 2: Wire static report and compliance links**

In `ui/dashboard.html`, make the following static prototype changes:

```html
<a class="btn-blue" href="compliance-report.html">查看完整报告</a>
<a class="btn-outline" href="compliance-report.html">导出PDF</a>
```

For the upload/select-contract action area, use:

```html
<a class="quick-action" href="compliance.html">
  <span>上传文件</span>
</a>
```

Keep existing classes where possible; if the current elements are `button`, convert only these flow actions to `a` so they can be verified in a static file.

- [ ] **Step 3: Verify dashboard links**

Run:

```powershell
rg -n 'href="compliance.html"|href="compliance-report.html"' ui\dashboard.html
```

Expected: Output includes both `compliance.html` and `compliance-report.html`.

- [ ] **Step 4: Browser check**

Open `ui/dashboard.html` in a browser and click:

```text
上传文件 -> compliance.html
查看完整报告 -> compliance-report.html
导出PDF -> compliance-report.html
```

Expected: Each click navigates to the target static page.

- [ ] **Step 5: Commit**

Run:

```powershell
git add ui\dashboard.html
git commit -m "fix: wire dashboard prototype flows"
```

Expected: Commit succeeds after user confirms committing is desired.

---

### Task 4: Complete Contract Management Prototype Flow

**Files:**
- Modify: `ui/contracts.html`
- Modify: `ui/contract-detail.html`
- Modify: `ui/contract-diff.html`

- [ ] **Step 1: Inspect contract flow anchors and actions**

Run:

```powershell
rg -n "合同详情|合同对比|版本历史|到期|提醒|发起合规校验|导出PDF|contract-detail|contract-diff|compliance" ui\contracts.html ui\contract-detail.html ui\contract-diff.html
```

Expected: Output shows list/detail/diff/report-related UI text.

- [ ] **Step 2: Ensure list links reach detail and diff**

In `ui/contracts.html`, key row actions should use:

```html
<a class="action-link" href="contract-detail.html">查看</a>
<a class="action-link" href="contract-diff.html">对比</a>
```

If existing class names differ, keep the existing class and only change the element/href/text needed for static navigation.

- [ ] **Step 3: Ensure detail actions reach compliance and diff**

In `ui/contract-detail.html`, key actions should use:

```html
<a class="btn-primary" href="compliance.html">发起合规校验</a>
<a class="btn-ghost" href="contract-diff.html">版本对比</a>
<a class="btn-ghost" href="compliance-report.html">查看合规报告</a>
```

Do not implement real PDF export; keep export as a visible prototype action if already present.

- [ ] **Step 4: Verify contract links**

Run:

```powershell
rg -n 'href="contract-detail.html"|href="contract-diff.html"|href="compliance.html"|href="compliance-report.html"' ui\contracts.html ui\contract-detail.html
```

Expected: Output includes all four target pages.

- [ ] **Step 5: Browser check**

Open `ui/contracts.html` in a browser and verify:

```text
合同列表 -> 合同详情
合同列表 -> 合同对比
合同详情 -> 合规校验
合同详情 -> 合规报告
```

Expected: Each path opens a real static page.

- [ ] **Step 6: Commit**

Run:

```powershell
git add ui\contracts.html ui\contract-detail.html ui\contract-diff.html
git commit -m "fix: complete contract prototype flow"
```

Expected: Commit succeeds after user confirms committing is desired.

---

### Task 5: Complete Compliance Check And Report Flow

**Files:**
- Modify: `ui/compliance.html`
- Modify: `ui/compliance-report.html`

- [ ] **Step 1: Inspect compliance actions and risk content**

Run:

```powershell
rg -n "单份校验|批量校验|最近校验报告|查看|导出PDF|高风险|中风险|低风险|修改建议|违反法规" ui\compliance.html ui\compliance-report.html
```

Expected: Output includes single/batch check actions, recent reports, and risk detail fields.

- [ ] **Step 2: Wire check actions to static report**

In `ui/compliance.html`, use these static targets:

```html
<a class="btn-primary" href="compliance-report.html">开始校验</a>
<a class="btn-primary" href="compliance-report.html">批量校验</a>
<a class="btn-ghost" href="compliance-report.html">查看</a>
```

Keep existing classes where possible; convert only flow-verification buttons to anchors.

- [ ] **Step 3: Keep report export as prototype navigation**

In `ui/compliance-report.html`, use:

```html
<a class="btn-blue" href="compliance-report.html">导出PDF</a>
```

This is intentionally a static self-link until a backend export endpoint exists.

- [ ] **Step 4: Verify compliance links and risk fields**

Run:

```powershell
rg -n 'href="compliance-report.html"|高风险|中风险|低风险|修改建议|违反法规' ui\compliance.html ui\compliance-report.html
```

Expected: Output includes report links plus all risk levels and report detail labels.

- [ ] **Step 5: Browser check**

Open `ui/compliance.html` and verify:

```text
开始校验 -> compliance-report.html
批量校验 -> compliance-report.html
最近报告查看 -> compliance-report.html
```

Expected: Each action opens the report page.

- [ ] **Step 6: Commit**

Run:

```powershell
git add ui\compliance.html ui\compliance-report.html
git commit -m "fix: complete compliance prototype flow"
```

Expected: Commit succeeds after user confirms committing is desired.

---

### Task 6: Complete Regulations Prototype Entry States

**Files:**
- Modify: `ui/regulations.html`

- [ ] **Step 1: Inspect regulations controls**

Run:

```powershell
rg -n "全部|法律|行政法规|部门规章|司法解释|语义搜索|关键词|同步官方法规|上传文件|查看|详情|最后同步" ui\regulations.html
```

Expected: Output includes category tabs, search mode controls, sync/upload actions, and table actions.

- [ ] **Step 2: Ensure static table actions are explicit**

For table row actions in `ui/regulations.html`, use visible static actions like:

```html
<a class="action-link" href="regulations.html">查看</a>
<a class="action-link" href="regulations.html">引用</a>
```

Keep them as self-links because no regulation detail page exists in the current prototype.

- [ ] **Step 3: Preserve sync/upload as visual prototype actions**

Keep these as buttons unless a target page exists:

```html
<button class="btn-outline">同步官方法规</button>
<button class="btn-ghost">上传文件</button>
```

Do not add fake upload JavaScript.

- [ ] **Step 4: Verify regulations controls**

Run:

```powershell
rg -n "同步官方法规|上传文件|语义搜索|关键词|最后同步|href=\"regulations.html\"" ui\regulations.html
```

Expected: Output confirms search, sync, upload, and static row links are visible.

- [ ] **Step 5: Browser check**

Open `ui/regulations.html`.

Expected: The page visibly supports filtering, search mode selection, sync, upload, and row actions without layout breakage.

- [ ] **Step 6: Commit**

Run:

```powershell
git add ui\regulations.html
git commit -m "fix: complete regulations prototype entries"
```

Expected: Commit succeeds after user confirms committing is desired.

---

### Task 7: Verify Settings Tabs And Notification Entry Consistency

**Files:**
- Modify: `ui/settings.html`
- Modify if needed: `ui/dashboard.html`
- Modify if needed: `ui/contracts.html`
- Modify if needed: `ui/contract-detail.html`
- Modify if needed: `ui/compliance.html`
- Modify if needed: `ui/compliance-report.html`
- Modify if needed: `ui/regulations.html`

- [ ] **Step 1: Inspect settings tabs and notification entries**

Run:

```powershell
rg -n "模型配置|用户管理|API Key|审计日志|通知|icon-btn|铃铛|badge|未读" ui\settings.html ui\dashboard.html ui\contracts.html ui\contract-detail.html ui\compliance.html ui\compliance-report.html ui\regulations.html
```

Expected: Output includes all four settings tabs and topbar notification affordances.

- [ ] **Step 2: Keep settings tab JavaScript minimal**

`ui/settings.html` should retain simple tab switching:

```html
<script>
function switchTab(el, tabId) {
  document.querySelectorAll('.tab').forEach(function(tab) { tab.classList.remove('active'); });
  document.querySelectorAll('.tab-content').forEach(function(content) { content.classList.remove('active'); });
  el.classList.add('active');
  document.getElementById(tabId).classList.add('active');
}
function activateSegment(el) {
  el.parentElement.querySelectorAll('.segmented-item').forEach(function(item) { item.classList.remove('active'); });
  el.classList.add('active');
}
</script>
```

If the existing code already matches this behavior, do not change it.

- [ ] **Step 3: Verify settings tab switching**

Open `ui/settings.html` in a browser and click:

```text
模型配置
用户管理
API Key
审计日志
```

Expected: Only the selected tab content is visible after each click.

- [ ] **Step 4: Verify notification entry exists on major pages**

Run:

```powershell
rg -l "通知|badge|badge-dot|dot" ui\dashboard.html ui\contracts.html ui\contract-detail.html ui\compliance.html ui\compliance-report.html ui\regulations.html ui\settings.html
```

Expected: Output includes the major app pages. If a page is missing notification affordance, add only the smallest matching topbar icon treatment used by nearby pages.

- [ ] **Step 5: Commit**

Run:

```powershell
git add ui\settings.html ui\dashboard.html ui\contracts.html ui\contract-detail.html ui\compliance.html ui\compliance-report.html ui\regulations.html
git commit -m "fix: align settings and notification prototype"
```

Expected: Commit succeeds after user confirms committing is desired.

---

### Task 8: Final Browser And Static Verification

**Files:**
- Verify: `ui/login.html`
- Verify: `ui/dashboard.html`
- Verify: `ui/contracts.html`
- Verify: `ui/contract-detail.html`
- Verify: `ui/contract-diff.html`
- Verify: `ui/compliance.html`
- Verify: `ui/compliance-report.html`
- Verify: `ui/regulations.html`
- Verify: `ui/settings.html`
- Modify: `tasks/todo.md`

- [ ] **Step 1: Run static link scan**

Run:

```powershell
rg -n 'href="(dashboard|contracts|contract-detail|contract-diff|compliance|compliance-report|regulations|settings)\\.html"' ui
```

Expected: Output shows links among all core prototype pages.

- [ ] **Step 2: Run placeholder scan**

Run:

```powershell
rg -n 'href="#"|占位|临时|未完成' ui tasks docs\superpowers\plans\2026-05-11-ui-design-alignment.md
```

Expected:
- No unresolved placeholder markers in the plan or changed UI.
- Remaining `href="#"` entries, if any, are non-core placeholders such as forgot-password or unavailable backend operations.

- [ ] **Step 3: Browser check all pages**

Open each file:

```text
ui/login.html
ui/dashboard.html
ui/contracts.html
ui/contract-detail.html
ui/contract-diff.html
ui/compliance.html
ui/compliance-report.html
ui/regulations.html
ui/settings.html
```

Expected:
- No obvious text overlap.
- No broken main navigation.
- Core content is visible at desktop width.
- Interactive tabs in `contract-detail.html` and `settings.html` still work.

- [ ] **Step 4: Record final review**

Append this section to `tasks/todo.md`:

```markdown

### 实施 Review

- 已完成：UI 静态原型已按设计文档核心页面和流程入口对齐。
- 验证命令：检查 `href="#"`、临时标记和占位文案是否仍存在于核心流程中。
- 浏览器验证：已逐页打开 `ui/*.html` 检查主流程、布局和标签切换。
- 未实现：真实 API、文件上传、PDF 导出、WebSocket 通知、Next.js 工程化迁移。
```

- [ ] **Step 5: Commit**

Run:

```powershell
git add ui tasks\todo.md
git commit -m "test: verify ui prototype alignment"
```

Expected: Commit succeeds after user confirms committing is desired.

---

## Self-Review

- Spec coverage: 第 9.2 的 Agent 对话、合同管理、合同对比、合规校验、法规知识库、系统设置均有任务覆盖；第 9.3 通知系统覆盖到静态入口，不覆盖 WebSocket 实时推送。
- Placeholder scan: 本计划没有留下未定义的执行内容；静态自链接仅用于没有详情页或后端能力的原型动作。
- Type consistency: 本计划不引入新类型或接口；页面路径统一使用现有 HTML 文件名。

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-05-11-ui-design-alignment.md`. Two execution options:

1. Subagent-Driven (recommended) - dispatch a fresh subagent per task, review between tasks, fast iteration
2. Inline Execution - execute tasks in this session using executing-plans, batch execution with checkpoints

Which approach?
