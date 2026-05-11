# UI 原型对齐设计文档计划

> 状态：待用户确认后实施
> 依据：`docs/2026-05-11-contract-agent-design.md`
> 范围：`ui/` 下现有静态 HTML 原型

## 假设与取舍

- 假设本轮目标是“先写中文实施计划”，不是立即改 UI。
- 假设当前 `ui/` 是静态 HTML 原型，短期目标是让原型更完整、更一致、更可验证。
- 设计文档目标技术栈是 React 18 + Next.js 14 + Zustand + Ant Design；但仓库当前没有前端工程脚手架。更简单的路径是先完善静态原型，再决定是否迁移到 Next.js。
- 如果目标实际是“直接迁移为 Next.js 工程”，需要把计划拆成新一轮工程化计划，避免在静态原型里混入半套 React 结构。

## 成功标准

- `ui/` 中核心页面与设计文档第 9 章核心页面清单一致：Agent 对话、合同管理、合同对比、合规校验、法规知识库、系统设置。
- 页面间主导航不再出现核心路径的空链接；详情、报告、对比等关键流程可以从原型中自然跳转。
- 静态原型覆盖设计文档提到的到期提醒、批量合规、导出报告、法规同步/上传、通知入口等 P0 交互入口。
- 用浏览器逐页打开 HTML 验证无明显布局破损、文字重叠和关键链接断裂。

## 待执行任务

- [x] **步骤 1：建立 UI 覆盖矩阵**
  - 文件：`docs/2026-05-11-contract-agent-design.md`、`ui/*.html`
  - 动作：列出设计文档第 9.2/9.3 的页面与现有 HTML 文件的对应关系。
  - 验证：确认每个核心页面至少有一个对应 HTML 文件；缺口写入计划 review。

- [x] **步骤 2：统一侧边栏和主导航路径**
  - 文件：`ui/dashboard.html`、`ui/contracts.html`、`ui/contract-detail.html`、`ui/contract-diff.html`、`ui/compliance.html`、`ui/compliance-report.html`、`ui/regulations.html`、`ui/settings.html`
  - 动作：仅修正核心导航的 `href="#"`、不一致品牌名、当前页高亮和返回路径。
  - 验证：运行 `rg -n 'href="#"|合同管理系统|合同管理Agent' ui`，确认剩余结果都是非核心占位或有明确原因。

- [x] **步骤 3：补齐 Agent 对话到业务页面的关键跳转**
  - 文件：`ui/dashboard.html`
  - 动作：让“上传文件”“查看完整报告”“导出PDF”等按钮在静态原型里表现为可验证入口；不增加真实上传逻辑。
  - 验证：从 `ui/dashboard.html` 能进入合规报告或合规校验页面。

- [x] **步骤 4：补齐合同管理流程入口**
  - 文件：`ui/contracts.html`、`ui/contract-detail.html`、`ui/contract-diff.html`
  - 动作：确保列表、详情、版本历史、合同对比、到期提醒入口与设计文档描述一致。
  - 验证：从合同列表能进入详情页和对比页；详情页能发起合规校验。

- [x] **步骤 5：补齐合规校验与报告流程入口**
  - 文件：`ui/compliance.html`、`ui/compliance-report.html`
  - 动作：保持单份/批量校验、风险项卡片、状态处理和导出 PDF 的静态原型完整。
  - 验证：从合规校验页能进入报告页；报告页风险项显示高/中/低风险、法规引用和修改建议。

- [x] **步骤 6：补齐法规知识库入口状态**
  - 文件：`ui/regulations.html`
  - 动作：保留类型筛选、语义/关键词搜索、官方同步、上传文件、查看条文入口；避免实现后端同步逻辑。
  - 验证：页面可清晰表达“搜索、同步、上传、查看”四类操作。

- [ ] **步骤 7：补齐系统设置与通知入口一致性**
  - 文件：`ui/settings.html` 以及含通知铃铛的页面
  - 动作：校准模型配置、用户管理、API Key、审计日志四个标签页；通知入口保持统一视觉与未读状态。
  - 验证：设置页标签切换可用；顶部通知入口在主要页面保持一致。

- [ ] **步骤 8：浏览器验收**
  - 文件：`ui/*.html`
  - 动作：逐页打开静态 HTML，检查桌面宽度下布局、文字、按钮、链接和滚动。
  - 验证：至少覆盖 `login.html`、`dashboard.html`、`contracts.html`、`contract-detail.html`、`contract-diff.html`、`compliance.html`、`compliance-report.html`、`regulations.html`、`settings.html`。

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

## Review

- 已完成：根据设计文档和现有 `ui/` 文件形成中文实施计划。
- 已完成：Task 3 将 `dashboard.html` 中“上传文件”链接到合规校验页，将“查看完整报告”“导出PDF”链接到合规报告页，并完成 `rg` 验证。
- 已完成：Task 4 将合同列表“查看/对比”改为静态跳转入口，并让合同详情页可进入合规校验、版本对比和合规报告。
- 已完成：Task 5 将合规校验页单份/批量校验和最近报告“查看”统一链接到合规报告页，并将报告页“导出PDF”保持为静态自链接。
- 已完成：Task 6 保留法规知识库搜索、同步、上传入口，并将表格行“查看/引用”改为静态自链接。
- 未实施：Task 7 及后续 UI 对齐步骤尚未执行。
- 风险：如果用户期望的是 Next.js 正式工程，而不是静态 HTML 原型完善，需要重写计划范围。
