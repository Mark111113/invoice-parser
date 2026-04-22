# invoice-parser

电子发票解析、查验、归档一站式工具。

## 功能概览

### 核心能力
- **多格式解析**：PDF（pdftotext）、XML（结构化）、OFD（ZIP/XML）
- **按购方自动分桶输出**：解析结果按购方实体自动路由到独立目录
- **自动验证码识别**：ddddocr + 颜色过滤，约 67% 准确率（配合重试可达 96%）
- **双轨查验**：API headless 批量查验 + 浏览器官方截图取证
- **Web 工作台**：http://localhost:8787

### 输入格式
| 格式 | 解析方式 | 状态 |
|------|---------|------|
| PDF | pdftotext 提取文本 + 正则 | ✅ 稳定 |
| XML | etree 结构化解析 | ✅ 稳定 |
| OFD | ZIP 解压 + XML 文本提取 | ⚠️ 基础实现（待真实样本验证） |

### 输出目录结构
解析后按购方实体自动分发到不同目录：
```
/mnt/fn/Download3/clawdbotfile/财务/
├── 发票_解析结果_苏州奥伟尔科技有限公司/
│   ├── entity_meta.json
│   ├── 发票解析结果.json
│   ├── 发票汇总.csv / .xlsx
│   ├── 查验状态台账.csv
│   ├── 查验准备清单.csv
│   ├── uploads/           ← 上传的原文件按购方归档
│   ├── verify_tasks/
│   └── verify_results/
│       ├── screenshots/   ← 官方页面截图
│       └── captcha/       ← 验证码图片
├── 发票_解析结果_个人/
└── 发票_解析结果_待确认购方/
```

分桶规则：
- **分组键**：优先 `buyer_tax_no`，其次 `buyer_name`，兜底 `待确认购方`
- **目录名**：`发票_解析结果_{buyer_name}`
- 同名不同税号时追加税号后缀避免混淆
- 每个目录含 `entity_meta.json` 记录主体信息

## 查验能力

### API 批量查验（主线）
- 直接调用省级税务 API（yzmQuery/vatQuery）
- 自动签名（wlop.js flwq39）
- 自动验证码识别 + 重试
- 覆盖 32/36 省份（88.9%）
- 不需要打开浏览器，速度快

### 浏览器补截图（辅线）
- 用 Playwright 打开真实国税页面
- 自动填表 + 自动验证码 + 自动截图
- 截图通过 HTTP 接口提供（支持 PNG/JPG）
- 不会覆盖 API 查验结论，只回填截图路径

### 省级端点覆盖
- ✅ 32/36 省份可用（2026-04-21 修正域名后实测）
- ❌ 吉林(SSL)/江西(断连)/四川(503)/甘肃(SSL)
- 关键修正：旧域名（如 `fpcy.tjsat.gov.cn`）→ 新域名（如 `fpcy.tianjin.chinatax.gov.cn`）

## 主要脚本

| 脚本 | 用途 |
|------|------|
| `parser.py` | 解析发票，支持按购方分桶输出 |
| `captcha_workbench.py` | Web 工作台（uvicorn），端口 8787 |
| `build_verify_assets.py` | 生成查验辅助文件和 ready_tasks.json |
| `apply_verify_results.py` | 回填查验结果到台账 |
| `verify_browser_assist.py` | 浏览器辅助查验 + 官方截图 |
| `verify_contract.py` | parser / verifier 对接契约 |
| `captcha_solver.py` | ddddocr + 颜色过滤验证码识别 |

## 运行

### Linux / macOS

#### 安装依赖
```bash
pip install -r requirements.txt
apt install poppler-utils  # 提供 pdftotext
playwright install chromium
```

#### Web 工作台（推荐）
```bash
# 服务已部署为 systemd service
sudo systemctl start invoice-workbench
# 访问 http://localhost:8787
```

#### 命令行解析
```bash
# 自动按购方分桶
python3 parser.py --input-dir /path/to/invoices --output-dir /path/to/output_root

# 生成查验任务
python3 build_verify_assets.py --output-dir /path/to/output_dir

# 浏览器辅助查验
python3 verify_browser_assist.py --headed --tasks-file /path/to/ready_tasks.json

# 回填查验结果
python3 apply_verify_results.py --output-dir /path/to/output_dir
```

### Windows

#### 1. 创建并激活 conda 环境
```bat
conda create -n ocr312 python=3.12 -y
conda activate ocr312
```

#### 2. 安装 Python 依赖
```bat
pip install -r requirements.txt
playwright install chromium
```

#### 3. 安装 Poppler（提供 `pdftotext.exe`）
推荐直接用 conda：
```bat
conda install -c conda-forge poppler -y
```

验证：
```bat
where pdftotext
pdftotext -v
```

#### 4. 启动 Web 工作台
```bat
python captcha_workbench.py --port 8787
```
访问：<http://localhost:8787>

#### 5. Windows 首次使用说明（重要）
- 首次启动时，如果还没有任何 `发票_解析结果_*` 目录，页面下拉框可能是空的，这是正常的。
- 现在可以直接在 **“手填/新建输出目录”** 输入一个目录，例如：
  - `D:\invoice_output`
  - `C:\Users\YourName\Documents\invoice_output`
- 点击 **“使用/创建该目录”** 后，就可以继续：
  - 上传 PDF/XML/OFD
  - 或填写输入目录后点“解析”
- 上传和解析都会自动在这个目录下创建后续所需的子目录与结果文件。

#### 6. 命令行解析
```bat
python parser.py --input-dir D:\invoices --output-dir D:\invoice_output
python build_verify_assets.py --output-dir D:\invoice_output\发票_解析结果_某主体
```

## 对接契约

### verify_request (`ready_tasks.json`)
- `schema_version = invoice-verify-request.v1`
- `task_id` / `match_keys` / `invoice` / `business`

### verify_result (`verify_results/*.json`)
- `schema_version = invoice-verify-result.v1`
- `task_id` / `verify_status_code` / `verify_result_code` / `artifacts`

## API 端点（captcha_workbench.py）

| 端点 | 方法 | 用途 |
|------|------|------|
| `/` | GET | Web 工作台首页 |
| `/api/parsed_invoices` | GET | 当前目录发票列表 |
| `/api/tasks` | GET | 当前目录查验任务 |
| `/api/parse_input_dir` | POST | 解析输入目录（按购方分桶） |
| `/api/upload_pdfs` | POST | 上传文件（按购方分桶归档） |
| `/api/fetch_captcha` | POST | 获取验证码 |
| `/api/submit_captcha` | POST | 提交查验 |
| `/api/bulk_start` | POST | 批量查验 |
| `/api/bulk_stop` | POST | 停止批量 |
| `/api/bulk_status` | GET | 批量进度 |
| `/api/capture_verify_screenshot` | POST | 单张补截图 |
| `/api/screenshot/{file_hash}` | GET | 查看/下载截图（?format=jpg） |
| `/api/reset_invoice_to_pending` | POST | 重置为待查验 |
| `/api/delete_invoice` | POST | 删除发票记录 |
| `/api/select_output_dir` | POST | 切换输出目录 |
| `/api/rebuild` | POST | 重建查验任务 |

## 验证码识别

- **方案**：ddddocr + 颜色过滤
- **颜色规则**：key4=01→红色, key4=03→蓝色, key4=00/02→全部
- **准确率**：~67%（颜色验证码），配合 3 次重试 ~96%
- **文件**：`captcha_solver.py`（在 invoice-verifier 项目中）
