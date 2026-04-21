# Invoice Parser - 电子发票解析查验工具

## 注意事项

> ⚠️ 本项目依赖 `wlop.js` 签名脚本，该文件不包含在仓库中。
> 首次运行 `verify/run_verifier.py` 时会自动从国家税务总局查验平台下载。
> 
> 请确保你有合法的使用场景（如企业内部发票核验）。

## 功能

- **多格式解析**：PDF / XML / OFD
- **按购方自动分桶**：解析结果按购方实体自动路由到独立目录
- **自动验证码识别**：ddddocr + 颜色过滤
- **双轨查验**：API headless 批量查验 + 浏览器官方截图取证
- **Web 工作台**：FastAPI + 原生前端，端口 8787

## 快速开始

```bash
# 安装依赖
pip install -r requirements.txt
apt install poppler-utils  # PDF 文本提取

# 启动工作台
python captcha_workbench.py --port 8787

# 或命令行解析
python parser.py --input-dir /path/to/invoices --output-dir /path/to/output
```

## 环境变量

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `OCR_BASE_URL` | `http://localhost:17861` | OCR 服务地址（可选） |
| `DISPLAY` | - | X 显示（浏览器截图需要） |

## 项目结构

```
├── parser.py                  # 发票解析（PDF/XML/OFD + 分桶）
├── captcha_workbench.py       # Web 工作台
├── verify_browser_assist.py   # 浏览器查验 + 截图
├── build_verify_assets.py     # 查验辅助生成
├── apply_verify_results.py    # 查验结果回填
├── verify/                    # 查验核心
│   ├── run_verifier.py        # API 签名 + 调用
│   ├── swjg_map.py            # 省级端点映射
│   ├── captcha_solver.py      # 验证码识别
│   ├── ocr_client.py          # OCR 服务客户端
│   ├── verify_contract.py     # 标准契约
│   └── upstream_js/           # 运行时自动下载
└── docs/
```

## 支持的省份

32/36 个省级查验端点可用（88.9%）。不可用：吉林/江西/四川/甘肃。

## License

MIT
