#!/usr/bin/env python3
import argparse
import html
import json
from datetime import datetime
from pathlib import Path
from urllib.parse import quote

import pandas as pd

from verify.verify_contract import build_verify_request

BASE_DIR = Path(__file__).resolve().parent
DEFAULT_OUTPUT_DIR = Path('/mnt/fn/Download3/clawdbotfile/财务/发票_解析结果')
OUTPUT_DIR = DEFAULT_OUTPUT_DIR
VERIFY_PREPARED_FILE = OUTPUT_DIR / '查验准备清单.csv'
VERIFY_STATUS_LEDGER = OUTPUT_DIR / '查验状态台账.csv'
MAIN_CSV = OUTPUT_DIR / '发票汇总.csv'
VERIFY_TEMPLATE = OUTPUT_DIR / '查验结果录入模板.csv'
VERIFY_DASHBOARD = OUTPUT_DIR / '查验辅助总览.html'
VERIFY_TASKS_DIR = OUTPUT_DIR / 'verify_tasks'
VERIFY_TASKS_JSON = VERIFY_TASKS_DIR / 'ready_tasks.json'
VERIFY_TASKS_README = VERIFY_TASKS_DIR / 'README.txt'
OFFICIAL_VERIFY_URL = 'https://inv-veri.chinatax.gov.cn/'


def read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path, dtype=str).fillna('')
    except pd.errors.EmptyDataError:
        return pd.DataFrame()


def main() -> None:
    global OUTPUT_DIR, VERIFY_PREPARED_FILE, VERIFY_STATUS_LEDGER, MAIN_CSV, VERIFY_TEMPLATE, VERIFY_DASHBOARD, VERIFY_TASKS_DIR, VERIFY_TASKS_JSON, VERIFY_TASKS_README

    parser = argparse.ArgumentParser(description='从 parser 输出生成查验辅助文件和 ready_tasks.json')
    parser.add_argument('--output-dir', default=str(DEFAULT_OUTPUT_DIR), help='parser 输出目录')
    args = parser.parse_args()

    OUTPUT_DIR = Path(args.output_dir)
    VERIFY_PREPARED_FILE = OUTPUT_DIR / '查验准备清单.csv'
    VERIFY_STATUS_LEDGER = OUTPUT_DIR / '查验状态台账.csv'
    MAIN_CSV = OUTPUT_DIR / '发票汇总.csv'
    VERIFY_TEMPLATE = OUTPUT_DIR / '查验结果录入模板.csv'
    VERIFY_DASHBOARD = OUTPUT_DIR / '查验辅助总览.html'
    VERIFY_TASKS_DIR = OUTPUT_DIR / 'verify_tasks'
    VERIFY_TASKS_JSON = VERIFY_TASKS_DIR / 'ready_tasks.json'
    VERIFY_TASKS_README = VERIFY_TASKS_DIR / 'README.txt'

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    VERIFY_TASKS_DIR.mkdir(parents=True, exist_ok=True)

    prepared_df = read_csv(VERIFY_PREPARED_FILE)
    status_df = read_csv(VERIFY_STATUS_LEDGER)
    main_df = read_csv(MAIN_CSV)

    if prepared_df.empty or status_df.empty:
        VERIFY_TASKS_JSON.write_text('[]', encoding='utf-8')
        VERIFY_TASKS_README.write_text(
            '国税查验工作流\n\n'
            '当前没有可用查验源数据或 parser 输出为空。\n',
            encoding='utf-8'
        )
        print('No verify source files found. Run parser.py first.')
        return

    merge_cols = [c for c in ['file_hash', 'duplicate_key', 'file_name', 'file_path', 'invoice_number', 'invoice_date', 'seller_name', 'total_amount'] if c in status_df.columns]
    keep_cols = merge_cols + [
        'verify_task_id', 'verify_status_code', 'verify_result_code',
        'verify_invoice_number', 'verify_invoice_date', 'verify_total_amount',
        'verify_check_code_last6_or_last6_hint', 'verify_invoice_type',
        'verify_required_fields_status', 'verify_missing_fields', 'verify_status',
        'verify_time', 'verify_channel', 'verify_result_summary',
        'is_voided', 'is_abnormal', 'verify_screenshot_path', 'verify_raw_result_path'
    ]
    status_subset = status_df[[c for c in keep_cols if c in status_df.columns]].copy()

    df = prepared_df.merge(
        status_subset,
        on=[c for c in ['file_hash', 'duplicate_key'] if c in prepared_df.columns and c in status_subset.columns],
        how='left',
        suffixes=('', '_status')
    )

    if not main_df.empty:
        enrich_cols = [c for c in ['file_hash', 'duplicate_key', 'invoice_code', 'expense_category', 'buyer_name', 'buyer_tax_no', 'seller_tax_no', 'parse_status', 'review_needed', 'review_reason'] if c in main_df.columns]
        df = df.merge(
            main_df[enrich_cols],
            on=[c for c in ['file_hash', 'duplicate_key'] if c in main_df.columns and c in df.columns],
            how='left'
        )

    for col in ['verify_task_id', 'verify_status_code', 'verify_result_code', 'verify_status', 'verify_time', 'verify_channel', 'verify_result_summary', 'is_voided', 'is_abnormal', 'verify_screenshot_path', 'verify_raw_result_path']:
        if col not in df.columns:
            df[col] = ''

    template_df = pd.DataFrame([
        {
            'verify_task_id': row.get('verify_task_id', ''),
            'verify_status_code': row.get('verify_status_code', ''),
            'verify_result_code': row.get('verify_result_code', ''),
            'file_hash': row.get('file_hash', ''),
            'duplicate_key': row.get('duplicate_key', ''),
            'file_name': row.get('file_name', ''),
            'file_path': row.get('file_path', ''),
            'invoice_code': row.get('invoice_code', ''),
            'invoice_number': row.get('verify_invoice_number', row.get('invoice_number', '')),
            'invoice_date': row.get('verify_invoice_date', row.get('invoice_date', '')),
            'invoice_type': row.get('verify_invoice_type', ''),
            'total_amount': row.get('verify_total_amount', row.get('total_amount', '')),
            'check_code_last6_or_hint': row.get('verify_check_code_last6_or_last6_hint', ''),
            'seller_name': row.get('seller_name', ''),
            'seller_tax_no': row.get('seller_tax_no', ''),
            'buyer_name': row.get('buyer_name', ''),
            'buyer_tax_no': row.get('buyer_tax_no', ''),
            'expense_category': row.get('expense_category', ''),
            'verify_required_fields_status': row.get('verify_required_fields_status', ''),
            'verify_missing_fields': row.get('verify_missing_fields', ''),
            'verify_status': row.get('verify_status', '未查验') or '未查验',
            'verify_time': row.get('verify_time', ''),
            'verify_channel': row.get('verify_channel', ''),
            'verify_result_summary': row.get('verify_result_summary', ''),
            'is_voided': row.get('is_voided', ''),
            'is_abnormal': row.get('is_abnormal', ''),
            'verify_screenshot_path': row.get('verify_screenshot_path', ''),
            'verify_raw_result_path': row.get('verify_raw_result_path', ''),
            'operator': '',
            'notes': '',
        }
        for _, row in df.iterrows()
    ])
    template_df.to_csv(VERIFY_TEMPLATE, index=False, encoding='utf-8-sig')

    ready_df = template_df[(template_df['verify_required_fields_status'] == 'ready') & (template_df['verify_status'].isin(['', '未查验', '待查验']))].copy()
    ready_tasks = [build_verify_request(row) for row in ready_df.to_dict(orient='records')]
    VERIFY_TASKS_JSON.write_text(json.dumps(ready_tasks, ensure_ascii=False, indent=2), encoding='utf-8')
    VERIFY_TASKS_README.write_text(
        '国税查验工作流\n\n'
        '1. 先运行 parser.py 或 run.sh，更新解析结果\n'
        '2. 打开 查验辅助总览.html 查看待查验任务\n'
        '3. 到官方查验页面核验，填写 查验结果录入模板.csv（建议另存为 查验结果录入.csv）\n'
        '4. 运行 python3 apply_verify_results.py 合并查验结果并重建报销输出\n',
        encoding='utf-8'
    )

    def file_link(path_str: str, label: str) -> str:
        if not path_str:
            return ''
        return f"<a href='file://{quote(path_str)}'>{html.escape(label)}</a>"

    cards = {
        '总发票数': len(template_df),
        '可直接查验': int((template_df['verify_required_fields_status'] == 'ready').sum()),
        '待补字段': int((template_df['verify_required_fields_status'] != 'ready').sum()),
        '未查验': int(template_df['verify_status'].isin(['', '未查验', '待查验']).sum()),
        '已通过': int((template_df['verify_status'] == '查验通过').sum()),
        '失败/异常': int(template_df['verify_status'].isin(['查验失败', '异常', '需人工复核']).sum()),
    }

    rows_html = ''
    for _, row in template_df.iterrows():
        status = row.get('verify_status', '') or '未查验'
        cls = 'ok' if status == '查验通过' else ('warn' if status in ['查验失败', '异常', '需人工复核'] else 'todo')
        rows_html += (
            '<tr>'
            f"<td>{file_link(row.get('file_path', ''), row.get('file_name', '') or '原文件')}</td>"
            f"<td>{html.escape(row.get('invoice_number', ''))}</td>"
            f"<td>{html.escape(row.get('invoice_date', ''))}</td>"
            f"<td>{html.escape(row.get('invoice_type', ''))}</td>"
            f"<td>{html.escape(row.get('seller_name', ''))}</td>"
            f"<td>{html.escape(row.get('total_amount', ''))}</td>"
            f"<td>{html.escape(row.get('check_code_last6_or_hint', ''))}</td>"
            f"<td>{html.escape(row.get('verify_required_fields_status', ''))}</td>"
            f"<td>{html.escape(row.get('verify_missing_fields', ''))}</td>"
            f"<td class='{cls}'>{html.escape(status)}</td>"
            f"<td>{html.escape(row.get('verify_result_summary', ''))}</td>"
            f"<td><a href='{OFFICIAL_VERIFY_URL}' target='_blank'>官方查验页</a></td>"
            '</tr>'
        )

    cards_html = ''.join([f"<div class='card'><div>{html.escape(k)}</div><strong>{v}</strong></div>" for k, v in cards.items()])
    html_text = f"""<!doctype html>
<html lang='zh-CN'>
<head>
<meta charset='utf-8'>
<title>查验辅助总览</title>
<style>
body{{font-family:Arial,'PingFang SC','Microsoft YaHei',sans-serif;margin:24px;color:#222;}}
.card{{display:inline-block;padding:16px 20px;margin:0 12px 12px 0;border-radius:12px;background:#f5f7fb;min-width:160px;}}
table{{border-collapse:collapse;width:100%;margin-top:16px;}}
th,td{{border:1px solid #ddd;padding:8px 10px;font-size:13px;text-align:left;vertical-align:top;}}
th{{background:#f2f2f2;position:sticky;top:0;}}
.ok{{color:#027a48;font-weight:600;}}
.warn{{color:#b54708;font-weight:600;}}
.todo{{color:#175cd3;font-weight:600;}}
code{{background:#f4f4f4;padding:2px 6px;border-radius:6px;}}
a{{color:#175cd3;text-decoration:none;}}
a:hover{{text-decoration:underline;}}
</style>
</head>
<body>
<h1>国税查验辅助总览</h1>
<p>官方查验入口：<a href='{OFFICIAL_VERIFY_URL}' target='_blank'>{OFFICIAL_VERIFY_URL}</a></p>
<p>生成时间：<code>{datetime.now().isoformat(timespec='seconds')}</code></p>
{cards_html}
<p>录入规则：先在官方页面完成查验，再把结果填入 <code>{VERIFY_TEMPLATE.name}</code>（建议复制一份命名为 <code>查验结果录入.csv</code>），然后运行 <code>python3 apply_verify_results.py</code> 回填到主报表。</p>
<table>
<tr>
<th>原文件</th><th>发票号码</th><th>开票日期</th><th>票种</th><th>销方</th><th>价税合计</th><th>校验码后6位</th><th>查验准备</th><th>缺失字段</th><th>查验状态</th><th>结果摘要</th><th>操作</th>
</tr>
{rows_html}
</table>
</body>
</html>
"""
    VERIFY_DASHBOARD.write_text(html_text, encoding='utf-8')
    print(f'Built verify assets -> {VERIFY_DASHBOARD}')


if __name__ == '__main__':
    main()
