#!/usr/bin/env python3
"""
浏览器辅助国税查验脚本

用法:
  python3 verify_browser_assist.py                    # 默认无头模式（终端输入验证码）
  python3 verify_browser_assist.py --headed          # 有头模式（浏览器可见，人工在页面输入验证码并点击查验）
  python3 verify_browser_assist.py --task 0          # 只查验第 N 张（0-based）
  python3 verify_browser_assist.py --max-retries 3   # 最大重试次数

流程:
  1. 读取 verify_tasks/ready_tasks.json
  2. 自动打开国税查验页并填入发票号码 / 开票日期 / 金额
  3. 无头模式：终端输入验证码，由脚本提交
  4. 有头模式：人工直接在浏览器页面输入验证码并点击“查验”
  5. 脚本监听查验响应，自动截图、保存原始结果、回填台账
"""

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path

# Ensure verify/ (where captcha_solver.py lives) is importable when running as a script
_SCRIPT_DIR = Path(__file__).resolve().parent
_VERIFY_DIR = _SCRIPT_DIR / 'verify'
if str(_VERIFY_DIR) not in sys.path:
    sys.path.insert(0, str(_VERIFY_DIR))

from playwright.sync_api import sync_playwright

from verify.verify_contract import build_standard_result, extract_update_from_result

# Ensure invoice-verifier dir is on path for captcha_solver
import os
_iv_dir = os.path.join(os.path.dirname(__file__), '..', 'invoice-verifier')
if os.path.isdir(_iv_dir) and _iv_dir not in sys.path:
    sys.path.insert(0, _iv_dir)

# Auto captcha solver (ddddocr-based)
try:
    from captcha_solver import solve_captcha_from_file
    _CAPTCHA_SOLVER_AVAILABLE = True
except ImportError:
    _CAPTCHA_SOLVER_AVAILABLE = False

BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = Path('/mnt/fn/Download3/clawdbotfile/财务/发票_解析结果')
RUNTIME_DIR = Path(os.environ.get('VERIFY_RUNTIME_DIR', str(OUTPUT_DIR)))
DEFAULT_TASKS_FILE = OUTPUT_DIR / 'verify_tasks' / 'ready_tasks.json'
RESULTS_DIR = RUNTIME_DIR / 'verify_results'
CAPTCHA_DIR = RESULTS_DIR / 'captcha'
SCREENSHOTS_DIR = RESULTS_DIR / 'screenshots'
LEDGER_FILE = RUNTIME_DIR / '查验状态台账.csv'
OFFICIAL_URL = 'https://inv-veri.chinatax.gov.cn/'

RESULT_CODE_MAP = {
    '000': '查验成功',
    '001': '查验成功（曾经查验过）',
    '002': '查询失败（查无此票）',
    '003': '查验失败（发票信息不一致）',
    '004': '查验失败（查无此票）',
    '005': '查验失败（系统异常）',
    '006': '查验失败（输入有误）',
    '007': '查验失败（超过查验次数）',
    '008': '验证码错误',
    '009': '查验失败（数据不规范）',
    '010': '查验失败（超过当日查验次数）',
    '011': '查验失败（系统维护）',
}


def date_to_yyyymmdd(date_str: str) -> str:
    m = re.match(r'(\d{4})年(\d{1,2})月(\d{1,2})日', date_str)
    if m:
        return f'{m.group(1)}{m.group(2).zfill(2)}{m.group(3).zfill(2)}'
    if re.match(r'^\d{8}$', date_str):
        return date_str
    return date_str


def parse_jsonp(text: str) -> dict:
    result = {}
    m = re.search(r'\((\{.*\})\)', text, re.DOTALL)
    if m:
        try:
            return json.loads(m.group(1))
        except json.JSONDecodeError:
            pass
    for pair in text.split('&'):
        if '=' in pair:
            k, v = pair.split('=', 1)
            result[k.strip()] = v.strip()
    if not result:
        try:
            result = json.loads(text)
        except json.JSONDecodeError:
            result = {'raw': text}
    return result


def load_tasks(tasks_file: Path) -> list:
    if not tasks_file.exists():
        print(f'任务文件不存在: {tasks_file}')
        print('请先运行: python3 parser.py && python3 build_verify_assets.py')
        sys.exit(1)
    with open(tasks_file, 'r', encoding='utf-8') as f:
        return json.load(f)


def refill_form(page, inv_num: str, date_str: str, total_amount: str) -> None:
    amount_str = total_amount.replace('.00', '').replace('.0', '') if '.' in total_amount else total_amount
    page.fill('#fphm', inv_num)
    page.dispatch_event('#fphm', 'blur')
    page.wait_for_timeout(800)
    page.fill('#kprq', date_str)
    page.dispatch_event('#kprq', 'blur')
    page.wait_for_timeout(400)
    page.fill('#kjje', amount_str)
    page.dispatch_event('#kjje', 'blur')
    page.wait_for_timeout(400)


def _backfill_screenshot_only(task: dict, screenshot_path: str) -> None:
    """Only update verify_screenshot_path in ledger, preserving all other fields."""
    import pandas as pd
    if not LEDGER_FILE.exists():
        return
    df = pd.read_csv(LEDGER_FILE, dtype=str).fillna('')
    inv_num = (task.get('invoice', {}) or {}).get('invoice_number', '')
    mask = df['invoice_number'].astype(str) == str(inv_num) if 'invoice_number' in df.columns and inv_num else None
    if mask is None or not mask.any():
        # Try file_hash
        fh = (task.get('invoice', {}) or {}).get('file_hash', '')
        if fh and 'file_hash' in df.columns:
            mask = df['file_hash'].astype(str) == str(fh)
    if mask is None or not mask.any():
        print(f'  → 台账中未找到匹配行，跳过回填')
        return
    df.loc[mask, 'verify_screenshot_path'] = screenshot_path
    try:
        df.to_csv(LEDGER_FILE, index=False, encoding='utf-8-sig')
        print(f'  → 已回填截图路径到台账')
    except Exception as e:
        print(f'  → 台账回填失败: {e}')


def save_result(task: dict, result: dict, screenshot_path: str, raw_response: str) -> Path:
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    SCREENSHOTS_DIR.mkdir(parents=True, exist_ok=True)

    invoice = task.get('invoice', {}) or {}
    business = task.get('business', {}) or {}
    inv_num = invoice.get('invoice_number', 'unknown')
    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    result_file = RESULTS_DIR / f'{inv_num}_{ts}.json'
    raw_file = RESULTS_DIR / f'{inv_num}_{ts}_raw.txt'

    key1 = result.get('key1', '')
    status_text = RESULT_CODE_MAP.get(key1, f'未知({key1})') if key1 else '无响应'
    verify_time = datetime.now().isoformat(timespec='seconds')

    record = build_standard_result(
        task,
        verify_time=verify_time,
        verify_channel='国税总局查验平台(浏览器辅助)',
        result_code=key1,
        result_status=status_text,
        result_summary=status_text,
        is_success=key1 in ('000', '001'),
        is_voided='是' if str(result.get('key15', '')).upper() == 'Y' else '否',
        is_abnormal='否' if key1 in ('000', '001') else '是',
        screenshot_path=screenshot_path,
        raw_response_path=str(raw_file),
        raw_response=raw_response[:5000],
        full_result={
            **result,
            '_invoice_number': invoice.get('invoice_number', ''),
            '_invoice_date': invoice.get('invoice_date', ''),
            '_total_amount': invoice.get('total_amount', ''),
            '_seller_name': business.get('seller_name', ''),
        },
        source_kind='browser_assist',
    )

    with open(result_file, 'w', encoding='utf-8') as f:
        json.dump(record, f, ensure_ascii=False, indent=2)
    raw_file.write_text(raw_response, encoding='utf-8')
    return result_file


def update_ledger(task: dict, record: dict, preserve_existing_status: bool = False) -> None:
    import pandas as pd

    if not LEDGER_FILE.exists():
        print(f'  → 跳过台账更新（不存在）: {LEDGER_FILE}')
        return

    df = pd.read_csv(LEDGER_FILE, dtype=str).fillna('')
    update = extract_update_from_result(record)
    match_keys = [
        ('verify_task_id', update.get('verify_task_id', '')),
        ('file_hash', update.get('file_hash', '')),
        ('duplicate_key', update.get('duplicate_key', '')),
    ]
    mask = None
    for col, value in match_keys:
        if col in df.columns and value:
            current = df[col] == value
            if current.any():
                mask = current
                break
    if mask is None or not mask.any():
        return

    allowed_cols = set(update.keys())
    if preserve_existing_status:
        allowed_cols = {'verify_screenshot_path', 'verify_raw_result_path'}

    for col, value in update.items():
        if col not in allowed_cols:
            continue
        if col in df.columns and value != '':
            df.loc[mask, col] = value

    try:
        df.to_csv(LEDGER_FILE, index=False, encoding='utf-8-sig')
        print(f'  → 已更新台账: {LEDGER_FILE}')
    except Exception as e:
        print(f'  → 台账更新失败，已保留单票结果文件: {e}')


def wait_for_manual_submit(page, timeout_seconds: int = 300) -> dict:
    captured = {}

    def on_response(resp):
        if 'vatQuery' in resp.url or 'fpAction' in resp.url:
            try:
                captured['url'] = resp.url
                captured['status'] = resp.status
                captured['text'] = resp.text()
            except Exception:
                captured['error'] = '读取响应失败'

    page.on('response', on_response)
    start = time.time()
    last_notice = 0
    while time.time() - start < timeout_seconds:
        if captured.get('text'):
            page.remove_listener('response', on_response)
            return captured
        now = int(time.time() - start)
        if now // 15 > last_notice:
            last_notice = now // 15
            print(f'  等待人工在页面输入验证码并点击“查验”... 已等待 {now}s')
        page.wait_for_timeout(500)
    page.remove_listener('response', on_response)
    return captured


def finalize_result(page, task: dict, captured: dict, preserve_existing_status: bool = False) -> dict | None:
    SCREENSHOTS_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    invoice = task.get('invoice', {}) or {}
    inv_num = invoice.get('invoice_number', 'unknown')

    raw_response = captured.get('text', '')
    if raw_response:
        result = parse_jsonp(raw_response)
    else:
        body_text = page.locator('body').text_content() or ''
        result = {'key1': '', 'page_text': body_text[:2000]}

    key1 = result.get('key1', '')
    status_text = RESULT_CODE_MAP.get(key1, f'未知({key1})') if key1 else '无响应(可能页面变更)'

    # Wait for page to render result before taking screenshot
    if key1 in ('000', '001'):
        try:
            page.wait_for_timeout(2000)
            # Try to wait for result area to appear
            page.wait_for_selector('.fpInfoDiv', timeout=5000)
        except Exception:
            page.wait_for_timeout(1500)

    screenshot_path = str(SCREENSHOTS_DIR / f'{inv_num}_{ts}.png')
    try:
        page.screenshot(path=screenshot_path, full_page=True)
    except Exception:
        screenshot_path = ''

    print(f'\n  查验结果: {status_text}')
    print(f'  返回码: {key1}')
    print(f'  截图: {screenshot_path}')

    result_file = save_result(task, result, screenshot_path, raw_response)
    print(f'  结果已保存: {result_file.name}')

    with open(result_file, 'r', encoding='utf-8') as f:
        record = json.load(f)
    update_ledger(task, record, preserve_existing_status=preserve_existing_status)
    return record


def process_one(page, task: dict, task_index: int, headless: bool, max_retries: int, is_first: bool = False, no_manual_captcha: bool = False, preserve_existing_status: bool = False) -> dict | None:
    invoice = task.get('invoice', {}) or {}
    business = task.get('business', {}) or {}
    inv_num = invoice.get('invoice_number', '?')
    inv_date = invoice.get('invoice_date', '')
    total_amount = invoice.get('total_amount', '')
    seller = business.get('seller_name', '')
    check_code = invoice.get('check_code_last6_or_hint', '')

    print(f'\n{"="*60}')
    print(f'发票 #{task_index}: {inv_num}')
    print(f'  开票日期: {inv_date}')
    print(f'  价税合计: {total_amount}')
    print(f'  销方: {seller}')
    print(f'  校验码后6位: {check_code or "(无)"}')
    print(f'{"="*60}')

    # 连续处理：只在第一张发票时打开页面，后续在同一页面内刷新验证码并重新填表
    if is_first:
        page.goto(OFFICIAL_URL, wait_until='domcontentloaded', timeout=60000)
        page.wait_for_timeout(2000)
    else:
        # 后续发票：清空表单字段，刷新验证码，在同一页面内继续
        print('  → 连续处理：刷新页面准备下一张...')
        try:
            # 清空表单
            for field in ['#fphm', '#kprq', '#kjje', '#yzm']:
                page.fill(field, '')
            # 刷新验证码
            page.click('#yzm_img')
            page.wait_for_timeout(1500)
        except Exception:
            # 如果上述操作失败，则刷新页面
            page.reload(wait_until='domcontentloaded', timeout=60000)
            page.wait_for_timeout(2000)

    date_str = date_to_yyyymmdd(inv_date)
    refill_form(page, inv_num, date_str, total_amount)

    context_text = ''
    try:
        context_text = page.locator('#context').text_content() or ''
    except Exception:
        pass
    print(f'  页面提示: {context_text}')

    if not headless:
        print('  请直接在浏览器页面里输入验证码，然后手动点击“查验”。')
        captured = wait_for_manual_submit(page, timeout_seconds=300)
        if not captured.get('text'):
            print('  5 分钟内未捕获到查验响应，跳过此发票。')
            return None
        return finalize_result(page, task, captured, preserve_existing_status=preserve_existing_status)

    for attempt in range(1, max_retries + 1):
        CAPTCHA_DIR.mkdir(parents=True, exist_ok=True)
        captcha_path = CAPTCHA_DIR / f'{inv_num}_attempt{attempt}.png'
        try:
            page.locator('#yzm_img').screenshot(path=str(captcha_path))
            print(f'\n  验证码已保存: {captcha_path}')
        except Exception as e:
            print(f'  验证码截图失败: {e}')

        # Try auto-recognition first
        yzm = ''
        if _CAPTCHA_SOLVER_AVAILABLE:
            try:
                # Determine key4 from page rule text
                rule_text = ''
                try:
                    rule_text = page.locator('#yzminfo').text_content() or ''
                except Exception:
                    pass
                # Infer key4 from rule text (fallback to '00' for all_chars)
                if '红色' in rule_text:
                    auto_key4 = '01'
                elif '蓝色' in rule_text:
                    auto_key4 = '03'
                else:
                    auto_key4 = '00'

                yzm = solve_captcha_from_file(str(captcha_path), auto_key4)
                print(f'  🤖 自动识别 (key4={auto_key4}): {yzm}')
            except Exception as e:
                print(f'  🤖 自动识别失败: {e}')
                yzm = ''

        if not yzm:
            if no_manual_captcha:
                if preserve_existing_status and attempt == max_retries:
                    # 补截图模式：验证码识别失败，截当前页面作为截图记录
                    print(f'  自动识别失败，补截图模式下截取当前页面状态...')
                    SCREENSHOTS_DIR.mkdir(parents=True, exist_ok=True)
                    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
                    screenshot_path = str(SCREENSHOTS_DIR / f'{inv_num}_{ts}_captcha_failed.png')
                    try:
                        page.screenshot(path=screenshot_path, full_page=True)
                        print(f'  页面截图已保存: {screenshot_path}')
                        # 回填截图路径到台账
                        _backfill_screenshot_only(task, screenshot_path)
                    except Exception as e:
                        print(f'  页面截图也失败: {e}')
                    return None
                print(f'  自动识别失败，且已禁用人工输入（第 {attempt}/{max_retries} 次）')
            print(f'  请查看图片并输入验证码（第 {attempt}/{max_retries} 次）')
            try:
                yzm = input('  验证码 > ').strip()
            except EOFError:
                print('  无法读取人工输入，跳过此发票。')
                return None
            if not yzm:
                print('  跳过此发票。')
                return None

        captured = {}

        def on_response(resp):
            if 'vatQuery' in resp.url or 'fpAction' in resp.url:
                try:
                    captured['url'] = resp.url
                    captured['status'] = resp.status
                    captured['text'] = resp.text()
                except Exception:
                    captured['error'] = '读取响应失败'

        page.on('response', on_response)
        page.fill('#yzm', yzm)
        page.dispatch_event('#yzm', 'blur')
        page.wait_for_timeout(300)

        clicked = False
        for selector in ('#checkfp', '#uncheckfp'):
            try:
                page.click(selector, timeout=2000)
                clicked = True
                break
            except Exception:
                continue
        if not clicked:
            page.remove_listener('response', on_response)
            print('  查验按钮不可用，跳过。')
            return None

        page.wait_for_timeout(5000)
        page.remove_listener('response', on_response)

        yzm_info = ''
        try:
            yzm_info = page.locator('#yzminfo').text_content() or ''
        except Exception:
            pass

        if '验证码' in yzm_info and '错' in yzm_info:
            print('  ✗ 验证码错误，请重试')
            try:
                page.click('#yzm_img')
                page.wait_for_timeout(1000)
            except Exception:
                page.reload(wait_until='domcontentloaded', timeout=60000)
                page.wait_for_timeout(2000)
                refill_form(page, inv_num, date_str, total_amount)
            continue

        return finalize_result(page, task, captured, preserve_existing_status=preserve_existing_status)

    print(f'  达到最大重试次数 ({max_retries})，跳过此发票')
    return None


def main():
    parser = argparse.ArgumentParser(description='浏览器辅助国税查验脚本')
    parser.add_argument('--headless', action='store_true', default=True, help='无头模式（默认）')
    parser.add_argument('--headed', action='store_true', help='有头模式（需要 DISPLAY）')
    parser.add_argument('--task', type=int, default=None, help='只查验第 N 张（0-based）')
    parser.add_argument('--max-retries', type=int, default=3, help='验证码最大重试次数（默认 3）')
    parser.add_argument('--tasks-file', default=str(DEFAULT_TASKS_FILE), help='任务文件路径，默认使用 parser 生成的 ready_tasks.json')
    parser.add_argument('--no-manual-captcha', action='store_true', help='自动识别失败时不进入终端人工输入，直接返回失败')
    parser.add_argument('--preserve-existing-status', action='store_true', help='仅回填截图/原始结果路径，不覆盖原有查验状态')
    args = parser.parse_args()

    headless = not args.headed
    tasks_file = Path(args.tasks_file)
    tasks = load_tasks(tasks_file)
    if not tasks:
        print('没有待查验的任务。')
        return

    print(f'共 {len(tasks)} 张待查验发票')
    if args.task is not None:
        if 0 <= args.task < len(tasks):
            tasks = [tasks[args.task]]
            print(f'只查验第 {args.task} 张')
        else:
            print(f'无效的任务索引: {args.task}（共 {len(tasks)} 张）')
            return

    results = []
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=headless,
            args=['--ignore-certificate-errors', '--disable-web-security']
        )
        context = browser.new_context(ignore_https_errors=True)
        page = context.new_page()

        for i, task in enumerate(tasks):
            try:
                result = process_one(page, task, i, headless, args.max_retries, is_first=(i == 0), no_manual_captcha=args.no_manual_captcha, preserve_existing_status=args.preserve_existing_status)
                if result:
                    results.append(result)
                    if i < len(tasks) - 1:
                        print(f'\n  ✅ 第 {i} 张完成，准备处理下一张...')
            except KeyboardInterrupt:
                print('\n\n用户中断，退出。')
                break
            except Exception as e:
                invoice = task.get('invoice', {}) or {}
                print(f'\n  处理发票 {invoice.get("invoice_number", "?")} 时出错: {e}')
                continue

        browser.close()

    if results:
        print(f'\n{"="*60}')
        print(f'查验完成，共处理 {len(results)} 张发票:')
        passed = sum(1 for r in results if r.get('is_success'))
        failed = len(results) - passed
        print(f'  ✅ 通过: {passed}')
        print(f'  ❌ 未通过: {failed}')
        print(f'结果目录: {RESULTS_DIR}')
        print(f'台账文件: {LEDGER_FILE}')
        print(f'{"="*60}')
    else:
        print('\n未完成任何查验。')


if __name__ == '__main__':
    main()
