#!/usr/bin/env python3
"""发票验证码工作台 — 一站式：解析 → 查验任务 → 验证码 → 提交"""
from __future__ import annotations

import argparse
import base64
import csv
import json
import os
import shutil
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Optional

from fastapi import FastAPI, HTTPException, UploadFile, File
from fastapi.responses import HTMLResponse, JSONResponse, FileResponse
from pydantic import BaseModel

DEFAULT_OUTPUT_PARENT = Path(os.environ.get('INVOICE_OUTPUT_PARENT', str(Path.home() / 'invoices_output')))


def _load_default_dirs() -> tuple[Path, Path]:
    """Load default input/output dirs, preferring env vars, falling back to ~/invoices."""
    inp = Path(os.environ.get('INVOICE_INPUT_DIR', str(Path.home() / 'invoices')))
    out = Path(os.environ.get('INVOICE_OUTPUT_DIR', str(DEFAULT_OUTPUT_PARENT)))
    return inp, out


DEFAULT_INPUT_DIR, DEFAULT_OUTPUT_DIR = _load_default_dirs()

BASE_DIR = Path(__file__).resolve().parent
WORKSPACE_DIR = BASE_DIR.parent
VERIFIER_DIR = WORKSPACE_DIR / 'invoice-verifier'
if str(VERIFIER_DIR) not in sys.path:
    sys.path.insert(0, str(VERIFIER_DIR))

from verify import run_verifier as rv
from verify.verify_contract import build_standard_result

try:
    from ocr_client import OCRServiceClient  # type: ignore
except Exception:
    OCRServiceClient = None

try:
    from verify.captcha_solver import solve_captcha  # type: ignore
except Exception:
    solve_captcha = None

DEFAULT_INPUT_DIR = _load_default_dirs()[0]
DEFAULT_OUTPUT_DIR = _load_default_dirs()[1]
INPUT_DIR = DEFAULT_INPUT_DIR
OUTPUT_DIR = DEFAULT_OUTPUT_DIR
UPLOAD_DIR = OUTPUT_DIR / 'uploads'
VERIFY_TASKS_JSON = OUTPUT_DIR / 'verify_tasks' / 'ready_tasks.json'
RESULTS_DIR = OUTPUT_DIR / 'verify_results'
CAPTCHA_DIR = RESULTS_DIR / 'captcha'
SESSION_DIR = RESULTS_DIR / 'captcha_sessions'
BUILD_VERIFY_ASSETS = BASE_DIR / 'build_verify_assets.py'
APPLY_VERIFY_RESULTS = BASE_DIR / 'apply_verify_results.py'
PARSER_SCRIPT = BASE_DIR / 'parser.py'

app = FastAPI(title='invoice-captcha-workbench', version='0.2.0')

BULK_STATE: dict[str, Any] = {
    'running': False,
    'stop_requested': False,
    'started_at': '',
    'finished_at': '',
    'total': 0,
    'done': 0,
    'success': 0,
    'failed': 0,
    'retrying': 0,
    'current_task_id': '',
    'current_invoice_number': '',
    'current_attempt': 0,
    'logs': [],
}
BULK_LOCK = threading.Lock()


# ── Pydantic models ──────────────────────────────────────────────

class TaskAction(BaseModel):
    task_id: str


class SubmitCaptchaRequest(BaseModel):
    task_id: str
    captcha_text: str


class SelectOutputDirRequest(BaseModel):
    output_dir: str


class ParseInputDirRequest(BaseModel):
    input_dir: str
    output_dir: str


class DeleteInvoiceRequest(BaseModel):
    file_hash: str
    delete_source_file: bool = False


class ResetInvoiceRequest(BaseModel):
    file_hash: str


class CaptureScreenshotRequest(BaseModel):
    file_hash: str


class AutoVerifyRequest(BaseModel):
    file_hash: str


# ── Global helpers ───────────────────────────────────────────────

def ensure_dirs() -> None:
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    CAPTCHA_DIR.mkdir(parents=True, exist_ok=True)
    SESSION_DIR.mkdir(parents=True, exist_ok=True)


def set_output_dir(path: str | Path) -> None:
    global OUTPUT_DIR, VERIFY_TASKS_JSON, RESULTS_DIR, CAPTCHA_DIR, SESSION_DIR, UPLOAD_DIR
    OUTPUT_DIR = Path(path)
    VERIFY_TASKS_JSON = OUTPUT_DIR / 'verify_tasks' / 'ready_tasks.json'
    RESULTS_DIR = OUTPUT_DIR / 'verify_results'
    CAPTCHA_DIR = RESULTS_DIR / 'captcha'
    SESSION_DIR = RESULTS_DIR / 'captcha_sessions'
    UPLOAD_DIR = OUTPUT_DIR / 'uploads'
    ensure_dirs()


def _current_output_parent(path: Path | None = None) -> Path:
    target = path or OUTPUT_DIR
    return target.parent if target.name.startswith('发票_解析结果') else target


def list_output_dirs() -> list[dict[str, Any]]:
    dirs: list[dict[str, Any]] = []
    seen: set[str] = set()
    parent = _current_output_parent()

    def _append_dir(p: Path) -> None:
        key = str(p.resolve()) if p.exists() else str(p)
        if key in seen:
            return
        tasks_file = p / 'verify_tasks' / 'ready_tasks.json'
        count = 0
        if tasks_file.exists():
            try:
                count = len(json.loads(tasks_file.read_text(encoding='utf-8')))
            except Exception:
                count = -1
        has_parsed = (p / '发票解析结果.json').exists()
        dirs.append({
            'path': str(p),
            'name': p.name or str(p),
            'task_count': count,
            'has_parsed': has_parsed,
            'selected': p.resolve() == OUTPUT_DIR.resolve() if p.exists() else False,
        })
        seen.add(key)

    if parent.exists():
        for p in sorted(parent.iterdir()):
            if not p.is_dir():
                continue
            if not p.name.startswith('发票_解析结果'):
                continue
            _append_dir(p)

    if OUTPUT_DIR.exists():
        _append_dir(OUTPUT_DIR)

    return dirs


def load_tasks() -> list[dict[str, Any]]:
    if not VERIFY_TASKS_JSON.exists():
        return []
    return json.loads(VERIFY_TASKS_JSON.read_text(encoding='utf-8'))


def find_task(task_id: str) -> dict[str, Any]:
    for task in load_tasks():
        if task.get('task_id') == task_id:
            return task
    raise HTTPException(status_code=404, detail=f'task not found: {task_id}')


def save_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')


def build_assets_for(output_dir: Path) -> None:
    subprocess.run([sys.executable, str(BUILD_VERIFY_ASSETS), '--output-dir', str(output_dir)], cwd=str(BASE_DIR), check=True)


def build_assets() -> None:
    build_assets_for(OUTPUT_DIR)


def apply_results_for(output_dir: Path) -> None:
    subprocess.run([sys.executable, str(APPLY_VERIFY_RESULTS), '--output-dir', str(output_dir)], cwd=str(BASE_DIR), check=True)


def apply_results() -> None:
    apply_results_for(OUTPUT_DIR)


def find_invoice_by_hash(file_hash: str) -> dict[str, Any]:
    for inv in load_parsed_invoices():
        if inv.get('file_hash') == file_hash:
            return inv
    raise HTTPException(status_code=404, detail=f'invoice not found: {file_hash}')


def find_task_for_invoice(inv: dict[str, Any]) -> dict[str, Any] | None:
    inv_num = inv.get('invoice_number', '')
    inv_date = inv.get('invoice_date', '')
    amt = inv.get('total_amount', '')
    for task in load_tasks():
        t_inv = task.get('invoice', {}) or {}
        if (
            (t_inv.get('invoice_number', '') == inv_num) and
            (t_inv.get('invoice_date', '') == inv_date) and
            (t_inv.get('total_amount', '') == amt)
        ):
            return task
    return None


def build_ephemeral_task_from_invoice(inv: dict[str, Any]) -> dict[str, Any]:
    from verify.verify_contract import build_verify_request
    return build_verify_request({
        'verify_task_id': inv.get('verify_task_id', ''),
        'file_hash': inv.get('file_hash', ''),
        'duplicate_key': inv.get('duplicate_key', ''),
        'invoice_code': inv.get('invoice_code', ''),
        'invoice_number': inv.get('invoice_number', ''),
        'invoice_date': inv.get('invoice_date', ''),
        'invoice_type': inv.get('invoice_type', ''),
        'total_amount': inv.get('total_amount', ''),
        'check_code_last6_or_hint': inv.get('verify_check_code_last6_or_last6_hint', ''),
        'file_name': inv.get('file_name', ''),
        'file_path': inv.get('file_path', ''),
        'seller_name': inv.get('seller_name', ''),
        'seller_tax_no': inv.get('seller_tax_no', ''),
        'buyer_name': inv.get('buyer_name', ''),
        'buyer_tax_no': inv.get('buyer_tax_no', ''),
        'expense_category': inv.get('expense_category', ''),
    })


def capture_verify_screenshot_for_invoice(file_hash: str) -> dict[str, Any]:
    inv = find_invoice_by_hash(file_hash)
    verify_status = inv.get('verify_status', '')
    if verify_status != '查验通过':
        raise HTTPException(status_code=400, detail=f'仅支持对已查验通过的发票补截图，当前状态: {verify_status or "未查验"}')

    task = find_task_for_invoice(inv) or build_ephemeral_task_from_invoice(inv)
    runtime_dir = OUTPUT_DIR
    temp_tasks_file = RESULTS_DIR / 'screenshot_task.json'
    save_json(temp_tasks_file, [task])

    cmd = [
        sys.executable,
        str(BASE_DIR / 'verify_browser_assist.py'),
        '--tasks-file', str(temp_tasks_file),
        '--max-retries', '3',
        '--no-manual-captcha',
        '--preserve-existing-status',
    ]
    env = os.environ.copy()
    env['VERIFY_RUNTIME_DIR'] = str(runtime_dir)
    env.setdefault('DISPLAY', ':0')
    env.setdefault('XAUTHORITY', '/home/li/.Xauthority')

    try:
        proc = subprocess.run(cmd, cwd=str(BASE_DIR), env=env, capture_output=True, text=True, timeout=120)
        stdout = proc.stdout
        stderr = proc.stderr
        returncode = proc.returncode
    except subprocess.TimeoutExpired as exc:
        stdout = exc.stdout or ''
        stderr = (exc.stderr or '') + '\n[timeout] verify_browser_assist exceeded 120s'
        returncode = 124
    except Exception as exc:
        stdout = ''
        stderr = f'[spawn-error] {exc}'
        returncode = 500

    try:
        apply_results()
    except Exception:
        pass

    invoices = load_parsed_invoices()
    refreshed = next((x for x in invoices if x.get('file_hash') == file_hash), inv)
    screenshot_path = refreshed.get('verify_screenshot_path', '')
    ok = bool(screenshot_path)
    return {
        'ok': ok,
        'file_hash': file_hash,
        'file_name': inv.get('file_name', ''),
        'verify_status': refreshed.get('verify_status', verify_status),
        'verify_screenshot_path': screenshot_path,
        'stdout': str(stdout)[-4000:],
        'stderr': str(stderr)[-2000:],
        'returncode': returncode,
    }


def make_ocr_client() -> Optional[Any]:
    if OCRServiceClient is None:
        return None
    base_url = os.environ.get('OCR_BASE_URL', os.environ.get('OCR_BASE_URL', 'http://localhost:17861'))
    token = os.environ.get('OCR_API_TOKEN')
    try:
        return OCRServiceClient(base_url=base_url, token=token, timeout=20)
    except Exception:
        return None


# ── OCR / captcha solver ─────────────────────────────────────────

def suggest_captcha_text(image_path: Path, image_b64: str = '', key4: str = '') -> dict[str, Any]:
    result: dict[str, Any] = {
        'enabled': False,
        'providers': {},
    }

    # 1) ddddocr suggestion (local, rule-aware)
    if solve_captcha is not None and image_b64:
        try:
            dddd_text = solve_captcha(image_b64, str(key4 or ''))
            result['providers']['ddddocr'] = {
                'enabled': True,
                'text': dddd_text,
            }
            result['enabled'] = True
        except Exception as exc:
            result['providers']['ddddocr'] = {
                'enabled': False,
                'reason': str(exc),
            }

    # 2) legacy OCR service suggestion (if available)
    client = make_ocr_client()
    if client is not None:
        try:
            rec = client.ocr_rec_char(image_path)
            char = client.ocr_char(image_path)
            result['providers']['ocr_service'] = {
                'enabled': True,
                'rec': rec,
                'char': char,
            }
            result['enabled'] = True
        except Exception as exc:
            result['providers']['ocr_service'] = {
                'enabled': False,
                'reason': str(exc),
            }

    if not result['enabled']:
        result['reason'] = 'no OCR provider available'
    return result


# ── Task context / fetch captcha / submit captcha ────────────────

def _prepare_task_context(task: dict[str, Any]) -> dict[str, Any]:
    invoice = task.get('invoice', {}) or {}
    inv_num = invoice.get('invoice_number', '')
    inv_date = invoice.get('invoice_date', '')
    total_amount = invoice.get('total_amount', '')
    fpdm = invoice.get('invoice_code', '') or (inv_num[:12] if len(inv_num) >= 20 else inv_num[:10])
    fphm = inv_num[-8:] if len(inv_num) >= 8 else inv_num
    kprq = inv_date.replace('年', '').replace('月', '').replace('日', '')
    key4_value = rv.normalize_money_for_key4(total_amount)
    swjg = rv.infer_swjg_from_invoice_number(inv_num) or rv.get_swjg(fpdm)
    if not swjg:
        raise HTTPException(status_code=400, detail='failed to infer swjg')
    return {
        'invoice': invoice,
        'inv_num': inv_num,
        'inv_date': inv_date,
        'total_amount': total_amount,
        'fpdm': fpdm,
        'fphm': fphm,
        'kprq': kprq,
        'key4_value': key4_value,
        'swjg': swjg,
    }


def fetch_captcha_for_task(task: dict[str, Any]) -> dict[str, Any]:
    ctx = _prepare_task_context(task)
    yzm_ts = rv.now_ms()
    yzm_payload = {
        'callback': f'jQuery{yzm_ts}',
        'fpdm': ctx['fpdm'],
        'fphm': ctx['fphm'],
        'r': '0.5',
        'v': '2.0.23_090',
        'nowtime': yzm_ts,
        'publickey': yzm_ts,
        'key9': rv.run_js_key9(ctx['fpdm'], ctx['fphm'], yzm_ts, '')['yzm'],
        '_': str(max(int(yzm_ts) - 1, 0)),
    }
    yzm_url = f"{ctx['swjg']['url']}/yzmQuery"
    signed_url, yzm_data = rv.post_jsonp_via_signed_url(yzm_url, yzm_payload)
    image_b64 = str(yzm_data.get('key1', ''))
    if not image_b64:
        raise HTTPException(status_code=502, detail='yzmQuery returned no image')

    ts = time.strftime('%Y%m%d_%H%M%S')
    image_path = CAPTCHA_DIR / f"{task['task_id']}_{ts}.png"
    image_path.write_bytes(base64.b64decode(image_b64))

    session_payload = {
        'task_id': task['task_id'],
        'fetched_at': time.strftime('%Y-%m-%dT%H:%M:%S'),
        'swjg': ctx['swjg'],
        'fpdm': ctx['fpdm'],
        'fphm': ctx['fphm'],
        'kprq': ctx['kprq'],
        'key4_value': ctx['key4_value'],
        'invoice_type': ctx['invoice'].get('invoice_type', ''),
        'inv_num': ctx['inv_num'],
        'yzm_payload': yzm_payload,
        'yzm_signed_url': signed_url,
        'yzm_response': yzm_data,
        'captcha_image_path': str(image_path),
    }
    session_path = SESSION_DIR / f"{task['task_id']}.json"
    save_json(session_path, session_payload)

    rule_key4 = str(yzm_data.get('key4', ''))
    ocr = suggest_captcha_text(image_path, image_b64=image_b64, key4=rule_key4)
    return {
        'task_id': task['task_id'],
        'captcha_image_path': str(image_path),
        'captcha_image_data_url': 'data:image/png;base64,' + image_b64,
        'rule_key4': rule_key4,
        'rule_key5': yzm_data.get('key5', ''),
        'rule_key6': yzm_data.get('key6', ''),
        'ocr': ocr,
    }


def submit_captcha_for_task(task: dict[str, Any], captcha_text: str) -> dict[str, Any]:
    session_path = SESSION_DIR / f"{task['task_id']}.json"
    if not session_path.exists():
        raise HTTPException(status_code=400, detail='captcha session missing, fetch captcha first')
    session = json.loads(session_path.read_text(encoding='utf-8'))
    text = (captcha_text or '').strip().upper()
    if not text:
        raise HTTPException(status_code=400, detail='captcha text empty')

    invoice = task.get('invoice', {}) or {}
    vat_publickey = time.strftime('%Y-%m-%d %H:%M:%S')
    vat_ts = rv.now_ms()
    vat_payload = {
        'callback': f'jQuery{vat_ts}',
        'key1': session['fpdm'],
        'key2': session['fphm'],
        'key3': session['kprq'],
        'key4': session['key4_value'],
        'fplx': rv.infer_fplx_from_invoice_type(invoice.get('invoice_type', ''), rv.get_invoice_type(session['fpdm']), session['inv_num']),
        'yzm': text,
        'yzmSj': vat_publickey,
        'index': session['yzm_response'].get('key3', ''),
        'key6': session['yzm_response'].get('key6', ''),
        'publickey': vat_publickey,
        'key9': rv.run_js_key9(session['fpdm'], session['fphm'], vat_publickey, vat_publickey)['cy'],
        '_': str(max(int(vat_ts) - 1, 0)),
    }
    vat_url = f"{session['swjg']['url']}/vatQuery"
    signed_url, vat_data = rv.post_jsonp_via_signed_url(vat_url, vat_payload, timeout=30)
    key1 = str(vat_data.get('key1', ''))
    status_text = rv.RESULT_CODE_MAP.get(key1, f'未知({key1})') if key1 else '无响应'

    raw_path = RESULTS_DIR / f"{task['task_id']}_{time.strftime('%Y%m%d_%H%M%S')}_captcha_workbench_raw.json"
    save_json(raw_path, {
        'captcha_session': session,
        'vat_request': {'url': signed_url, 'base_url': vat_url, 'payload': vat_payload},
        'vat_response': vat_data,
    })

    if key1 == '008':
        return {
            'ok': False,
            'captcha_error': True,
            'verify_result_code': key1,
            'verify_result_summary': status_text,
            'raw_result_path': str(raw_path),
        }

    result_payload = build_standard_result(
        task,
        verify_time=time.strftime('%Y-%m-%dT%H:%M:%S'),
        verify_channel='invoice-verifier(captcha_workbench)',
        result_code=key1,
        result_status=status_text,
        result_summary=status_text,
        is_success=key1 in ('000', '001'),
        is_voided='未知',
        is_abnormal='否' if key1 in ('000', '001') else '是',
        screenshot_path='',
        raw_response_path=str(raw_path),
        raw_response=json.dumps(vat_data, ensure_ascii=False)[:5000],
        full_result={'captcha_session': session, 'vat_response': vat_data},
        source_kind='captcha_workbench',
    )
    result_file = RESULTS_DIR / f"{task['task_id']}_{time.strftime('%Y%m%d_%H%M%S')}.json"
    save_json(result_file, result_payload)
    apply_results()
    build_assets()
    return {
        'ok': True,
        'captcha_error': False,
        'verify_result_code': key1,
        'verify_result_summary': status_text,
        'result_file': str(result_file),
    }


# ── Summarize helpers ────────────────────────────────────────────

def summarize_tasks(tasks: list[dict[str, Any]]) -> dict[str, Any]:
    items = []
    for task in tasks:
        invoice = task.get('invoice', {}) or {}
        business = task.get('business', {}) or {}
        key = str(invoice.get('invoice_number', ''))
        if len(key) >= 4:
            color_hint = key[-2:]
        else:
            color_hint = ''
        items.append({
            'task_id': task.get('task_id', ''),
            'invoice_number': invoice.get('invoice_number', ''),
            'invoice_date': invoice.get('invoice_date', ''),
            'invoice_type': invoice.get('invoice_type', ''),
            'total_amount': invoice.get('total_amount', ''),
            'seller_name': business.get('seller_name', ''),
            'expense_category': business.get('expense_category', ''),
            'file_name': business.get('file_name', ''),
            'file_path': business.get('file_path', ''),
            'color_hint': color_hint,
        })
    return {'count': len(items), 'tasks': items}


# ── Parsed invoices API ──────────────────────────────────────────

def load_parsed_invoices(output_dir: Path | None = None) -> list[dict[str, Any]]:
    """Load from output dir and merge latest verify state from ledger."""
    output_dir = output_dir or OUTPUT_DIR
    result_file = output_dir / '发票解析结果.json'
    if not result_file.exists():
        return []
    data = json.loads(result_file.read_text(encoding='utf-8'))
    if isinstance(data, list):
        invoices = data
    elif isinstance(data, dict):
        invoices = data.get('invoices', data.get('records', data.get('data', [])))
    else:
        invoices = []

    ledger = output_dir / '查验状态台账.csv'
    state_by_hash: dict[str, dict[str, str]] = {}
    state_by_dup: dict[str, dict[str, str]] = {}
    if ledger.exists():
        try:
            with ledger.open('r', encoding='utf-8-sig', newline='') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if row.get('file_hash'):
                        state_by_hash[row['file_hash']] = row
                    if row.get('duplicate_key'):
                        state_by_dup[row['duplicate_key']] = row
        except Exception:
            pass

    merged = []
    for inv in invoices:
        row = dict(inv)
        state = state_by_hash.get(row.get('file_hash', '')) or state_by_dup.get(row.get('duplicate_key', '')) or {}
        for k in ['verify_required_fields_status', 'verify_missing_fields', 'verify_status', 'verify_time', 'verify_channel', 'verify_result_summary', 'is_voided', 'is_abnormal', 'verify_screenshot_path', 'verify_raw_result_path']:
            if state.get(k):
                row[k] = state.get(k, row.get(k, ''))
        merged.append(row)
    return merged


def enrich_invoice_sources(invoices: list[dict[str, Any]], output_dir: Path | None = None) -> list[dict[str, Any]]:
    output_dir = output_dir or OUTPUT_DIR
    upload_dir = output_dir / 'uploads'
    uploaded_names = {p.name for p in upload_dir.iterdir() if p.is_file()} if upload_dir.exists() else set()
    enriched = []
    for inv in invoices:
        row = dict(inv)
        file_name = row.get('file_name', '')
        file_path = row.get('file_path', '')
        source_kind = 'uploaded' if file_name in uploaded_names or '/uploads/' in file_path else 'parsed-from-dir'
        row['source_kind'] = source_kind
        enriched.append(row)
    return enriched


def invoice_tabs_payload(invoices: list[dict[str, Any]]) -> dict[str, int]:
    total = len(invoices)
    pending = 0
    partial = 0
    passed = 0
    failed = 0
    for inv in invoices:
        verify_required = inv.get('verify_required_fields_status', '')
        verify_status = inv.get('verify_status', '') or '未查验'
        if verify_status == '查验通过':
            passed += 1
        elif verify_status in ('查验失败', '异常', '需人工复核'):
            failed += 1
        elif verify_required == 'ready':
            pending += 1
        else:
            partial += 1
    return {
        'all': total,
        'pending': pending,
        'partial': partial,
        'passed': passed,
        'failed': failed,
    }


def rebuild_parser_outputs(records: list[dict[str, Any]], output_dir: Path | None = None) -> None:
    import parser as parser_mod
    output_dir = output_dir or OUTPUT_DIR
    parser_mod.set_parser_output_dir(output_dir)
    items_cls = parser_mod.InvoiceItem
    record_cls = parser_mod.InvoiceRecord
    rebuilt = []
    for row in records:
        payload = dict(row)
        payload['items'] = [items_cls(**item) for item in payload.get('items', [])]
        rebuilt.append(record_cls(**payload))
    parser_mod.build_outputs(rebuilt)


def _resolve_output_dir_for_record(rec: dict[str, Any], output_parent: Path | None = None) -> Path:
    import parser as parser_mod
    output_parent = output_parent or DEFAULT_OUTPUT_PARENT
    return parser_mod.resolve_output_dir_for_record(rec, output_parent=output_parent)


def _move_uploaded_source_to_bucket(src_path: Path, target_output_dir: Path) -> Path:
    target_upload_dir = target_output_dir / 'uploads'
    target_upload_dir.mkdir(parents=True, exist_ok=True)
    dest = target_upload_dir / src_path.name
    if dest.exists() and dest.resolve() != src_path.resolve():
        dest = target_upload_dir / f"{src_path.stem}_{uuid.uuid4().hex[:6]}{src_path.suffix}"
    if src_path.resolve() != dest.resolve():
        shutil.move(str(src_path), str(dest))
    return dest


def merge_records_into_entity_dirs(new_records: list[dict[str, Any]], *, move_uploaded_sources: bool = False, output_parent: Path | None = None) -> list[dict[str, Any]]:
    touched: dict[Path, list[dict[str, Any]]] = {}
    for rec in new_records:
        target_dir = _resolve_output_dir_for_record(rec, output_parent=output_parent)
        row = dict(rec)
        if move_uploaded_sources:
            src = Path(row.get('file_path', ''))
            if src.exists():
                dest = _move_uploaded_source_to_bucket(src, target_dir)
                row['file_path'] = str(dest)
                row['file_name'] = dest.name
        touched.setdefault(target_dir, []).append(row)

    summaries: list[dict[str, Any]] = []
    for target_dir, rows in touched.items():
        existing_records = load_parsed_invoices(target_dir)
        existing_hashes = {r.get('file_hash', '') for r in existing_records}
        merged = list(existing_records)
        added = 0
        for row in rows:
            if row.get('file_hash', '') in existing_hashes:
                continue
            merged.append(row)
            added += 1
        rebuild_parser_outputs(merged, target_dir)
        build_assets_for(target_dir)
        summaries.append({'output_dir': str(target_dir), 'added': added, 'total': len(merged)})
    return summaries


def bulk_log(message: str) -> None:
    with BULK_LOCK:
        BULK_STATE['logs'].append(f"[{time.strftime('%H:%M:%S')}] {message}")
        BULK_STATE['logs'] = BULK_STATE['logs'][-80:]


def bulk_snapshot() -> dict[str, Any]:
    with BULK_LOCK:
        return dict(BULK_STATE)


def run_bulk_verify_worker(tasks_snapshot: list[dict[str, Any]]) -> None:
    with BULK_LOCK:
        BULK_STATE.update({
            'running': True,
            'stop_requested': False,
            'started_at': time.strftime('%Y-%m-%dT%H:%M:%S'),
            'finished_at': '',
            'total': len(tasks_snapshot),
            'done': 0,
            'success': 0,
            'failed': 0,
            'retrying': 0,
            'current_task_id': '',
            'current_invoice_number': '',
            'current_attempt': 0,
            'logs': [],
        })
    bulk_log(f'批量查验开始，共 {len(tasks_snapshot)} 张')

    try:
        for task in tasks_snapshot:
            with BULK_LOCK:
                if BULK_STATE.get('stop_requested'):
                    bulk_log('收到停止请求，批量任务结束')
                    break
                BULK_STATE['current_task_id'] = task.get('task_id', '')
                BULK_STATE['current_invoice_number'] = (task.get('invoice', {}) or {}).get('invoice_number', '')
                BULK_STATE['current_attempt'] = 0

            max_attempts = 5
            success = False
            terminal_recorded = False
            for attempt in range(1, max_attempts + 1):
                with BULK_LOCK:
                    if BULK_STATE.get('stop_requested'):
                        break
                    BULK_STATE['current_attempt'] = attempt
                inv_num = (task.get('invoice', {}) or {}).get('invoice_number', '')
                bulk_log(f'{inv_num} 开始第 {attempt}/{max_attempts} 次尝试')

                try:
                    fetched = fetch_captcha_for_task(task)
                    ocr = fetched.get('ocr', {}) or {}
                    providers = ocr.get('providers', {}) or {}
                    text = ''
                    if providers.get('ddddocr', {}).get('enabled') and providers.get('ddddocr', {}).get('text'):
                        text = providers['ddddocr']['text']
                    if not text:
                        bulk_log(f'{inv_num} OCR 未拿到候选，记为失败/复核')
                        with BULK_LOCK:
                            BULK_STATE['failed'] += 1
                            BULK_STATE['done'] += 1
                        terminal_recorded = True
                        break
                    bulk_log(f'{inv_num} OCR={text}')
                    result = submit_captcha_for_task(task, text)
                    code = result.get('verify_result_code', '')
                    summary = result.get('verify_result_summary', '')
                    if result.get('ok'):
                        bulk_log(f'{inv_num} 成功：{code} {summary}')
                        with BULK_LOCK:
                            BULK_STATE['success'] += 1
                            BULK_STATE['done'] += 1
                        success = True
                        break
                    if code == '008':
                        with BULK_LOCK:
                            BULK_STATE['retrying'] += 1
                        bulk_log(f'{inv_num} 验证码错误，重试 {attempt}/{max_attempts}')
                        time.sleep(min(0.3 * attempt, 1.0))
                        continue
                    if code == '007':
                        bulk_log(f'{inv_num} 官方限制：{summary}，不重试')
                        with BULK_LOCK:
                            BULK_STATE['failed'] += 1
                            BULK_STATE['done'] += 1
                        terminal_recorded = True
                        break
                    bulk_log(f'{inv_num} 失败：{code} {summary}')
                    with BULK_LOCK:
                        BULK_STATE['failed'] += 1
                        BULK_STATE['done'] += 1
                    terminal_recorded = True
                    break
                except Exception as exc:
                    if attempt < 2:
                        bulk_log(f'{inv_num} 临时异常：{exc}，再试一次')
                        time.sleep(0.5)
                        continue
                    bulk_log(f'{inv_num} 异常失败：{exc}')
                    with BULK_LOCK:
                        BULK_STATE['failed'] += 1
                        BULK_STATE['done'] += 1
                    terminal_recorded = True
                    break

            if not success and not terminal_recorded and bulk_snapshot().get('current_attempt', 0) >= max_attempts:
                inv_num = (task.get('invoice', {}) or {}).get('invoice_number', '')
                bulk_log(f'{inv_num} 连续 {max_attempts} 次验证码失败，记为失败/复核')
                with BULK_LOCK:
                    BULK_STATE['failed'] += 1
                    BULK_STATE['done'] += 1
    finally:
        with BULK_LOCK:
            BULK_STATE['running'] = False
            BULK_STATE['finished_at'] = time.strftime('%Y-%m-%dT%H:%M:%S')
            BULK_STATE['current_task_id'] = ''
            BULK_STATE['current_invoice_number'] = ''
            BULK_STATE['current_attempt'] = 0
        bulk_log('批量查验结束')


# ── API endpoints ────────────────────────────────────────────────

@app.get('/api/tasks')
def api_tasks():
    tasks = load_tasks()
    invoices = enrich_invoice_sources(load_parsed_invoices())
    return {
        **summarize_tasks(tasks),
        'output_dir': str(OUTPUT_DIR),
        'output_dirs': list_output_dirs(),
        'input_dir': str(INPUT_DIR),
        'tabs': invoice_tabs_payload(invoices),
    }


@app.post('/api/select_output_dir')
def api_select_output_dir(body: SelectOutputDirRequest):
    output_dir = (body.output_dir or '').strip()
    if not output_dir:
        raise HTTPException(status_code=400, detail='output_dir is required')
    target = Path(output_dir)
    try:
        target.mkdir(parents=True, exist_ok=True)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f'failed to create output_dir: {exc}')
    if not target.is_dir():
        raise HTTPException(status_code=400, detail='output_dir is not a directory')
    set_output_dir(target)
    tasks = load_tasks()
    return {
        'ok': True,
        'output_dir': str(OUTPUT_DIR),
        **summarize_tasks(tasks),
        'output_dirs': list_output_dirs(),
    }


@app.post('/api/parse_input_dir')
def api_parse_input_dir(body: ParseInputDirRequest):
    global INPUT_DIR, OUTPUT_DIR
    input_dir = Path(body.input_dir)
    output_dir = Path(body.output_dir)
    if not input_dir.exists() or not input_dir.is_dir():
        raise HTTPException(status_code=400, detail=f'input_dir not found: {input_dir}')

    output_dir.mkdir(parents=True, exist_ok=True)
    INPUT_DIR = input_dir
    output_parent = output_dir.parent if output_dir.name.startswith('发票_解析结果') else output_dir
    set_output_dir(output_dir)

    from parser import parse_invoice

    existing_hashes: set[str] = set()
    for item in list_output_dirs():
        p = Path(item['path'])
        for inv in load_parsed_invoices(p):
            fh = inv.get('file_hash', '')
            if fh:
                existing_hashes.add(fh)

    source_paths = sorted([p for p in input_dir.iterdir() if p.is_file() and p.suffix.lower() in {'.pdf', '.xml', '.ofd'}])
    new_records: list[dict[str, Any]] = []
    skipped: list[str] = []
    for p in source_paths:
        rec = parse_invoice(p)
        rec_dict = {k: v for k, v in rec.__dict__.items() if not k.startswith('_')}
        rec_dict['items'] = [item.__dict__ for item in rec.items]
        if rec_dict.get('file_hash', '') in existing_hashes:
            skipped.append(f'{p.name} (重复)')
            continue
        new_records.append(rec_dict)

    try:
        routed = merge_records_into_entity_dirs(new_records, move_uploaded_sources=False, output_parent=output_parent)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f'entity routing failed: {exc}')

    if routed:
        set_output_dir(Path(routed[0]['output_dir']))

    invoices = enrich_invoice_sources(load_parsed_invoices())
    tasks = load_tasks()
    return {
        'ok': True,
        'new_invoices': sum(x['added'] for x in routed),
        'skipped': skipped,
        'invoice_count': len(invoices),
        'task_count': len(tasks),
        'output_dir': str(OUTPUT_DIR),
        'routed_outputs': routed,
        'invoices': invoices,
        'output_dirs': list_output_dirs(),
    }


@app.get('/api/parsed_invoices')
def api_parsed_invoices():
    invoices = enrich_invoice_sources(load_parsed_invoices())
    return {
        'count': len(invoices),
        'output_dir': str(OUTPUT_DIR),
        'invoices': invoices,
        'tabs': invoice_tabs_payload(invoices),
    }


@app.get('/api/bulk_status')
def api_bulk_status():
    return bulk_snapshot()


@app.post('/api/bulk_start')
def api_bulk_start():
    with BULK_LOCK:
        if BULK_STATE.get('running'):
            raise HTTPException(status_code=400, detail='bulk verify already running')
    tasks_snapshot = load_tasks()
    if not tasks_snapshot:
        raise HTTPException(status_code=400, detail='no pending tasks to process')
    t = threading.Thread(target=run_bulk_verify_worker, args=(tasks_snapshot,), daemon=True)
    t.start()
    return {'ok': True, 'queued': len(tasks_snapshot), 'status': bulk_snapshot()}


@app.post('/api/bulk_stop')
def api_bulk_stop():
    with BULK_LOCK:
        BULK_STATE['stop_requested'] = True
    bulk_log('用户请求停止批量查验')
    return {'ok': True, 'status': bulk_snapshot()}


@app.post('/api/upload_pdfs')
async def api_upload_pdfs(files: list[UploadFile] = File(...)):
    if not files:
        raise HTTPException(status_code=400, detail='no files provided')

    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

    saved_paths: list[str] = []
    skipped: list[str] = []
    for f in files:
        if not f.filename:
            continue
        name = f.filename
        if not any(name.lower().endswith(ext) for ext in ('.pdf', '.xml', '.ofd')):
            skipped.append(f'{name} (非PDF/XML/OFD)')
            continue
        basename = Path(name).name
        dest = UPLOAD_DIR / basename
        if dest.exists():
            stem = dest.stem
            suffix = dest.suffix
            dest = UPLOAD_DIR / f"{stem}_{uuid.uuid4().hex[:6]}{suffix}"
        content = await f.read()
        dest.write_bytes(content)
        saved_paths.append(str(dest))

    if not saved_paths:
        raise HTTPException(status_code=400, detail=f'no supported files uploaded, skipped: {skipped}')

    from parser import parse_invoice

    existing_hashes: set[str] = set()
    for item in list_output_dirs():
        p = Path(item['path'])
        for inv in load_parsed_invoices(p):
            fh = inv.get('file_hash', '')
            if fh:
                existing_hashes.add(fh)

    new_records = []
    for p_str in saved_paths:
        p = Path(p_str)
        rec = parse_invoice(p)
        rec_dict = {k: v for k, v in rec.__dict__.items() if not k.startswith('_')}
        rec_dict['items'] = [item.__dict__ for item in rec.items]
        if rec_dict.get('file_hash', '') in existing_hashes:
            skipped.append(f'{p.name} (重复)')
            try:
                p.unlink(missing_ok=True)
            except Exception:
                pass
            continue
        new_records.append(rec_dict)

    if not new_records:
        invoices = load_parsed_invoices()
        tasks = load_tasks()
        return {
            'ok': True,
            'uploaded': len(saved_paths),
            'new_invoices': 0,
            'skipped': skipped,
            'invoice_count': len(invoices),
            'task_count': len(tasks),
            'output_dir': str(OUTPUT_DIR),
            'routed_outputs': [],
        }

    try:
        routed = merge_records_into_entity_dirs(new_records, move_uploaded_sources=True, output_parent=OUTPUT_DIR.parent)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f'entity routing failed: {exc}')

    if routed:
        set_output_dir(Path(routed[0]['output_dir']))

    invoices = enrich_invoice_sources(load_parsed_invoices())
    tasks = load_tasks()
    return {
        'ok': True,
        'uploaded': len(saved_paths),
        'new_invoices': sum(x['added'] for x in routed),
        'skipped': skipped,
        'invoice_count': len(invoices),
        'task_count': len(tasks),
        'output_dir': str(OUTPUT_DIR),
        'routed_outputs': routed,
    }


@app.post('/api/capture_verify_screenshot')
def api_capture_verify_screenshot(body: CaptureScreenshotRequest):
    return capture_verify_screenshot_for_invoice(body.file_hash)


@app.post('/api/auto_verify_screenshot')
def api_auto_verify_screenshot(body: AutoVerifyRequest):
    inv = find_invoice_by_hash(body.file_hash)
    if not inv:
        raise HTTPException(status_code=404, detail='invoice not found')
    verify_status = inv.get('verify_status', '')
    if verify_status not in ('未查验', '待查验', ''):
        raise HTTPException(status_code=400, detail='only pending: ' + (verify_status or 'pending'))

    task = find_task_for_invoice(inv) or build_ephemeral_task_from_invoice(inv)
    temp_tasks_file = RESULTS_DIR / 'screenshot_task.json'
    save_json(temp_tasks_file, [task])

    cmd = [
        sys.executable,
        str(BASE_DIR / 'verify_browser_assist.py'),
        '--tasks-file', str(temp_tasks_file),
        '--max-retries', '5',
        '--no-manual-captcha',
    ]
    env = os.environ.copy()
    env['VERIFY_RUNTIME_DIR'] = str(OUTPUT_DIR)
    env.setdefault('DISPLAY', ':0')
    env.setdefault('XAUTHORITY', '/home/li/.Xauthority')

    try:
        proc = subprocess.run(cmd, cwd=str(BASE_DIR), env=env, capture_output=True, text=True, timeout=180)
        stdout, stderr, returncode = proc.stdout, proc.stderr, proc.returncode
    except subprocess.TimeoutExpired as exc:
        stdout, stderr, returncode = exc.stdout or '', (exc.stderr or '') + '\n[timeout 180s]', 124
    except Exception as exc:
        stdout, stderr, returncode = '', '[spawn] ' + str(exc), 500

    try:
        apply_results()
    except Exception:
        pass

    invoices = load_parsed_invoices()
    refreshed = next((x for x in invoices if x.get('file_hash') == body.file_hash), inv)

    return {
        'ok': bool(refreshed.get('verify_screenshot_path') or refreshed.get('verify_status') in ('查验通过', '查验成功')),
        'file_hash': body.file_hash,
        'file_name': inv.get('file_name', ''),
        'verify_status': refreshed.get('verify_status', verify_status),
        'verify_screenshot_path': refreshed.get('verify_screenshot_path', ''),
        'stdout': str(stdout)[-4000:],
        'stderr': str(stderr)[-2000:],
        'returncode': returncode,
    }


@app.get('/api/screenshot/{file_hash}')
def api_get_screenshot(file_hash: str, format: str = ''):
    from starlette.responses import Response
    inv = find_invoice_by_hash(file_hash)
    path = inv.get('verify_screenshot_path', '')
    if not path or not Path(path).exists():
        raise HTTPException(status_code=404, detail='screenshot not found')
    if format.lower() in ('jpg', 'jpeg'):
        from PIL import Image
        import io as _io
        img = Image.open(path).convert('RGB')
        buf = _io.BytesIO()
        img.save(buf, format='JPEG', quality=90)
        return Response(content=buf.getvalue(), media_type='image/jpeg',
                        headers={'Content-Disposition': f'inline; filename="{Path(path).stem}.jpg"'})
    return FileResponse(path, media_type='image/png',
                        headers={'Content-Disposition': f'inline; filename="{Path(path).name}"'})


@app.post('/api/reset_invoice_to_pending')
def api_reset_invoice_to_pending(body: ResetInvoiceRequest):
    invoices = load_parsed_invoices()
    target = None
    updated = []
    for inv in invoices:
        row = dict(inv)
        if row.get('file_hash', '') == body.file_hash and target is None:
            target = row
            row['verify_status'] = '未查验'
            row['verify_result_summary'] = ''
            row['verify_time'] = ''
            row['verify_channel'] = ''
            row['verify_result_code'] = ''
            row['verify_screenshot_path'] = ''
            row['verify_raw_result_path'] = ''
            row['is_voided'] = '未知'
            row['is_abnormal'] = '未知'
        updated.append(row)
    if target is None:
        raise HTTPException(status_code=404, detail='invoice not found by file_hash')

    rebuild_parser_outputs(updated)
    build_assets()
    invoices = enrich_invoice_sources(load_parsed_invoices())
    tasks = load_tasks()
    return {
        'ok': True,
        'reset_file_name': target.get('file_name', ''),
        'invoice_count': len(invoices),
        'task_count': len(tasks),
        'tabs': invoice_tabs_payload(invoices),
    }


@app.post('/api/delete_invoice')
def api_delete_invoice(body: DeleteInvoiceRequest):
    invoices = load_parsed_invoices()
    target = None
    kept = []
    for inv in invoices:
        if inv.get('file_hash', '') == body.file_hash and target is None:
            target = inv
        else:
            kept.append(inv)
    if target is None:
        raise HTTPException(status_code=404, detail='invoice not found by file_hash')

    rebuild_parser_outputs(kept)

    # Remove uploaded source file by default if it lives under uploads/
    file_path = Path(target.get('file_path', '')) if target.get('file_path') else None
    removed_source = False
    if file_path and file_path.exists() and (str(file_path).startswith(str(UPLOAD_DIR)) or body.delete_source_file):
        try:
            file_path.unlink()
            removed_source = True
        except Exception:
            removed_source = False

    # Remove captcha/session/cache files associated with task id
    task_id = target.get('verify_task_id', '')
    removed_cache = 0
    if task_id:
        for d in [CAPTCHA_DIR, SESSION_DIR, RESULTS_DIR]:
            if not d.exists():
                continue
            for p in d.glob(f'{task_id}*'):
                try:
                    if p.is_file():
                        p.unlink()
                        removed_cache += 1
                except Exception:
                    pass

    build_assets()
    invoices = enrich_invoice_sources(load_parsed_invoices())
    tasks = load_tasks()
    return {
        'ok': True,
        'deleted_file_name': target.get('file_name', ''),
        'removed_source_file': removed_source,
        'removed_cache_files': removed_cache,
        'invoice_count': len(invoices),
        'task_count': len(tasks),
        'tabs': invoice_tabs_payload(invoices),
    }


@app.post('/api/fetch_captcha')
def api_fetch_captcha(body: TaskAction):
    ensure_dirs()
    task = find_task(body.task_id)
    return fetch_captcha_for_task(task)


@app.post('/api/submit_captcha')
def api_submit_captcha(body: SubmitCaptchaRequest):
    ensure_dirs()
    task = find_task(body.task_id)
    return submit_captcha_for_task(task, body.captcha_text)


@app.post('/api/rebuild')
def api_rebuild():
    build_assets()
    return {'ok': True, **summarize_tasks(load_tasks())}


@app.get('/healthz')
def healthz():
    return {
        'ok': True,
        'output_dir': str(OUTPUT_DIR),
        'tasks_file': str(VERIFY_TASKS_JSON),
    }


# ── HTML / Frontend ──────────────────────────────────────────────

INDEX_HTML = """<!doctype html>
<html lang='zh-CN'>
<head>
<meta charset='utf-8'>
<title>Invoice Workbench</title>
<style>
body { font-family: Arial, 'PingFang SC', 'Microsoft YaHei', sans-serif; margin: 20px; color: #222; }
.container { display: grid; grid-template-columns: 1.2fr 1fr; gap: 20px; }
.panel { border: 1px solid #ddd; border-radius: 12px; padding: 16px; }
.task, .invoice-item { border: 1px solid #eee; border-radius: 10px; padding: 10px; margin-bottom: 10px; }
.task button, .invoice-item button, .actions button, .ocr button { margin-right: 8px; margin-top: 6px; }
#captcha-img { max-width: 100%; border: 1px solid #ddd; border-radius: 8px; background: #fafafa; }
code { background: #f4f4f4; padding: 2px 6px; border-radius: 6px; }
.small { color: #666; font-size: 12px; }
.ok { color: #027a48; }
.warn { color: #b54708; }
.err { color: #c53030; }
.section-title { font-size: 15px; font-weight: 600; margin: 12px 0 6px 0; padding-bottom: 4px; border-bottom: 1px solid #eee; }
input[type=text] { padding: 6px 10px; border: 1px solid #ccc; border-radius: 6px; font-size: 14px; }
select { padding: 6px 10px; border: 1px solid #ccc; border-radius: 6px; font-size: 14px; }
button { padding: 6px 14px; border-radius: 6px; border: 1px solid #bbb; background: #f9f9f9; cursor: pointer; font-size: 14px; }
button:hover { background: #e8e8e8; }
button.primary { background: #1677ff; color: #fff; border-color: #1677ff; }
button.primary:hover { background: #4096ff; }
.status-badge { display: inline-block; padding: 2px 8px; border-radius: 10px; font-size: 11px; font-weight: 600; }
.status-badge.pending { background: #fff7e6; color: #b54708; }
.status-badge.done { background: #e6fffb; color: #027a48; }
.status-badge.failed { background: #fff1f0; color: #c53030; }
.status-badge.partial { background: #f0f5ff; color: #1d39c4; }
.status-badge.source { background: #f5f5f5; color: #555; }
#tabs-bar button.active { background:#1677ff; color:#fff; border-color:#1677ff; }
button.danger { background:#fff1f0; color:#c53030; border-color:#ffccc7; }
#result-box { white-space: pre-wrap; background: #fafafa; padding: 12px; border-radius: 8px; min-height: 40px; font-size: 13px; }
</style>
</head>
<body>
<h1>📋 发票验证码工作台</h1>

<div class='actions' style='margin-bottom:16px;'>
  <span class='section-title' style='display:inline; margin:0; border:0;'>📂 输入设置</span>
</div>
<div class='actions'>
  <label>输入目录：<input id='input-dir' type='text' value='' placeholder='选择或输入发票目录路径' style='width:380px; margin-right:6px;' /></label>
  <label>输出目录：<select id='output-dir-select' style='min-width:300px; margin-right:6px;'></select></label>
  <label>手填/新建输出目录：<input id='output-dir-input' type='text' value='' placeholder='首次启动可直接填一个目录，如 D:\\invoice_output' style='width:380px; margin-right:6px;' /></label>
  <button onclick='applyOutputDirInput()'>使用/创建该目录</button>
  <button class='primary' onclick='parseInputDir()'>解析</button>
  <button onclick='rebuildTasks()'>重建查验任务</button>
  <button onclick='loadTasks()'>刷新</button>
</div>
<div class='actions' style='margin-top:8px; padding:10px; background:#f6f8fa; border-radius:8px;'>
  <span style='font-weight:600; margin-right:8px;'>📤 上传PDF：</span>
  <input id='file-upload' type='file' accept='.pdf,.xml,.ofd' multiple style='display:none' onchange='uploadFiles(this.files)' />
  <button onclick='document.getElementById("file-upload").click()'>选择文件</button>
  <input id='dir-upload' type='file' accept='.pdf,.xml,.ofd' webkitdirectory style='display:none' onchange='uploadFiles(this.files)' />
  <button onclick='document.getElementById("dir-upload").click()'>选择目录</button>
  <span id='upload-status' class='small' style='margin-left:12px;'></span>
</div>
<div class='actions' style='margin-top:8px; padding:10px; background:#fffbe6; border-radius:8px;'>
  <span style='font-weight:600; margin-right:8px;'>🤖 批量查验：</span>
  <button class='primary' id='bulk-start-btn' onclick='startBulkVerify()'>开始批量查验当前待查验</button>
  <button id='bulk-stop-btn' onclick='stopBulkVerify()'>停止</button>
  <span id='bulk-summary' class='small' style='margin-left:12px;'></span>
</div>

<div class='container'>
  <div class='panel'>
    <h2 id='list-title'>全部发票 <span id='task-count'></span></h2>
    <div id='tabs-bar' style='margin-bottom:12px; display:flex; gap:8px; flex-wrap:wrap;'></div>
    <div id='task-list'></div>
  </div>
  <div class='panel'>
    <h2>验证码面板</h2>
    <div id='current-task'></div>
    <img id='captcha-img' alt='captcha' />
    <div class='ocr' id='ocr-box'></div>
    <div style='margin-top: 12px;'>
      <input id='captcha-input' placeholder='输入/确认验证码' style='font-size: 20px; padding: 8px; width: 220px; text-transform: uppercase;' />
      <button onclick='submitCaptcha()'>提交查验</button>
    </div>
    <pre id='result-box'></pre>
    <div class='section-title'>批量日志</div>
    <pre id='bulk-log-box' style='white-space: pre-wrap; background:#fafafa; padding:12px; border-radius:8px; min-height:120px; max-height:260px; overflow:auto;'></pre>
  </div>
</div>

<script>
let currentTaskId = null;
let currentTab = 'all';
let lastParsedInvoices = [];
let lastTasks = [];
let lastTabs = {all:0,pending:0,partial:0,passed:0,failed:0};
let bulkPollTimer = null;

function basenameOfPath(p) {
  if (!p) return '';
  return p.replace(/[\\/]+$/, '').split(/[\\/]/).pop();
}

function getChosenOutputDir() {
  const inputVal = (document.getElementById('output-dir-input')?.value || '').trim();
  const selectVal = (document.getElementById('output-dir-select')?.value || '').trim();
  return inputVal || selectVal;
}

function renderOutputDirs(data) {
  const sel = document.getElementById('output-dir-select');
  const input = document.getElementById('output-dir-input');
  const curVal = sel.value;
  sel.innerHTML = '';
  (data.output_dirs || []).forEach(item => {
    const opt = document.createElement('option');
    opt.value = item.path;
    let label = item.name;
    if (item.task_count >= 0) label += `（待查验 ${item.task_count}）`;
    if (item.has_parsed) label += ' ✅';
    opt.textContent = label;
    if (item.path === data.output_dir || item.path === curVal) opt.selected = true;
    sel.appendChild(opt);
  });
  if (!sel.value && sel.options.length) sel.selectedIndex = 0;
  if (input && data.output_dir && !input.value.trim()) input.value = data.output_dir;
}

function statusBadge(status) {
  if (!status) return '';
  const m = {
    'parsed': ['done', '已解析'],
    'partial': ['partial', '待补字段'],
    'failed': ['failed', '失败'],
    '未查验': ['pending', '未查验'],
    '待查验': ['pending', '待查验'],
    '查验通过': ['done', '已查验'],
    '查验失败': ['failed', '查验失败'],
    '异常': ['failed', '异常'],
    '需人工复核': ['failed', '需复核'],
  };
  const [cls, text] = m[status] || ['pending', status];
  return `<span class='status-badge ${cls}'>${text}</span>`;
}

function sourceBadge(sourceKind) {
  const label = sourceKind === 'uploaded' ? 'uploaded' : 'parsed-from-dir';
  return `<span class='status-badge source'>${label}</span>`;
}

function renderTabs(tabs) {
  lastTabs = tabs || {all:0,pending:0,partial:0,passed:0,failed:0};
  const el = document.getElementById('tabs-bar');
  const defs = [
    ['all', `全部 ${lastTabs.all || 0}`],
    ['pending', `待查验 ${lastTabs.pending || 0}`],
    ['partial', `待补字段 ${lastTabs.partial || 0}`],
    ['passed', `已通过 ${lastTabs.passed || 0}`],
    ['failed', `失败/复核 ${lastTabs.failed || 0}`],
  ];
  el.innerHTML = '';
  defs.forEach(([key, text]) => {
    const btn = document.createElement('button');
    btn.textContent = text;
    if (key === currentTab) btn.classList.add('active');
    btn.onclick = () => { currentTab = key; renderCurrentList(); };
    el.appendChild(btn);
  });
}

function filterInvoicesByTab(invoices) {
  if (currentTab === 'all') return invoices;
  return (invoices || []).filter(inv => {
    const verifyRequired = inv.verify_required_fields_status || '';
    const verifyStatus = inv.verify_status || '未查验';
    if (currentTab === 'passed') return verifyStatus === '查验通过';
    if (currentTab === 'failed') return ['查验失败', '异常', '需人工复核'].includes(verifyStatus);
    if (currentTab === 'pending') return verifyRequired === 'ready' && ['', '未查验', '待查验'].includes(verifyStatus);
    if (currentTab === 'partial') return verifyRequired !== 'ready' && ['', '未查验', '待查验'].includes(verifyStatus);
    return true;
  });
}

function renderInvoiceList(invoices, tasks) {
  const el = document.getElementById('task-list');
  const title = document.getElementById('list-title');
  const filtered = filterInvoicesByTab(invoices || []);
  const titleMap = {all:'全部发票', pending:'待查验', partial:'待补字段', passed:'已通过', failed:'失败/复核'};
  title.innerHTML = `${titleMap[currentTab] || '发票列表'} <span id='task-count'>(${filtered.length} 张)</span>`;

  const taskMap = {};
  (tasks || []).forEach(t => {
    const invNum = t.invoice_number || (t.invoice || {}).invoice_number || '';
    const invDate = t.invoice_date || (t.invoice || {}).invoice_date || '';
    const amt = t.total_amount || (t.invoice || {}).total_amount || '';
    taskMap[invNum + '|' + invDate + '|' + amt] = t;
  });

  if (!filtered.length) {
    el.innerHTML = '<div class="small">当前 tab 下没有发票。</div>';
    return;
  }

  let html = '';
  filtered.forEach(inv => {
    const invNum = inv.invoice_number || '';
    const invDate = inv.invoice_date || '';
    const amt = inv.total_amount || '';
    const seller = inv.seller_name || '';
    const fname = inv.file_name || '';
    const pStatus = inv.parse_status || '';
    const vStatus = inv.verify_status || '未查验';
    const sourceKind = inv.source_kind || 'parsed-from-dir';
    const missing = inv.verify_missing_fields || inv.review_reason || '';
    const taskKey = invNum + '|' + invDate + '|' + amt;
    const task = taskMap[taskKey];
    const taskId = task ? task.task_id : '';
    let actions = '';
    if (taskId && !['passed','failed'].includes(currentTab)) {
      actions += `<button onclick="fetchCaptcha('${taskId}')">取验证码</button>`;
      actions += `<button class='primary' onclick="autoVerifyScreenshot('${inv.file_hash}')">一键验证+截图</button>`;
    }
    if (vStatus === '查验通过') {
      actions += `<button onclick="captureVerifyScreenshot('${inv.file_hash}')">补截图</button>`;
      if (inv.verify_screenshot_path) {
        actions += `<button onclick="window.open('/api/screenshot/${inv.file_hash}','_blank')">查看截图</button>`;
      }
    }
    if (['passed','failed'].includes(currentTab)) {
      actions += `<button onclick="resetInvoiceToPending('${inv.file_hash}')">重置为待查验</button>`;
    }
    const deleteSource = sourceKind === 'uploaded' ? 'true' : 'false';
    actions += `<button class='danger' onclick="deleteInvoice('${inv.file_hash}','${deleteSource}')">${sourceKind === 'uploaded' ? '删除文件+记录' : '移除记录'}</button>`;
    html += `<div class='invoice-item'>
      <div><strong>${invNum || '—'}</strong> ${statusBadge(pStatus)} ${statusBadge(vStatus)} ${sourceBadge(sourceKind)}</div>
      <div>${invDate || '—'} · ¥${amt || '—'} · ${seller || '—'}</div>
      <div class='small'>${fname}</div>
      ${missing ? `<div class='small warn'>${missing}</div>` : ''}
      ${actions}
    </div>`;
  });
  el.innerHTML = html;
}

function renderCurrentList() {
  renderTabs(lastTabs);
  renderInvoiceList(lastParsedInvoices, lastTasks);
}

async function refreshAllData(showMessage='') {
  const [tasksRes, invRes] = await Promise.all([
    fetch('/api/tasks'),
    fetch('/api/parsed_invoices')
  ]);
  const tasksData = await tasksRes.json();
  const invData = await invRes.json();
  renderOutputDirs(tasksData);
  if (tasksData.input_dir) {
    const inputEl = document.getElementById('input-dir');
    if (inputEl && !inputEl.value) inputEl.value = tasksData.input_dir;
  }
  if (tasksData.output_dir) {
    const outputInputEl = document.getElementById('output-dir-input');
    if (outputInputEl && !outputInputEl.value.trim()) outputInputEl.value = tasksData.output_dir;
  }
  lastTasks = tasksData.tasks || [];
  lastParsedInvoices = invData.invoices || [];
  renderTabs(invData.tabs || tasksData.tabs || {all:0,pending:0,partial:0,passed:0,failed:0});
  renderCurrentList();
  if (showMessage) document.getElementById('result-box').textContent = showMessage;
}

function renderBulkStatus(status) {
  const summaryEl = document.getElementById('bulk-summary');
  const logEl = document.getElementById('bulk-log-box');
  const startBtn = document.getElementById('bulk-start-btn');
  const stopBtn = document.getElementById('bulk-stop-btn');
  if (!status) return;
  summaryEl.textContent = `运行中=${status.running ? '是' : '否'} | 总数 ${status.total || 0} | 已完成 ${status.done || 0} | 成功 ${status.success || 0} | 失败 ${status.failed || 0}${status.current_invoice_number ? ' | 当前 ' + status.current_invoice_number + ' #' + (status.current_attempt || 0) : ''}`;
  logEl.textContent = (status.logs || []).join('\\n');
  startBtn.disabled = !!status.running;
  stopBtn.disabled = !status.running;
}

async function pollBulkStatus() {
  try {
    const res = await fetch('/api/bulk_status');
    const data = await res.json();
    renderBulkStatus(data);
    if (data.running) {
      bulkPollTimer = setTimeout(pollBulkStatus, 1000);
    } else {
      bulkPollTimer = null;
      await refreshAllData();
    }
  } catch (e) {
    document.getElementById('bulk-log-box').textContent = `批量状态获取失败: ${e.message}`;
    bulkPollTimer = null;
  }
}

async function startBulkVerify() {
  const res = await fetch('/api/bulk_start', {method:'POST'});
  const data = await res.json();
  if (!res.ok) {
    document.getElementById('result-box').textContent = JSON.stringify(data, null, 2);
    return;
  }
  renderBulkStatus(data.status || {});
  document.getElementById('result-box').textContent = `✅ 已启动批量查验，本轮快照 ${data.queued} 张`;
  if (bulkPollTimer) clearTimeout(bulkPollTimer);
  bulkPollTimer = setTimeout(pollBulkStatus, 800);
}

async function stopBulkVerify() {
  const res = await fetch('/api/bulk_stop', {method:'POST'});
  const data = await res.json();
  renderBulkStatus(data.status || {});
  document.getElementById('result-box').textContent = '已请求停止批量查验';
}

async function loadTasks() {
  document.getElementById('result-box').textContent = '';
  await refreshAllData();
}

async function selectOutputDir() {
  const outputDir = document.getElementById('output-dir-select').value;
  if (!outputDir) return;
  const res = await fetch('/api/select_output_dir', {
    method: 'POST', headers: {'Content-Type':'application/json'}, body: JSON.stringify({output_dir: outputDir})
  });
  const data = await res.json();
  if (!res.ok) { document.getElementById('result-box').textContent = JSON.stringify(data, null, 2); return; }
  const input = document.getElementById('output-dir-input');
  if (input) input.value = data.output_dir || outputDir;
  await refreshAllData(`已切换目录：${data.output_dir}`);
}

async function applyOutputDirInput() {
  const outputDir = (document.getElementById('output-dir-input').value || '').trim();
  if (!outputDir) {
    alert('请输入输出目录');
    return;
  }
  document.getElementById('result-box').textContent = `⏳ 正在使用/创建输出目录：${outputDir}`;
  const res = await fetch('/api/select_output_dir', {
    method: 'POST', headers: {'Content-Type':'application/json'}, body: JSON.stringify({output_dir: outputDir})
  });
  const data = await res.json();
  if (!res.ok) {
    document.getElementById('result-box').textContent = '❌ ' + JSON.stringify(data, null, 2);
    return;
  }
  await refreshAllData(`✅ 已使用输出目录：${data.output_dir}`);
}

async function uploadFiles(fileList) {
  if (!fileList || !fileList.length) return;
  const outputDir = getChosenOutputDir();
  if (!outputDir) { alert('请先选择或填写输出目录'); return; }

  const statusEl = document.getElementById('upload-status');
  statusEl.textContent = `⏳ 正在准备输出目录并上传 ${fileList.length} 个文件...`;
  document.getElementById('result-box').textContent = '';

  const selectRes = await fetch('/api/select_output_dir', {
    method: 'POST', headers: {'Content-Type':'application/json'}, body: JSON.stringify({output_dir: outputDir})
  });
  const selectData = await selectRes.json();
  if (!selectRes.ok) {
    statusEl.textContent = `❌ ${selectData.detail || '输出目录设置失败'}`;
    document.getElementById('result-box').textContent = JSON.stringify(selectData, null, 2);
    return;
  }

  const formData = new FormData();
  for (const f of fileList) formData.append('files', f);

  try {
    const res = await fetch('/api/upload_pdfs', { method: 'POST', body: formData });
    const data = await res.json();
    if (!res.ok) {
      statusEl.textContent = `❌ ${data.detail || '上传失败'}`;
      document.getElementById('result-box').textContent = JSON.stringify(data, null, 2);
      return;
    }
    const routed = data.routed_outputs || [];
    const routedText = routed.length ? '；分发到 ' + routed.map(x => `${basenameOfPath(x.output_dir)}(+${x.added})`).join('，') : '';
    statusEl.textContent = `✅ 上传 ${data.uploaded} 个文件，新入库 ${data.new_invoices || 0} 张${routedText}`;
    if (data.skipped && data.skipped.length) {
      statusEl.textContent += `（跳过 ${data.skipped.join(', ')}）`;
    }
    currentTab = 'all';
    await refreshAllData();
  } catch (e) {
    statusEl.textContent = `❌ 网络错误: ${e.message}`;
  }
}

async function parseInputDir() {
  const inputDir = document.getElementById('input-dir').value.trim();
  const outputDir = getChosenOutputDir();
  if (!inputDir) { alert('请输入输入目录'); return; }
  if (!outputDir) { alert('请选择或填写输出目录'); return; }

  document.getElementById('result-box').textContent = '⏳ 正在解析，请稍候...';
  document.getElementById('result-box').className = 'warn';

  const res = await fetch('/api/parse_input_dir', {
    method: 'POST', headers: {'Content-Type':'application/json'},
    body: JSON.stringify({ input_dir: inputDir, output_dir: outputDir })
  });
  const data = await res.json();
  document.getElementById('result-box').className = '';

  if (!res.ok) {
    document.getElementById('result-box').textContent = '❌ ' + JSON.stringify(data, null, 2);
    return;
  }

  currentTab = 'all';
  await refreshAllData();

  const routed = data.routed_outputs || [];
  const summary = routed.length
    ? `✅ 解析完成，新入库 ${data.new_invoices || 0} 张，分发到：${routed.map(x => `${basenameOfPath(x.output_dir)}(+${x.added})`).join('，')}`
    : `✅ 解析完成：${data.invoice_count || 0} 张发票 → ${data.output_dir}`;
  if (data.task_count) {
    document.getElementById('result-box').textContent = summary + `\\n当前选中目录已生成 ${data.task_count} 条查验任务，可点击"取验证码"。`;
  } else {
    document.getElementById('result-box').textContent = summary;
  }
}

async function rebuildTasks() {
  document.getElementById('result-box').textContent = '⏳ 正在重建...';
  const res = await fetch('/api/rebuild', {method:'POST'});
  const data = await res.json();
  await refreshAllData('✅ 已重建 ready_tasks');
}

function openLocalFile(path) {
  if (!path) return;
  window.open('file://' + path, '_blank');
}

async function captureVerifyScreenshot(fileHash) {
  document.getElementById('result-box').textContent = '⏳ 正在补取官方截图，请稍候...';
  const res = await fetch('/api/capture_verify_screenshot', {
    method: 'POST', headers: {'Content-Type':'application/json'},
    body: JSON.stringify({file_hash: fileHash})
  });
  const data = await res.json();
  if (!res.ok || !data.ok) {
    document.getElementById('result-box').textContent = ['❌ 补截图失败', JSON.stringify(data, null, 2)].join(String.fromCharCode(10));
    await refreshAllData();
    return;
  }
  await refreshAllData();
  document.getElementById('result-box').textContent = ['✅ 补截图完成', (data.verify_screenshot_path || '')].join(String.fromCharCode(10));
}

async function autoVerifyScreenshot(fileHash) {
  document.getElementById('result-box').textContent = '⏳ 正在一键验证+截图（自动识别验证码，最多重试5次）...';
  document.getElementById('result-box').className = 'warn';
  const res = await fetch('/api/auto_verify_screenshot', {
    method: 'POST', headers: {'Content-Type':'application/json'},
    body: JSON.stringify({file_hash: fileHash})
  });
  const data = await res.json();
  document.getElementById('result-box').className = '';
  if (!res.ok) {
    document.getElementById('result-box').textContent = '❌ ' + JSON.stringify(data, null, 2);
    await refreshAllData();
    return;
  }
  const status = data.verify_status || '';
  const screenshot = data.verify_screenshot_path || '';
  var summary;
  if (data.ok) {
    summary = '✅ 验证完成：' + status;
    if (screenshot) { summary = summary + '\n截图：' + screenshot; }
  } else {
    summary = '⚠️ 验证结果：' + status;
    if (data.stdout) { summary = summary + '\n' + data.stdout.slice(-500); }
  }
  document.getElementById('result-box').textContent = summary;
  currentTab = 'all';
  await refreshAllData();
}

async function resetInvoiceToPending(fileHash) {
  const confirmed = window.confirm('确认将这条记录重置为待查验？');
  if (!confirmed) return;
  const res = await fetch('/api/reset_invoice_to_pending', {
    method: 'POST', headers: {'Content-Type':'application/json'},
    body: JSON.stringify({file_hash: fileHash})
  });
  const data = await res.json();
  if (!res.ok) {
    document.getElementById('result-box').textContent = JSON.stringify(data, null, 2);
    return;
  }
  currentTab = 'pending';
  await refreshAllData(`✅ 已重置为待查验：${data.reset_file_name}`);
}

async function deleteInvoice(fileHash, deleteSourceFile) {
  const confirmed = window.confirm(deleteSourceFile === 'true' ? '确认删除这张已上传发票的文件和记录？' : '确认从当前输出目录移除这条记录？');
  if (!confirmed) return;
  const res = await fetch('/api/delete_invoice', {
    method: 'POST', headers: {'Content-Type':'application/json'},
    body: JSON.stringify({file_hash: fileHash, delete_source_file: deleteSourceFile === 'true'})
  });
  const data = await res.json();
  if (!res.ok) {
    document.getElementById('result-box').textContent = JSON.stringify(data, null, 2);
    return;
  }
  await refreshAllData(`✅ 已处理：${data.deleted_file_name}`);
}

async function fetchCaptcha(taskId) {
  currentTaskId = taskId;
  document.getElementById('result-box').textContent = '正在取验证码...';
  document.getElementById('result-box').className = '';
  const res = await fetch('/api/fetch_captcha', {
    method: 'POST', headers: {'Content-Type':'application/json'}, body: JSON.stringify({task_id: taskId})
  });
  const data = await res.json();
  if (!res.ok) {
    document.getElementById('result-box').textContent = JSON.stringify(data, null, 2);
    return;
  }
  document.getElementById('captcha-img').src = data.captcha_image_data_url;
  document.getElementById('current-task').innerHTML = `<div><code>${taskId}</code></div><div class='small'>rule key4=${data.rule_key4} key5=${data.rule_key5}</div>`;
  const ocrBox = document.getElementById('ocr-box');
  ocrBox.innerHTML = '';
  let firstCandidate = '';
  if (data.ocr && data.ocr.enabled) {
    const candidates = [];
    const providers = (data.ocr || {}).providers || {};
    if (providers.ddddocr && providers.ddddocr.enabled && providers.ddddocr.text) {
      const text = providers.ddddocr.text;
      candidates.push(text);
      const tag = document.createElement('div');
      tag.className = 'small ok';
      tag.textContent = `ddddocr: ${text}`;
      ocrBox.appendChild(tag);
    }
    if (providers.ocr_service && providers.ocr_service.enabled) {
      const recTopk = (((providers.ocr_service || {}).rec || {}).topk || []);
      const charTopk = (((providers.ocr_service || {}).char || {}).topk || []);
      recTopk.forEach(x => { if (x.text && !candidates.includes(x.text)) candidates.push(x.text); });
      charTopk.forEach(x => { if (x.text && !candidates.includes(x.text)) candidates.push(x.text); });
    }
    if (candidates.length) firstCandidate = candidates[0];
    candidates.slice(0, 8).forEach((text, idx) => {
      const btn = document.createElement('button');
      btn.textContent = idx === 0 ? `${text} ← 默认` : text;
      btn.onclick = () => document.getElementById('captcha-input').value = text;
      ocrBox.appendChild(btn);
    });
  } else if (data.ocr) {
    ocrBox.innerHTML = `<div class='small'>OCR不可用：${data.ocr.reason || 'unknown'}</div>`;
  }
  document.getElementById('captcha-input').value = firstCandidate;
  document.getElementById('result-box').textContent = '验证码已取回，请确认后提交。';
}

async function submitCaptcha() {
  if (!currentTaskId) { document.getElementById('result-box').textContent = '请先取验证码'; return; }
  const captchaText = document.getElementById('captcha-input').value.trim();
  const res = await fetch('/api/submit_captcha', {
    method: 'POST', headers: {'Content-Type':'application/json'}, body: JSON.stringify({task_id: currentTaskId, captcha_text: captchaText})
  });
  const data = await res.json();
  document.getElementById('result-box').textContent = JSON.stringify(data, null, 2);
  if (data.ok) {
    document.getElementById('captcha-input').value = '';
    await refreshAllData();
  }
}

document.getElementById('output-dir-select').addEventListener('change', selectOutputDir);
loadTasks();
pollBulkStatus();
</script>
</body>
</html>"""


@app.get('/', response_class=HTMLResponse)
def index():
    return HTMLResponse(INDEX_HTML)


# ── Main ─────────────────────────────────────────────────────────

def main() -> None:
    global INPUT_DIR
    parser = argparse.ArgumentParser(description='发票验证码工作台')
    parser.add_argument('--output-dir', default=str(DEFAULT_OUTPUT_DIR), help='parser 输出目录')
    parser.add_argument('--input-dir', default=str(DEFAULT_INPUT_DIR), help='发票 PDF 输入目录')
    parser.add_argument('--host', default='0.0.0.0')
    parser.add_argument('--port', type=int, default=8787)
    args = parser.parse_args()

    if args.input_dir:
        INPUT_DIR = Path(args.input_dir)
    set_output_dir(args.output_dir)

    # Auto-download wlop.js on first run (needed for captcha verification)
    try:
        from verify.run_verifier import ensure_wlop_js
        wlop = ensure_wlop_js()
        print(f'[startup] wlop.js ready: {wlop} ({wlop.stat().st_size} bytes)')
    except Exception as exc:
        print(f'[startup] ⚠️ wlop.js auto-download failed: {exc}')
        print('[startup] 验证码查验功能将不可用。请检查网络连接或手动下载 wlop.js')
        print('[startup]   参考 README 中 "手动下载 wlop.js" 章节')

    import uvicorn
    uvicorn.run(app, host=args.host, port=args.port)


if __name__ == '__main__':
    main()
