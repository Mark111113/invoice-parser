#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import requests

BASE_DIR = Path(__file__).resolve().parent
WORKSPACE_DIR = BASE_DIR.parent
PARSER_DIR = WORKSPACE_DIR / 'invoice-parser'
if str(PARSER_DIR) not in sys.path:
    sys.path.insert(0, str(PARSER_DIR))

from .verify_contract import build_standard_result, extract_update_from_result  # type: ignore
from .swjg_map import SWJG_MAP, get_swjg, get_invoice_type  # type: ignore

requests.packages.urllib3.disable_warnings()

DEFAULT_OUTPUT_DIR = Path('/mnt/fn/Download3/clawdbotfile/财务/发票_解析结果')
DEFAULT_TASKS_FILE = DEFAULT_OUTPUT_DIR / 'verify_tasks' / 'ready_tasks.json'
DEFAULT_RESULTS_DIR = DEFAULT_OUTPUT_DIR / 'verify_results'
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


def normalize_money_for_key4(value: str) -> str:
    value = (value or '').replace(',', '').strip()
    if not value:
        return ''
    try:
        from decimal import Decimal

        num = Decimal(value)
        text = format(num, 'f')
        if '.' in text:
            text = text.rstrip('0').rstrip('.')
        return text
    except Exception:
        return value


def infer_fplx_from_invoice_type(invoice_type_text: str, fallback_code: str, invoice_number: str = '') -> str:
    text = invoice_type_text or ''
    if invoice_number.isdigit() and len(invoice_number) == 20 and '电子发票' in text:
        return '09'
    if '电子发票' in text and '增值税专用发票' in text:
        return '09'
    return fallback_code


def load_tasks(path: Path) -> list[dict[str, Any]]:
    return json.loads(path.read_text(encoding='utf-8')) if path.exists() else []


def filter_tasks(tasks: list[dict[str, Any]], task_ids: list[str], limit: int | None) -> list[dict[str, Any]]:
    selected = tasks
    if task_ids:
        allow = set(task_ids)
        selected = [t for t in selected if t.get('task_id', '') in allow]
    if limit is not None:
        selected = selected[:limit]
    return selected


def ensure_dirs(results_dir: Path) -> None:
    results_dir.mkdir(parents=True, exist_ok=True)
    (results_dir / 'artifacts').mkdir(parents=True, exist_ok=True)


def now_ms() -> str:
    return str(int(time.time() * 1000))


def jsonp_to_dict(text: str) -> dict[str, Any]:
    text = text.strip()
    if '(' in text and text.endswith(')'):
        inner = text[text.find('(') + 1:-1]
        try:
            return json.loads(inner)
        except Exception:
            return {'raw': text}
    try:
        return json.loads(text)
    except Exception:
        return {'raw': text}


def infer_swjg_from_invoice_number(inv_num: str) -> dict[str, Any] | None:
    inv_num = (inv_num or '').strip()
    if len(inv_num) == 20:
        code = inv_num[2:4]
        special = {
            '91': '2102',
            '92': '3702',
            '93': '3302',
            '94': '3502',
            '95': '4403',
            '00': '0000',
        }
        dqdm = special.get(code) or f'{code}00'
        for item in SWJG_MAP:
            if item['code'] == dqdm:
                return {
                    'name': item['name'],
                    'url': item['url'] + '/NWebQuery',
                    'area': dqdm,
                }
    if len(inv_num) >= 10:
        return get_swjg(inv_num[:10])
    return None


def build_js_runtime_env() -> dict[str, Any]:
    return {
        'webdriver': True,
        'innerWidth': 1280,
        'innerHeight': 720,
        'screenX': 10,
        'screenY': -10,
        'bodyClientWidth': 1265,
        'bodyClientHeight': 1093,
        'docClientWidth': 1265,
        'docClientHeight': 720,
        'wzwschallenge': '5d60fac72ec4c898a9ab0317efc4bca4',
        'wzwschallengex': 'cmhsZWdkY2hi',
        'jqueryState': {'m1Count': '220', 'm2Count': '5533'},
        'localStorage': {'vendorSub': '0'},
    }


def run_js_key9(fpdm: str, fphm: str, yzm_publickey: str, cy_arg: str) -> dict[str, Any]:
    # Ensure wlop.js is available before running the node sandbox
    ensure_wlop_js()
    node_script = BASE_DIR / 'node_key9_sandbox.js'
    payload = {
        'fpdm': fpdm,
        'fphm': fphm,
        'kprq': cy_arg,
        'cyArg': cy_arg,
        'yzmPublicKey': yzm_publickey,
        'env': build_js_runtime_env(),
    }
    proc = subprocess.run(
        ['node', str(node_script)],
        input=json.dumps(payload, ensure_ascii=False),
        text=True,
        capture_output=True,
        check=False,
        cwd=str(BASE_DIR),
    )
    output_text = (proc.stdout or proc.stderr or '').strip()
    if not output_text:
        raise RuntimeError('node_key9_sandbox 无输出')
    try:
        data = json.loads(output_text)
    except Exception as exc:
        raise RuntimeError(f'node_key9_sandbox 输出不可解析: {output_text[:500]}') from exc
    if not data.get('ok'):
        raise RuntimeError(data.get('error') or 'node_key9_sandbox 执行失败')
    return data


WLOP_JS_URL = 'https://inv-veri.chinatax.gov.cn/js/wlop.js'


def ensure_wlop_js() -> Path:
    """Ensure wlop.js exists locally, downloading from tax bureau if needed."""
    wlop_path = BASE_DIR / 'upstream_js' / 'wlop.js'
    if wlop_path.exists() and wlop_path.stat().st_size > 10000:
        return wlop_path
    wlop_path.parent.mkdir(parents=True, exist_ok=True)
    print(f'[verify] Downloading wlop.js from {WLOP_JS_URL} ...')
    try:
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    except Exception:
        pass
    resp = requests.get(WLOP_JS_URL, headers={
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36',
        'Referer': 'https://inv-veri.chinatax.gov.cn/',
    }, verify=False, timeout=60)
    resp.raise_for_status()
    if len(resp.content) < 10000:
        raise RuntimeError(f'Downloaded wlop.js too small ({len(resp.content)} bytes), likely not the real file')
    wlop_path.write_bytes(resp.content)
    print(f'[verify] wlop.js saved ({len(resp.content)} bytes)')
    return wlop_path


def append_flwq39(url: str, params: dict[str, Any]) -> str:
    callback = params.get('callback', 'jQueryCallback')
    base = f"{url}?callback={callback}"
    for k, v in params.items():
        if k == 'callback':
            continue
        base += f"&{requests.utils.quote(str(k), safe='')}={requests.utils.quote(str(v), safe='')}"
    req_json = json.dumps({'type': 'POST', 'url': base}, ensure_ascii=False)
    wlop_path = ensure_wlop_js()
    script = f"""
const fs=require('fs'), vm=require('vm');
const code=fs.readFileSync('{wlop_path.as_posix()}','utf8');
function encodeBase64(s){{return Buffer.from(String(s),'utf8').toString('base64');}}
function makeProto(tag,next=null){{ const proto={{toString(){{return `[object ${{tag}}]`;}}}}; Object.defineProperty(proto, Symbol.toStringTag, {{value:tag}}); Object.setPrototypeOf(proto,next); return proto; }}
const wp4=makeProto('Object'), wp3=makeProto('EventTarget',wp4), wp2=makeProto('WindowProperties',wp3), wp1=makeProto('Window',wp2);
const dp5=makeProto('Object'), dp4=makeProto('EventTarget',dp5), dp3=makeProto('Node',dp4), dp2=makeProto('Document',dp3), dp1=makeProto('HTMLDocument',dp2);
const windowObj={{navigator:{{webdriver:true, appName:'Netscape', userAgent:'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36'}}, innerWidth:1280, innerHeight:720, screenX:10, screenY:-10, screen:{{width:1280,height:720}}, toString(){{return '[object Window]';}}}};
Object.setPrototypeOf(windowObj, wp1);
const document={{body:{{clientWidth:1265,clientHeight:1093}},documentElement:{{clientWidth:1265,clientHeight:720}},createElement(){{return {{}};}},all:undefined,dda:undefined,toString(){{return '[object HTMLDocument]';}}}};
Object.setPrototypeOf(document, dp1);
windowObj.document=document; windowObj.window=windowObj; windowObj.self=windowObj; windowObj.top=windowObj; windowObj.parent=windowObj; document.defaultView=windowObj;
const store=new Map([['vendorSub','0']]);
const $=function(){{}}; $.extend=o=>(Object.assign($,o),$); $.ajaxSettings={{}}; $.ajaxSetup=function(obj){{ $.ajaxSettings=Object.assign($.ajaxSettings||{{}}, obj||{{}}); }};
$.cs={{encode:encodeBase64}}; $.m1Count='220'; $.m2Count='5533';
function JSEncrypt(){{}}; JSEncrypt.prototype.setPublicKey=function(){{}}; JSEncrypt.prototype.encrypt=function(x){{return x;}};
const context={{console:{{log(){{}},warn(){{}},error(){{}},info(){{}},debug(){{}}}}, window:windowObj, document, navigator:windowObj.navigator, top:windowObj, self:windowObj, parent:windowObj, innerWidth:1280, innerHeight:720, screenX:10, screenY:-10, screen:windowObj.screen, invInt:'30', wzwschallenge:'5d60fac72ec4c898a9ab0317efc4bca4', wzwschallengex:'cmhsZWdkY2hi', localStorage:{{getItem(k){{return store.has(k)?store.get(k):null;}}, setItem(k,v){{store.set(k,String(v));}}, removeItem(k){{store.delete(k);}}}}, HTMLAllCollection:function HTMLAllCollection(){{}}, setInterval(){{return 1;}}, clearInterval(){{}}, setTimeout(){{return 1;}}, clearTimeout(){{}}, unescape, encodeURIComponent, decodeURIComponent, escape, Uint8Array, Uint32Array, ArrayBuffer, Math, Date, parseInt, JSEncrypt, $, jQuery:$}};
context.global=context; context.globalThis=context;
vm.createContext(context); vm.runInContext(code, context, {{timeout:20000}}); if(context.$?.bicc?.iii) context.$.bicc.iii();
const req={req_json};
context.$.ajaxSettings.beforeSend({{}}, req);
process.stdout.write(req.url);
"""
    proc = subprocess.run(['node', '-e', script], text=True, capture_output=True, check=False, cwd=str(BASE_DIR))
    out = (proc.stdout or proc.stderr or '').strip()
    if not out:
        raise RuntimeError('flwq39 生成失败')
    return out


def post_jsonp_via_signed_url(url: str, params: dict[str, Any], timeout: int = 20) -> tuple[str, dict[str, Any]]:
    signed_url = append_flwq39(url, params)
    resp = requests.post(signed_url, headers=build_request_headers(), verify=False, timeout=timeout)
    resp.raise_for_status()
    return signed_url, jsonp_to_dict(resp.text)


def build_request_headers() -> dict[str, str]:
    return {
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36',
        'Referer': 'https://inv-veri.chinatax.gov.cn/',
        'Accept': '*/*',
    }


def write_standard_result(task: dict[str, Any], payload: dict[str, Any], result_file: Path) -> None:
    result_file.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')


def backend_api_probe(task: dict[str, Any], results_dir: Path) -> dict[str, Any]:
    invoice = task.get('invoice', {}) or {}
    inv_num = invoice.get('invoice_number', '')
    inv_date = invoice.get('invoice_date', '')
    total_amount = invoice.get('total_amount', '')

    if not inv_num or not inv_date:
        raise ValueError('task 缺少 invoice_number / invoice_date')

    fpdm = invoice.get('invoice_code', '') or ''
    if not fpdm and len(inv_num) >= 20:
        fpdm = inv_num[:12]
    elif not fpdm and len(inv_num) >= 10:
        fpdm = inv_num[:10]
    fphm = inv_num[-8:] if len(inv_num) >= 8 else inv_num
    kprq = inv_date.replace('年', '').replace('月', '').replace('日', '')
    key4_value = normalize_money_for_key4(total_amount)

    if not fpdm or len(fpdm) not in (10, 12):
        return build_standard_result(
            task,
            verify_time=time.strftime('%Y-%m-%dT%H:%M:%S'),
            verify_channel='invoice-verifier(api_probe)',
            result_code='',
            result_status='缺少可用发票代码',
            result_summary='缺少可用发票代码，当前无法直接走 API 查验',
            is_success=False,
            is_voided='未知',
            is_abnormal='是',
            screenshot_path='',
            raw_response_path='',
            raw_response='',
            full_result={'invoice_code': fpdm, 'invoice_number': inv_num},
            source_kind='api_probe',
            verify_status_code='unsupported',
            verify_status='需人工复核',
        )

    swjg = infer_swjg_from_invoice_number(inv_num) or get_swjg(fpdm)
    yzm_ts = now_ms()
    yzm_callback = f'jQuery{yzm_ts}'
    yzm_key9_bundle = run_js_key9(fpdm, fphm, yzm_ts, '')
    yzm_payload = {
        'callback': yzm_callback,
        'fpdm': fpdm,
        'fphm': fphm,
        'r': '0.5',
        'v': '2.0.23_090',
        'nowtime': yzm_ts,
        'publickey': yzm_ts,
        'key9': yzm_key9_bundle['yzm'],
        '_': str(max(int(yzm_ts) - 1, 0)),
    }
    yzm_url = f"{swjg['url']}/yzmQuery"
    yzm_signed_url, yzm_data = post_jsonp_via_signed_url(yzm_url, yzm_payload)

    vat_ts = now_ms()
    vat_callback = f'jQuery{vat_ts}'
    vat_publickey = time.strftime('%Y-%m-%d %H:%M:%S')
    vat_key9_bundle = run_js_key9(fpdm, fphm, vat_publickey, vat_publickey)
    vat_payload = {
        'callback': vat_callback,
        'key1': fpdm,
        'key2': fphm,
        'key3': kprq,
        'key4': key4_value,
        'fplx': infer_fplx_from_invoice_type(invoice.get('invoice_type', ''), get_invoice_type(fpdm), inv_num),
        'yzm': 'TEST',
        'yzmSj': vat_publickey,
        'index': yzm_data.get('key3', ''),
        'key6': yzm_data.get('key6', ''),
        'publickey': vat_publickey,
        'key9': vat_key9_bundle['cy'],
        '_': str(max(int(vat_ts) - 1, 0)),
    }
    vat_url = f"{swjg['url']}/vatQuery"
    vat_signed_url, vat_data = post_jsonp_via_signed_url(vat_url, vat_payload, timeout=30)
    vat_text = json.dumps(vat_data, ensure_ascii=False)

    key1 = str(vat_data.get('key1', ''))
    status_text = RESULT_CODE_MAP.get(key1, f'未知({key1})') if key1 else '无响应'
    raw_path = results_dir / f"{task['task_id']}_api_probe_raw.json"
    raw_path.write_text(
        json.dumps(
            {
                'diagnostic': {
                    'note': 'Current Python key9 implementation is based on old static logic and is known to diverge from modern browser runtime logic (webdriver/window/localStorage influenced).',
                    'field_mapping_basis': 'live-browser-traces-2026-04-13',
                },
                'yzm_request': {'url': yzm_signed_url, 'base_url': yzm_url, 'payload': yzm_payload, 'key9_bundle': yzm_key9_bundle},
                'yzm_response': yzm_data,
                'vat_request': {'url': vat_signed_url, 'base_url': vat_url, 'payload': vat_payload, 'key9_bundle': vat_key9_bundle},
                'vat_response_text': vat_text,
                'vat_response': vat_data,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding='utf-8',
    )

    return build_standard_result(
        task,
        verify_time=time.strftime('%Y-%m-%dT%H:%M:%S'),
        verify_channel='invoice-verifier(api_probe)',
        result_code=key1,
        result_status=status_text,
        result_summary=status_text,
        is_success=key1 in ('000', '001'),
        is_voided='未知',
        is_abnormal='否' if key1 in ('000', '001') else '是',
        screenshot_path='',
        raw_response_path=str(raw_path),
        raw_response=vat_text[:5000],
        full_result={
            'yzm_response': yzm_data,
            'vat_response': vat_data,
            'swjg': swjg,
            'vat_payload': vat_payload,
        },
        source_kind='api_probe',
    )


def backend_browser_assist(tasks_file: Path, output_dir: Path, headed: bool, max_retries: int) -> None:
    cmd = [
        sys.executable,
        str(PARSER_DIR / 'verify_browser_assist.py'),
        '--tasks-file',
        str(tasks_file),
        '--max-retries',
        str(max_retries),
    ]
    if headed:
        cmd.append('--headed')
    env = os.environ.copy()
    env.setdefault('VERIFY_RUNTIME_DIR', str(output_dir))
    raise SystemExit(subprocess.call(cmd, cwd=str(PARSER_DIR), env=env))


def save_result_and_index(result: dict[str, Any], results_dir: Path) -> Path:
    task_id = result.get('task_id', 'unknown')
    result_file = results_dir / f'{task_id}.json'
    write_standard_result({}, result, result_file)
    return result_file


def print_summary(result_files: list[Path]) -> None:
    print(f'完成 {len(result_files)} 个 verifier 结果文件:')
    for path in result_files:
        try:
            payload = json.loads(path.read_text(encoding='utf-8'))
            update = extract_update_from_result(payload)
            print(f"- {path.name}: {update.get('verify_status_code', '')} / {update.get('verify_result_summary', '')}")
        except Exception:
            print(f'- {path.name}')


def main() -> None:
    parser = argparse.ArgumentParser(description='invoice-verifier 正式入口')
    parser.add_argument('--tasks-file', default=str(DEFAULT_TASKS_FILE), help='parser 生成的 ready_tasks.json 路径')
    parser.add_argument('--output-dir', default=str(DEFAULT_OUTPUT_DIR), help='输出目录（默认 parser 输出目录）')
    parser.add_argument('--backend', choices=['browser_assist', 'api_probe'], default='browser_assist', help='执行 backend')
    parser.add_argument('--task-id', action='append', default=[], help='仅执行指定 task_id，可重复传参')
    parser.add_argument('--limit', type=int, default=None, help='最多执行多少个任务')
    parser.add_argument('--headed', action='store_true', help='browser_assist 模式下启用有头浏览器')
    parser.add_argument('--max-retries', type=int, default=3, help='browser_assist 模式下验证码最大重试次数')
    args = parser.parse_args()

    tasks_file = Path(args.tasks_file)
    output_dir = Path(args.output_dir)
    results_dir = output_dir / 'verify_results'
    ensure_dirs(results_dir)

    tasks = load_tasks(tasks_file)
    tasks = filter_tasks(tasks, args.task_id, args.limit)
    if not tasks:
        print('没有可执行的 verifier 任务。')
        return

    if args.backend == 'browser_assist':
        temp_tasks_file = results_dir / '__verifier_selected_tasks.json'
        temp_tasks_file.write_text(json.dumps(tasks, ensure_ascii=False, indent=2), encoding='utf-8')
        backend_browser_assist(temp_tasks_file, output_dir, args.headed, args.max_retries)
        return

    result_files: list[Path] = []
    for task in tasks:
        try:
            result = backend_api_probe(task, results_dir)
            result_file = save_result_and_index(result, results_dir)
            result_files.append(result_file)
        except Exception as exc:
            fail_result = build_standard_result(
                task,
                verify_time=time.strftime('%Y-%m-%dT%H:%M:%S'),
                verify_channel='invoice-verifier(api_probe)',
                result_code='',
                result_status='执行异常',
                result_summary=f'执行异常: {exc}',
                is_success=False,
                is_voided='未知',
                is_abnormal='是',
                screenshot_path='',
                raw_response_path='',
                raw_response='',
                full_result={'error': str(exc)},
                source_kind='api_probe',
            )
            result_file = save_result_and_index(fail_result, results_dir)
            result_files.append(result_file)

    print_summary(result_files)


if __name__ == '__main__':
    main()
