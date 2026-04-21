#!/usr/bin/env python3
from __future__ import annotations

import hashlib
from datetime import datetime
from typing import Any

VERIFY_REQUEST_SCHEMA_VERSION = 'invoice-verify-request.v1'
VERIFY_RESULT_SCHEMA_VERSION = 'invoice-verify-result.v1'

VERIFY_PENDING_LABELS = {'', '未查验', '待查验'}

VERIFY_STATUS_CODE_LABELS = {
    'pending': '未查验',
    'verified_match': '查验通过',
    'verified_mismatch': '查验失败',
    'rate_limited': '需人工复核',
    'captcha_error': '需人工复核',
    'manual_review': '需人工复核',
    'system_error': '异常',
    'unsupported': '需人工复核',
}

RESULT_CODE_STATUS_CODE_MAP = {
    '000': 'verified_match',
    '001': 'verified_match',
    '002': 'verified_mismatch',
    '003': 'verified_mismatch',
    '004': 'verified_mismatch',
    '005': 'system_error',
    '006': 'manual_review',
    '007': 'rate_limited',
    '008': 'captcha_error',
    '009': 'manual_review',
    '010': 'rate_limited',
    '011': 'system_error',
}


def now_iso() -> str:
    return datetime.now().isoformat(timespec='seconds')


def build_verify_task_id(*parts: str) -> str:
    raw = '|'.join((part or '').strip() for part in parts)
    digest = hashlib.sha1(raw.encode('utf-8')).hexdigest()[:16]
    return f'ivt_{digest}'


def status_code_from_result_code(result_code: str) -> str:
    return RESULT_CODE_STATUS_CODE_MAP.get((result_code or '').strip(), 'manual_review')


def label_from_status_code(status_code: str, fallback: str = '') -> str:
    if fallback:
        return fallback
    return VERIFY_STATUS_CODE_LABELS.get((status_code or '').strip(), '需人工复核')


def normalize_status_code(status_code: str = '', verify_status: str = '', result_code: str = '') -> str:
    if status_code:
        return status_code
    status = (verify_status or '').strip()
    if result_code:
        return status_code_from_result_code(result_code)
    if status in VERIFY_PENDING_LABELS:
        return 'pending'
    if status == '查验通过':
        return 'verified_match'
    if status == '查验失败':
        return 'verified_mismatch'
    if status == '异常':
        return 'system_error'
    if status == '需人工复核':
        return 'manual_review'
    return 'pending'


def build_match_keys(task: dict[str, Any]) -> dict[str, str]:
    return {
        'task_id': task.get('verify_task_id', '') or task.get('task_id', ''),
        'file_hash': task.get('file_hash', ''),
        'duplicate_key': task.get('duplicate_key', ''),
        'invoice_code': task.get('invoice_code', ''),
        'invoice_number': task.get('invoice_number', '') or task.get('verify_invoice_number', ''),
        'invoice_date': task.get('invoice_date', '') or task.get('verify_invoice_date', ''),
        'total_amount': task.get('total_amount', '') or task.get('verify_total_amount', ''),
    }


def build_verify_request(task: dict[str, Any]) -> dict[str, Any]:
    match_keys = build_match_keys(task)
    return {
        'schema_version': VERIFY_REQUEST_SCHEMA_VERSION,
        'task_id': match_keys['task_id'],
        'generated_at': now_iso(),
        'match_keys': match_keys,
        'invoice': {
            'invoice_code': task.get('invoice_code', ''),
            'invoice_number': task.get('invoice_number', '') or task.get('verify_invoice_number', ''),
            'invoice_date': task.get('invoice_date', '') or task.get('verify_invoice_date', ''),
            'invoice_type': task.get('invoice_type', '') or task.get('verify_invoice_type', ''),
            'total_amount': task.get('total_amount', '') or task.get('verify_total_amount', ''),
            'check_code_last6_or_hint': task.get('check_code_last6_or_hint', '') or task.get('verify_check_code_last6_or_last6_hint', ''),
        },
        'business': {
            'file_name': task.get('file_name', ''),
            'file_path': task.get('file_path', ''),
            'seller_name': task.get('seller_name', ''),
            'seller_tax_no': task.get('seller_tax_no', ''),
            'buyer_name': task.get('buyer_name', ''),
            'buyer_tax_no': task.get('buyer_tax_no', ''),
            'expense_category': task.get('expense_category', ''),
        },
    }


def build_standard_result(
    task: dict[str, Any],
    *,
    verify_time: str,
    verify_channel: str,
    result_code: str,
    result_status: str,
    result_summary: str,
    is_success: bool,
    is_voided: str,
    is_abnormal: str,
    screenshot_path: str,
    raw_response_path: str,
    raw_response: str,
    full_result: dict[str, Any],
    source_kind: str,
    verify_status_code: str = '',
    verify_status: str = '',
) -> dict[str, Any]:
    status_code = normalize_status_code(status_code=verify_status_code, verify_status=verify_status, result_code=result_code)
    status_label = label_from_status_code(status_code, verify_status or ('查验通过' if is_success else ''))
    return {
        'schema_version': VERIFY_RESULT_SCHEMA_VERSION,
        'task_id': task.get('verify_task_id', '') or task.get('task_id', ''),
        'match_keys': build_match_keys(task),
        'verify_time': verify_time,
        'verify_channel': verify_channel,
        'verify_status_code': status_code,
        'verify_status': status_label,
        'verify_result_code': result_code,
        'verify_result_status': result_status,
        'verify_result_summary': result_summary,
        'is_success': is_success,
        'is_voided': is_voided,
        'is_abnormal': is_abnormal,
        'artifacts': {
            'verify_screenshot_path': screenshot_path,
            'verify_raw_result_path': raw_response_path,
        },
        'source': {
            'producer': 'invoice-parser',
            'kind': source_kind,
        },
        'raw_response': raw_response,
        'full_result': full_result,
    }


def extract_update_from_result(payload: dict[str, Any]) -> dict[str, str]:
    task_id = payload.get('task_id', '')
    match_keys = payload.get('match_keys', {}) or {}
    artifacts = payload.get('artifacts', {}) or {}

    if payload.get('schema_version') == VERIFY_RESULT_SCHEMA_VERSION:
        status_code = normalize_status_code(
            status_code=payload.get('verify_status_code', ''),
            verify_status=payload.get('verify_status', ''),
            result_code=payload.get('verify_result_code', ''),
        )
        status_label = label_from_status_code(status_code, payload.get('verify_status', ''))
        return {
            'verify_task_id': task_id or match_keys.get('task_id', ''),
            'file_hash': match_keys.get('file_hash', ''),
            'duplicate_key': match_keys.get('duplicate_key', ''),
            'verify_status_code': status_code,
            'verify_status': status_label,
            'verify_time': payload.get('verify_time', ''),
            'verify_channel': payload.get('verify_channel', ''),
            'verify_result_code': payload.get('verify_result_code', ''),
            'verify_result_summary': payload.get('verify_result_summary', '') or payload.get('verify_result_status', ''),
            'is_voided': payload.get('is_voided', ''),
            'is_abnormal': payload.get('is_abnormal', ''),
            'verify_screenshot_path': artifacts.get('verify_screenshot_path', ''),
            'verify_raw_result_path': artifacts.get('verify_raw_result_path', ''),
        }

    result_code = payload.get('result_code', '')
    status_code = normalize_status_code(
        status_code=payload.get('verify_status_code', ''),
        verify_status=payload.get('verify_status', payload.get('result_status', '')),
        result_code=result_code,
    )
    status_label = label_from_status_code(
        status_code,
        '查验通过' if payload.get('is_success') else ('查验失败' if result_code else payload.get('verify_status', '')),
    )
    return {
        'verify_task_id': task_id or payload.get('verify_task_id', '') or match_keys.get('task_id', ''),
        'file_hash': payload.get('file_hash', '') or match_keys.get('file_hash', ''),
        'duplicate_key': payload.get('duplicate_key', '') or match_keys.get('duplicate_key', ''),
        'verify_status_code': status_code,
        'verify_status': status_label,
        'verify_time': payload.get('verify_time', ''),
        'verify_channel': payload.get('verify_channel', ''),
        'verify_result_code': result_code,
        'verify_result_summary': payload.get('result_summary', '') or payload.get('verify_result_summary', '') or payload.get('result_status', ''),
        'is_voided': payload.get('is_voided', ''),
        'is_abnormal': payload.get('is_abnormal', ''),
        'verify_screenshot_path': payload.get('screenshot_path', '') or artifacts.get('verify_screenshot_path', ''),
        'verify_raw_result_path': payload.get('raw_response_path', '') or artifacts.get('verify_raw_result_path', ''),
    }
