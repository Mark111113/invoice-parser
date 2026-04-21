#!/usr/bin/env python3
import argparse
import os
import hashlib
import html
import json
import re
import subprocess
import xml.etree.ElementTree as ET
import zipfile
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import quote

import pandas as pd

from verify.verify_contract import build_verify_task_id

BASE_DIR = Path(__file__).resolve().parent
DEFAULT_INPUT_DIR = Path(os.environ.get('INVOICE_INPUT_DIR', str(Path.home() / 'invoices')))
DEFAULT_OUTPUT_DIR = Path(os.environ.get('INVOICE_OUTPUT_DIR', str(Path.home() / 'invoices_output')))
CATEGORY_RULES_PATH = BASE_DIR / 'category_rules.json'
INPUT_DIR = DEFAULT_INPUT_DIR
OUTPUT_DIR = DEFAULT_OUTPUT_DIR
VERIFY_STATUS_LEDGER = OUTPUT_DIR / '查验状态台账.csv'
VERIFY_PREPARED_FILE = OUTPUT_DIR / '查验准备清单.csv'
RESULT_JSON = OUTPUT_DIR / '发票解析结果.json'

VERIFY_STATE_FIELDS = [
    'verify_task_id',
    'verify_status_code',
    'verify_result_code',
    'verify_invoice_number',
    'verify_invoice_date',
    'verify_total_amount',
    'verify_check_code_last6_or_last6_hint',
    'verify_invoice_type',
    'verify_required_fields_status',
    'verify_missing_fields',
    'verify_status',
    'verify_time',
    'verify_channel',
    'verify_result_summary',
    'is_voided',
    'is_abnormal',
    'verify_screenshot_path',
    'verify_raw_result_path',
]


@dataclass
class InvoiceItem:
    item_name: str = ''
    spec_model: str = ''
    unit: str = ''
    quantity: str = ''
    unit_price: str = ''
    amount: str = ''
    tax_rate: str = ''
    tax_amount: str = ''


@dataclass
class InvoiceRecord:
    file_name: str
    file_path: str
    file_hash: str
    file_mtime: str
    parse_status: str = 'parsed'
    review_needed: bool = False
    review_reason: str = ''
    invoice_type: str = ''
    invoice_code: str = ''
    invoice_number: str = ''
    invoice_date: str = ''
    buyer_name: str = ''
    buyer_tax_no: str = ''
    seller_name: str = ''
    seller_tax_no: str = ''
    item_name: str = ''
    spec_model: str = ''
    unit: str = ''
    quantity: str = ''
    unit_price: str = ''
    amount_ex_tax: str = ''
    tax_rate: str = ''
    tax_amount: str = ''
    total_amount: str = ''
    drawer: str = ''
    remarks: str = ''
    expense_category: str = ''
    reimbursement_month: str = ''
    duplicate_key: str = ''
    duplicate_count: int = 1
    text_extract_method: str = 'pdftotext'
    raw_text_excerpt: str = ''
    verify_task_id: str = ''
    verify_status_code: str = 'pending'
    verify_result_code: str = ''
    verify_invoice_number: str = ''
    verify_invoice_date: str = ''
    verify_total_amount: str = ''
    verify_check_code_last6_or_last6_hint: str = ''
    verify_invoice_type: str = ''
    verify_required_fields_status: str = 'missing'
    verify_missing_fields: str = ''
    verify_status: str = '未查验'
    verify_time: str = ''
    verify_channel: str = ''
    verify_result_summary: str = ''
    is_voided: str = '未知'
    is_abnormal: str = '未知'
    verify_screenshot_path: str = ''
    verify_raw_result_path: str = ''
    items: List[InvoiceItem] = field(default_factory=list)


def normalize_entity_name(name: str) -> str:
    name = re.sub(r'\s+', ' ', (name or '').strip())
    return name or '待确认购方'


def sanitize_output_component(name: str) -> str:
    name = normalize_entity_name(name)
    name = re.sub(r'[\\/:*?"<>|]+', '_', name)
    name = name.strip(' ._') or '待确认购方'
    return name[:80]


def entity_group_key(record: InvoiceRecord | dict) -> str:
    buyer_tax_no = (record.get('buyer_tax_no', '') if isinstance(record, dict) else record.buyer_tax_no).strip()
    buyer_name = normalize_entity_name(record.get('buyer_name', '') if isinstance(record, dict) else record.buyer_name)
    if buyer_tax_no:
        return f'tax:{buyer_tax_no.upper()}'
    if buyer_name and buyer_name != '待确认购方':
        return f'name:{buyer_name}'
    return 'unknown'


def entity_display_name(record: InvoiceRecord | dict) -> str:
    buyer_name = normalize_entity_name(record.get('buyer_name', '') if isinstance(record, dict) else record.buyer_name)
    return buyer_name or '待确认购方'


def read_entity_meta(output_dir: Path) -> dict:
    meta_path = output_dir / 'entity_meta.json'
    if not meta_path.exists():
        return {}
    try:
        return json.loads(meta_path.read_text(encoding='utf-8'))
    except Exception:
        return {}


def resolve_output_dir_for_record(record: InvoiceRecord | dict, *, output_parent: Path | None = None) -> Path:
    output_parent = output_parent or DEFAULT_OUTPUT_DIR.parent
    buyer_name = entity_display_name(record)
    buyer_tax_no = (record.get('buyer_tax_no', '') if isinstance(record, dict) else record.buyer_tax_no).strip()
    preferred = output_parent / f'发票_解析结果_{sanitize_output_component(buyer_name)}'
    if not preferred.exists():
        return preferred
    meta = read_entity_meta(preferred)
    if not meta:
        return preferred
    meta_tax = str(meta.get('buyer_tax_no', '')).strip()
    meta_name = normalize_entity_name(str(meta.get('buyer_name', '')))
    if (buyer_tax_no and meta_tax == buyer_tax_no) or (not buyer_tax_no and meta_name == buyer_name):
        return preferred
    if buyer_tax_no:
        alt = output_parent / f'发票_解析结果_{sanitize_output_component(buyer_name)}_{sanitize_output_component(buyer_tax_no)}'
        return alt
    return preferred


def write_entity_meta(output_dir: Path, sample: InvoiceRecord | dict) -> None:
    buyer_name = entity_display_name(sample)
    buyer_tax_no = (sample.get('buyer_tax_no', '') if isinstance(sample, dict) else sample.buyer_tax_no).strip()
    meta = {
        'buyer_name': buyer_name,
        'buyer_tax_no': buyer_tax_no,
        'group_key': entity_group_key(sample),
        'updated_at': datetime.now().isoformat(timespec='seconds'),
    }
    meta_path = output_dir / 'entity_meta.json'
    if meta_path.exists():
        try:
            existing = json.loads(meta_path.read_text(encoding='utf-8'))
            meta['created_at'] = existing.get('created_at', meta['updated_at'])
        except Exception:
            meta['created_at'] = meta['updated_at']
    else:
        meta['created_at'] = meta['updated_at']
    meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding='utf-8')


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open('rb') as f:
        for chunk in iter(lambda: f.read(8192), b''):
            h.update(chunk)
    return h.hexdigest()


def extract_text_pdftotext(path: Path) -> str:
    try:
        return subprocess.check_output(['pdftotext', str(path), '-'], stderr=subprocess.DEVNULL, text=True)
    except Exception:
        return ''


def normalize_text(text: str) -> str:
    text = text.replace('\r', '\n').replace('\u3000', ' ')
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def search(pattern: str, text: str, flags=0, group: int = 1) -> str:
    m = re.search(pattern, text, flags)
    return m.group(group).strip() if m else ''


def clean_money(v: str) -> str:
    return v.replace('¥', '').replace(',', '').replace(' ', '') if v else ''


def normalize_invoice_type(raw_type: str, full_text: str = '') -> str:
    text = f'{raw_type} {full_text}'
    if '专用发票' in text:
        return '电子发票-增值税专用发票' if '电子发票' in text else '增值税专用发票'
    if '普通发票' in text:
        return '电子发票-普通发票' if '电子发票' in text else '普通发票'
    if '电子发票' in text:
        return '电子发票'
    return raw_type or '未知票种'


def parse_buyer_seller(text: str):
    buyer_name = search(r'购\s*名称[:：]\s*([^\n]+)', text)
    buyer_tax = search(r'购[\s\S]{0,80}?统一社会信用代码/纳税人识别号[:：]\s*([A-Z0-9]+)', text)
    seller_name = search(r'销\s*名称[:：]\s*([^\n]+)', text)
    seller_tax = search(r'销[\s\S]{0,80}?统一社会信用代码/纳税人识别号[:：]\s*([A-Z0-9]+)', text)

    if not buyer_name:
        buyer_name = search(r'买\s*名称[:：]\s*([^\n]+)', text)
    if not buyer_tax:
        buyer_tax = search(r'买[\s\S]{0,80}?统一社会信用代码/纳税人识别号[:：]\s*([A-Z0-9]+)', text)
    if not seller_name:
        seller_name = search(r'售\s*名称[:：]\s*([^\n]+)', text)
    if not seller_tax:
        seller_tax = search(r'售[\s\S]{0,80}?统一社会信用代码/纳税人识别号[:：]\s*([A-Z0-9]+)', text)

    # More tolerant patterns for vertically split / spaced labels like “销\n售 名 称:” and “购\n买 名 称:”
    if not buyer_name:
        buyer_name = search(r'购\s*买\s*名\s*称[:：]\s*([^\n]+)', text)
    if not seller_name:
        seller_name = search(r'销\s*售\s*名\s*称[:：]\s*([^\n]+)', text)
    if not buyer_tax:
        buyer_tax = search(r'购\s*买[\s\S]{0,120}?统一社会信用代码/纳税人识别号[:：]\s*([A-Z0-9]+)', text)
    if not seller_tax:
        seller_tax = search(r'销\s*售[\s\S]{0,120}?统一社会信用代码/纳税人识别号[:：]\s*([A-Z0-9]+)', text)

    if not buyer_name or not seller_name:
        compact = re.sub(r'\s+', ' ', text)
        buyer_name = buyer_name or search(r'购买?\s*名\s*称[:：]\s*(.*?)\s+(?:方\s*)?统一社会信用代码', compact)
        seller_name = seller_name or search(r'销售?\s*名\s*称[:：]\s*(.*?)\s+(?:方\s*)?统一社会信用代码', compact)
        buyer_tax = buyer_tax or search(r'购买?\s*名\s*称[:：].*?统一社会信用代码/纳税人识别号[:：]\s*([A-Z0-9]+)', compact)
        seller_tax = seller_tax or search(r'销售?\s*名\s*称[:：].*?统一社会信用代码/纳税人识别号[:：]\s*([A-Z0-9]+)', compact)

    return buyer_name, buyer_tax, seller_name, seller_tax


def load_category_rules() -> list:
    try:
        with CATEGORY_RULES_PATH.open('r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return []


def infer_category(item_name: str, seller_name: str, file_name: str) -> str:
    hay = f'{item_name} {seller_name} {file_name}'
    for rule in load_category_rules():
        category = rule.get('category', '待分类')
        keys = rule.get('keywords', [])
        if any(k in hay for k in keys):
            return category
    return '待分类'


def _extract_block_lines(text: str, start_marker: str, stop_markers: list[str]) -> str:
    lines = [line.strip() for line in text.splitlines()]
    out = []
    collecting = False
    for line in lines:
        if not collecting and start_marker in line:
            collecting = True
            continue
        if collecting:
            if not line:
                if out:
                    break
                continue
            if any(marker in line for marker in stop_markers):
                break
            out.append(line)
    return ' '.join(out).strip()


def parse_first_item(text: str) -> InvoiceItem:
    item_name = _extract_block_lines(text, '项目名称', ['规格型号', '单 位', '单价', '税率/征收率', '开票人', '发票号码', '购', '销'])
    if not item_name:
        item_name = search(r'项目名称\s*\n([\s\S]{0,200}?)\n\s*(?:合|规格型号|开票人|发票号码)', text)
    if item_name.startswith('规格型号'):
        item_name = item_name.replace('规格型号', '').strip()
    item_name = re.sub(r'\s+', ' ', item_name).strip()

    spec = _extract_block_lines(text, '规格型号', ['单 位', '单价', '金 额', '税率/征收率', '开票人', '购', '销'])
    unit = search(r'单\s*位\s*\n([^\n]+)', text)
    quantity = search(r'(?:数\s*量|量)\s*\n?\s*(-?\d+(?:\.\d+)?)', text)
    unit_price = search(r'单\s*价\s*\n?\s*(-?\d+(?:\.\d+)?)', text)
    amount = search(r'金\s*额\s*\n?\s*(-?\d+(?:\.\d+)?)', text)
    tax_rate = search(r'税率/征收率\s*\n?\s*([0-9]+%)', text)
    tax_amount = search(r'税\s*额\s*\n?\s*¥?\s*(-?\d+(?:\.\d+)?)', text)

    return InvoiceItem(
        item_name=item_name,
        spec_model=spec.strip(),
        unit=unit.strip(),
        quantity=quantity,
        unit_price=unit_price,
        amount=amount,
        tax_rate=tax_rate,
        tax_amount=tax_amount,
    )


def _xml_text(elem: ET.Element, path: str) -> str:
    """Safely get text from nested XML path like 'SellerInformation/SellerName'."""
    node = elem.find(path)
    return (node.text or '').strip() if node is not None else ''


def _xml_float(elem: ET.Element, path: str) -> str:
    """Get numeric text, normalized."""
    t = _xml_text(elem, path)
    if not t:
        return ''
    try:
        v = float(t)
        if v == int(v):
            return str(int(v))
        return f'{v:.2f}'
    except (ValueError, TypeError):
        return t


def _xml_tax_rate(elem: ET.Element, path: str) -> str:
    t = _xml_text(elem, path)
    if not t:
        return ''
    try:
        v = float(t)
        if 0 < v < 1:
            return f'{int(round(v * 100))}%'
        if v <= 100 and float(v).is_integer():
            return f'{int(v)}%'
        return t
    except (ValueError, TypeError):
        return t


def parse_invoice_xml(path: Path) -> InvoiceRecord:
    """Parse an EInvoice XML file (standard format from tax bureau / JD / etc.)."""
    tree = ET.parse(str(path))
    root = tree.getroot()

    header = root.find('Header')
    if header is None:
        header = root
    data = root.find('EInvoiceData')
    if data is None:
        data = root
    tax_info = root.find('TaxSupervisionInfo')
    if tax_info is None:
        tax_info = root

    # Header labels
    inherent = header.find('InherentLabel')
    inv_type_code = ''
    inv_type_name = ''
    general_special = ''
    if inherent is not None:
        inv_type_code = _xml_text(inherent, 'EInvoiceType/LabelCode')
        inv_type_name = _xml_text(inherent, 'EInvoiceType/LabelName')
        general_special = _xml_text(inherent, 'GeneralOrSpecialVAT/LabelCode')

    # Map type codes
    if inv_type_code == '01' and general_special == '01':
        invoice_type = '电子发票-增值税专用发票'
    elif inv_type_code == '01' and general_special == '02':
        invoice_type = '电子发票-普通发票'
    elif general_special == '01':
        invoice_type = '增值税专用发票'
    elif general_special == '02':
        invoice_type = '普通发票'
    else:
        invoice_type = inv_type_name or '电子发票'

    # Invoice number / date
    inv_num = _xml_text(tax_info, 'InvoiceNumber')
    issue_time = _xml_text(tax_info, 'IssueTime') or _xml_text(data, 'BasicInformation/RequestTime')
    inv_date = ''
    if issue_time:
        try:
            dt = datetime.strptime(issue_time[:19], '%Y-%m-%d %H:%M:%S')
            inv_date = dt.strftime('%Y年%m月%d日')
        except ValueError:
            inv_date = issue_time[:10]

    # Seller
    seller_name = _xml_text(data, 'SellerInformation/SellerName')
    seller_tax = _xml_text(data, 'SellerInformation/SellerIdNum')
    seller_addr = _xml_text(data, 'SellerInformation/SellerAddr')
    seller_tel = _xml_text(data, 'SellerInformation/SellerTelNum')
    seller_bank = _xml_text(data, 'SellerInformation/SellerBankName')
    seller_account = _xml_text(data, 'SellerInformation/SellerBankAccNum')

    # Buyer
    buyer_name = _xml_text(data, 'BuyerInformation/BuyerName')
    buyer_tax = _xml_text(data, 'BuyerInformation/BuyerIdNum')

    # Amounts
    amount_ex_tax = _xml_float(data, 'BasicInformation/TotalAmWithoutTax')
    tax_amount = _xml_float(data, 'BasicInformation/TotalTaxAm')
    total_amount = _xml_float(data, 'BasicInformation/TotalTax-includedAmount')

    # Drawer
    drawer = _xml_text(data, 'BasicInformation/Drawer')

    # Remarks
    remarks = _xml_text(data, 'AdditionalInformation/Remark')

    # Items (support multiple)
    items: list[InvoiceItem] = []
    item_elems = data.findall('.//IssuItemInformation')
    if not item_elems:
        item_elems = data.findall('.//ItemInformation')
    for item_el in item_elems:
        items.append(InvoiceItem(
            item_name=_xml_text(item_el, 'ItemName') or _xml_text(item_el, 'GoodsName'),
            spec_model=_xml_text(item_el, 'SpecMod') or _xml_text(item_el, 'Specification'),
            unit=_xml_text(item_el, 'MeaUnits') or _xml_text(item_el, 'Unit'),
            quantity=_xml_float(item_el, 'Quantity'),
            unit_price=_xml_float(item_el, 'UnPrice'),
            amount=_xml_float(item_el, 'Amount'),
            tax_rate=_xml_tax_rate(item_el, 'TaxRate'),
            tax_amount=_xml_float(item_el, 'ComTaxAm'),
        ))

    # Invoice code: for 20-digit numbers, first 12 digits = code
    invoice_code = ''
    if inv_num.isdigit() and len(inv_num) == 20:
        invoice_code = inv_num[:12]

    # Deduplicate item fields
    item_name = items[0].item_name if items else ''
    spec_model = items[0].spec_model if items else ''
    unit = items[0].unit if items else ''
    quantity = items[0].quantity if items else ''
    unit_price = items[0].unit_price if items else ''

    # Expense category
    expense_category = infer_category(item_name, seller_name, path.name)

    # Reimbursement month
    reimbursement_month = ''
    if inv_date:
        m = re.match(r'(\d{4})年(\d{2})月', inv_date)
        if m:
            reimbursement_month = f'{m.group(1)}-{m.group(2)}'

    # Duplicate key
    duplicate_key = '|'.join([inv_num, inv_date, seller_tax, total_amount])

    rec = InvoiceRecord(
        file_name=path.name,
        file_path=str(path),
        file_hash=sha256_file(path),
        file_mtime=datetime.fromtimestamp(path.stat().st_mtime).isoformat(timespec='seconds'),
        parse_status='parsed',
        review_needed=False,
        review_reason='',
        invoice_type=invoice_type,
        invoice_code=invoice_code,
        invoice_number=inv_num,
        invoice_date=inv_date,
        buyer_name=buyer_name,
        buyer_tax_no=buyer_tax,
        seller_name=seller_name,
        seller_tax_no=seller_tax,
        item_name=item_name,
        spec_model=spec_model,
        unit=unit,
        quantity=quantity,
        unit_price=unit_price,
        amount_ex_tax=amount_ex_tax,
        tax_rate=items[0].tax_rate if items else '',
        tax_amount=tax_amount,
        total_amount=total_amount,
        drawer=drawer,
        remarks=remarks,
        expense_category=expense_category,
        reimbursement_month=reimbursement_month,
        duplicate_key=duplicate_key,
        duplicate_count=1,
        text_extract_method='xml',
        raw_text_excerpt=f'[XML] {seller_name} → {buyer_name} | {invoice_type} | ¥{total_amount}',
        items=items,
    )

    # Check missing required fields
    missing = []
    for label, val in [('发票号码', inv_num), ('开票日期', inv_date), ('销售方名称', seller_name),
                       ('购买方名称', buyer_name), ('价税合计', total_amount)]:
        if not val:
            missing.append(label)
    if missing:
        rec.review_needed = True
        rec.review_reason = '缺少关键字段: ' + '、'.join(missing)
        rec.parse_status = 'partial'

    build_verify_fields(rec, '')
    return rec


def _extract_text_from_ofd(path: Path) -> str:
    """Extract text content from an OFD file (ZIP containing XML)."""
    try:
        with zipfile.ZipFile(str(path), 'r') as zf:
            text_parts = []
            for name in zf.namelist():
                if name.endswith('/Content.xml') or name.endswith('Content.xml'):
                    try:
                        content = zf.read(name)
                        root = ET.fromstring(content)
                        ns = {'ofd': 'http://www.ofdspec.org/2016'}
                        # Try with namespace first, then without
                        for text_obj in (root.findall('.//ofd:TextObject', ns) or
                                         root.findall('.//ofd:TextCode', ns) or
                                         root.findall('.//TextObject') or
                                         root.findall('.//TextCode')):
                            t = text_obj.text or ''
                            if t.strip():
                                text_parts.append(t.strip())
                    except Exception:
                        continue
            return '\n'.join(text_parts)
    except Exception:
        return ''


def parse_invoice_ofd(path: Path) -> InvoiceRecord:
    """Parse an OFD invoice by extracting text then using existing PDF regex logic."""
    text = normalize_text(_extract_text_from_ofd(path))
    if not text:
        return InvoiceRecord(
            file_name=path.name, file_path=str(path),
            file_hash=sha256_file(path),
            file_mtime=datetime.fromtimestamp(path.stat().st_mtime).isoformat(timespec='seconds'),
            parse_status='failed', review_needed=True,
            review_reason='OFD文本提取失败', text_extract_method='ofd',
        )

    # Reuse the full PDF parse pipeline on extracted text
    rec = InvoiceRecord(
        file_name=path.name, file_path=str(path),
        file_hash=sha256_file(path),
        file_mtime=datetime.fromtimestamp(path.stat().st_mtime).isoformat(timespec='seconds'),
        raw_text_excerpt=text[:500], text_extract_method='ofd',
    )
    rec.invoice_type = search(r'(电子发票（[^\n]+）)', text) or search(r'(电子发票\([^\n]+\))', text)
    rec.invoice_number = search(r'发票号码[:：]\s*([0-9]{8,})', text)
    rec.invoice_date = search(r'开票日期[:：]\s*([0-9]{4}年[0-9]{2}月[0-9]{2}日)', text)
    rec.drawer = search(r'开票人[:：]\s*([^\n]+)', text)
    rec.total_amount = clean_money(search(r'[（(]小写[）)]\s*¥\s*([0-9.,]+)', text))
    rec.tax_amount = clean_money(search(r'税\s*额[\s\S]{0,30}?¥\s*(-?[0-9.,]+)', text))
    rec.amount_ex_tax = clean_money(search(r'金\s*额\s*\n?\s*(-?[0-9.,]+)', text))
    rec.tax_rate = search(r'税率/征收率\s*\n?\s*([0-9]+%)', text)
    rec.remarks = search(r'备\s*注[:：]\s*([^\n]+)', text)

    buyer_name, buyer_tax, seller_name, seller_tax = parse_buyer_seller(text)
    rec.buyer_name, rec.buyer_tax_no = buyer_name, buyer_tax
    rec.seller_name, rec.seller_tax_no = seller_name, seller_tax

    item = parse_first_item(text)
    rec.items = [item]
    rec.item_name = item.item_name
    rec.spec_model = item.spec_model
    rec.unit = item.unit
    rec.quantity = item.quantity
    rec.unit_price = item.unit_price
    if not rec.amount_ex_tax:
        rec.amount_ex_tax = item.amount
    if not rec.tax_rate:
        rec.tax_rate = item.tax_rate
    if not rec.tax_amount:
        rec.tax_amount = item.tax_amount

    rec.expense_category = infer_category(rec.item_name, rec.seller_name, rec.file_name)
    if rec.invoice_date:
        m = re.match(r'(\d{4})年(\d{2})月', rec.invoice_date)
        if m:
            rec.reimbursement_month = f'{m.group(1)}-{m.group(2)}'
    rec.duplicate_key = '|'.join([rec.invoice_number or '', rec.invoice_date or '',
                                   rec.seller_tax_no or '', rec.total_amount or ''])

    if not text:
        rec.parse_status = 'failed'
        rec.review_needed = True
        rec.review_reason = '文本提取失败'
    else:
        missing = []
        for label, val in [('发票号码', rec.invoice_number), ('开票日期', rec.invoice_date),
                           ('销售方名称', rec.seller_name), ('购买方名称', rec.buyer_name),
                           ('价税合计', rec.total_amount)]:
            if not val:
                missing.append(label)
        if missing:
            rec.review_needed = True
            rec.review_reason = '缺少关键字段: ' + '、'.join(missing)
            rec.parse_status = 'partial'

    build_verify_fields(rec, text)
    return rec


def infer_invoice_code(rec: InvoiceRecord, text: str) -> str:
    code = search(r'发票代码[:：]\s*([0-9]{10,12})', text)
    if code:
        return code
    if rec.invoice_number.isdigit() and len(rec.invoice_number) == 20:
        return rec.invoice_number[:12]
    return ''


def build_verify_fields(rec: InvoiceRecord, text: str) -> None:
    rec.invoice_code = infer_invoice_code(rec, text)
    rec.verify_invoice_number = rec.invoice_number
    rec.verify_invoice_date = rec.invoice_date
    rec.verify_total_amount = rec.total_amount
    rec.verify_invoice_type = normalize_invoice_type(rec.invoice_type, text)
    check_code = search(r'校验码[:：]\s*([0-9*]{6,})', text)
    rec.verify_check_code_last6_or_last6_hint = check_code[-6:] if check_code else ''
    rec.verify_task_id = build_verify_task_id(
        rec.file_hash,
        rec.duplicate_key,
        rec.verify_invoice_number,
        rec.verify_invoice_date,
        rec.verify_total_amount,
    )

    missing = []
    for label, value in [
        ('发票号码', rec.verify_invoice_number),
        ('开票日期', rec.verify_invoice_date),
        ('价税合计', rec.verify_total_amount),
        ('票种', rec.verify_invoice_type if rec.verify_invoice_type != '未知票种' else ''),
    ]:
        if not value:
            missing.append(label)

    rec.verify_missing_fields = '、'.join(missing)
    if len(missing) == 0:
        rec.verify_required_fields_status = 'ready'
    elif len(missing) < 4:
        rec.verify_required_fields_status = 'partial'
    else:
        rec.verify_required_fields_status = 'missing'


def set_parser_output_dir(path: Path) -> None:
    global OUTPUT_DIR, VERIFY_STATUS_LEDGER, VERIFY_PREPARED_FILE, RESULT_JSON
    OUTPUT_DIR = path
    VERIFY_STATUS_LEDGER = OUTPUT_DIR / '查验状态台账.csv'
    VERIFY_PREPARED_FILE = OUTPUT_DIR / '查验准备清单.csv'
    RESULT_JSON = OUTPUT_DIR / '发票解析结果.json'


def load_existing_verify_state() -> Dict[str, dict]:
    state = {}
    if not VERIFY_STATUS_LEDGER.exists():
        return state
    try:
        df = pd.read_csv(VERIFY_STATUS_LEDGER, dtype=str).fillna('')
    except Exception:
        return state

    for _, row in df.iterrows():
        payload = {field: row.get(field, '') for field in VERIFY_STATE_FIELDS}
        if row.get('file_hash'):
            state[f"hash:{row.get('file_hash')}"] = payload
        if row.get('duplicate_key'):
            state[f"dup:{row.get('duplicate_key')}"] = payload
    return state


def apply_existing_verify_state(rec: InvoiceRecord, state: Dict[str, dict]) -> None:
    payload = state.get(f'hash:{rec.file_hash}') or state.get(f'dup:{rec.duplicate_key}')
    if not payload:
        return
    for field in VERIFY_STATE_FIELDS:
        value = payload.get(field, '')
        if value != '':
            setattr(rec, field, value)


def parse_invoice(path: Path) -> InvoiceRecord:
    suffix = path.suffix.lower()
    if suffix == '.xml':
        return parse_invoice_xml(path)
    if suffix == '.ofd':
        return parse_invoice_ofd(path)

    text = normalize_text(extract_text_pdftotext(path))
    rec = InvoiceRecord(
        file_name=path.name,
        file_path=str(path),
        file_hash=sha256_file(path),
        file_mtime=datetime.fromtimestamp(path.stat().st_mtime).isoformat(timespec='seconds'),
        raw_text_excerpt=text[:500],
    )

    if not text:
        rec.parse_status = 'failed'
        rec.review_needed = True
        rec.review_reason = '文本提取失败'
        build_verify_fields(rec, text)
        return rec

    rec.invoice_type = search(r'(电子发票（[^\n]+）)', text) or search(r'(电子发票\([^\n]+\))', text)
    rec.invoice_number = search(r'发票号码[:：]\s*([0-9]{8,})', text)
    rec.invoice_date = search(r'开票日期[:：]\s*([0-9]{4}年[0-9]{2}月[0-9]{2}日)', text)
    rec.drawer = search(r'开票人[:：]\s*([^\n]+)', text)
    rec.total_amount = clean_money(search(r'[（(]小写[）)]\s*¥\s*([0-9.,]+)', text))
    rec.tax_amount = clean_money(search(r'税\s*额[\s\S]{0,30}?¥\s*(-?[0-9.,]+)', text))
    rec.amount_ex_tax = clean_money(search(r'金\s*额\s*\n?\s*(-?[0-9.,]+)', text))
    rec.tax_rate = search(r'税率/征收率\s*\n?\s*([0-9]+%)', text)

    buyer_name, buyer_tax, seller_name, seller_tax = parse_buyer_seller(text)
    rec.buyer_name = buyer_name
    rec.buyer_tax_no = buyer_tax
    rec.seller_name = seller_name
    rec.seller_tax_no = seller_tax

    item = parse_first_item(text)
    rec.items = [item]
    rec.item_name = item.item_name
    rec.spec_model = item.spec_model
    rec.unit = item.unit
    rec.quantity = item.quantity
    rec.unit_price = item.unit_price
    if not rec.amount_ex_tax:
        rec.amount_ex_tax = item.amount
    if not rec.tax_rate:
        rec.tax_rate = item.tax_rate
    if not rec.tax_amount:
        rec.tax_amount = item.tax_amount

    rec.expense_category = infer_category(rec.item_name, rec.seller_name, rec.file_name)
    if rec.invoice_date:
        m = re.match(r'(\d{4})年(\d{2})月', rec.invoice_date)
        if m:
            rec.reimbursement_month = f'{m.group(1)}-{m.group(2)}'

    rec.duplicate_key = '|'.join([
        rec.invoice_number or '',
        rec.invoice_date or '',
        rec.seller_tax_no or '',
        rec.total_amount or '',
    ])

    missing = []
    for key, val in [
        ('发票号码', rec.invoice_number),
        ('开票日期', rec.invoice_date),
        ('销售方名称', rec.seller_name),
        ('购买方名称', rec.buyer_name),
        ('价税合计', rec.total_amount),
    ]:
        if not val:
            missing.append(key)
    if missing:
        rec.review_needed = True
        rec.review_reason = '缺少关键字段: ' + '、'.join(missing)
        rec.parse_status = 'partial'

    build_verify_fields(rec, text)
    return rec


def build_outputs(records: List[InvoiceRecord]) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    (OUTPUT_DIR / 'verify_tasks').mkdir(parents=True, exist_ok=True)
    (OUTPUT_DIR / 'verify_results').mkdir(parents=True, exist_ok=True)
    if records:
        write_entity_meta(OUTPUT_DIR, records[0])

    dup_counts = {}
    for r in records:
        dup_counts[r.duplicate_key] = dup_counts.get(r.duplicate_key, 0) + 1
    for r in records:
        r.duplicate_count = dup_counts.get(r.duplicate_key, 1)
        if r.duplicate_count > 1 and r.duplicate_key.strip('|'):
            r.review_needed = True
            r.review_reason = (r.review_reason + '; ' if r.review_reason else '') + '疑似重复发票'

    main_rows = []
    detail_rows = []
    failed_rows = []
    verify_prepare_rows = []
    verify_status_rows = []

    for r in records:
        row = asdict(r)
        row['items'] = json.dumps([asdict(i) for i in r.items], ensure_ascii=False)
        main_rows.append(row)

        for idx, item in enumerate(r.items, start=1):
            detail_rows.append({
                'file_name': r.file_name,
                'invoice_number': r.invoice_number,
                'item_index': idx,
                **asdict(item),
            })

        if r.review_needed or r.parse_status != 'parsed':
            failed_rows.append({
                'file_name': r.file_name,
                'invoice_number': r.invoice_number,
                'parse_status': r.parse_status,
                'review_reason': r.review_reason,
                'file_path': r.file_path,
            })

        verify_prepare_rows.append({
            'file_name': r.file_name,
            'file_path': r.file_path,
            'file_hash': r.file_hash,
            'duplicate_key': r.duplicate_key,
            'verify_invoice_number': r.verify_invoice_number,
            'verify_invoice_date': r.verify_invoice_date,
            'verify_total_amount': r.verify_total_amount,
            'verify_check_code_last6_or_last6_hint': r.verify_check_code_last6_or_last6_hint,
            'verify_invoice_type': r.verify_invoice_type,
            'verify_required_fields_status': r.verify_required_fields_status,
            'verify_missing_fields': r.verify_missing_fields,
        })

        verify_status_rows.append({
            'file_name': r.file_name,
            'file_path': r.file_path,
            'file_hash': r.file_hash,
            'duplicate_key': r.duplicate_key,
            'invoice_number': r.invoice_number,
            'invoice_date': r.invoice_date,
            'seller_name': r.seller_name,
            'total_amount': r.total_amount,
            **{field: getattr(r, field) for field in VERIFY_STATE_FIELDS},
        })

    df_main = pd.DataFrame(main_rows)
    df_detail = pd.DataFrame(detail_rows, columns=['file_name', 'invoice_number', 'item_index', 'item_name', 'spec_model', 'unit', 'quantity', 'unit_price', 'amount', 'tax_rate', 'tax_amount'])
    df_failed = pd.DataFrame(failed_rows, columns=['file_name', 'invoice_number', 'parse_status', 'review_reason', 'file_path'])
    df_verify_prepare = pd.DataFrame(verify_prepare_rows)
    df_verify_status = pd.DataFrame(verify_status_rows)

    df_dups = df_main[df_main['duplicate_count'] > 1][[
        'file_name', 'invoice_number', 'invoice_date', 'seller_name', 'seller_tax_no', 'total_amount', 'duplicate_key', 'duplicate_count'
    ]] if not df_main.empty else pd.DataFrame(columns=['file_name', 'invoice_number', 'invoice_date', 'seller_name', 'seller_tax_no', 'total_amount', 'duplicate_key', 'duplicate_count'])

    reimbursement_df = pd.DataFrame([
        {
            '报销月份': r.reimbursement_month,
            '费用分类': r.expense_category,
            '发票号码': r.invoice_number,
            '开票日期': r.invoice_date,
            '销方名称': r.seller_name,
            '购方名称': r.buyer_name,
            '项目名称': r.item_name,
            '不含税金额': r.amount_ex_tax,
            '税额': r.tax_amount,
            '价税合计': r.total_amount,
            '税率': r.tax_rate,
            '查验准备状态': r.verify_required_fields_status,
            '查验状态': r.verify_status,
            '原文件路径': r.file_path,
            '解析状态': r.parse_status,
            '待复核': '是' if r.review_needed else '否',
            '复核原因': r.review_reason,
        }
        for r in records
    ])

    summary_df = pd.DataFrame([
        {'指标': '发票总数', '值': len(records)},
        {'指标': '解析成功数', '值': sum(1 for r in records if r.parse_status == 'parsed')},
        {'指标': '待复核数', '值': sum(1 for r in records if r.review_needed)},
        {'指标': '价税合计总额', '值': round(sum(float(r.total_amount) if r.total_amount else 0 for r in records), 2)},
    ])

    category_summary_df = pd.DataFrame([
        {
            '费用分类': cat,
            '票数': sum(1 for r in records if r.expense_category == cat),
            '价税合计': round(sum(float(r.total_amount) if r.total_amount and r.expense_category == cat else 0 for r in records), 2),
        }
        for cat in sorted({r.expense_category for r in records})
    ])

    verify_prepare_summary_df = pd.DataFrame([
        {
            '查验准备状态': status,
            '票数': sum(1 for r in records if r.verify_required_fields_status == status),
        }
        for status in ['ready', 'partial', 'missing']
    ])

    verify_status_summary_df = pd.DataFrame([
        {
            '查验状态': status,
            '票数': sum(1 for r in records if r.verify_status == status),
        }
        for status in sorted({r.verify_status for r in records})
    ])

    df_main.to_csv(OUTPUT_DIR / '发票汇总.csv', index=False, encoding='utf-8-sig')
    df_detail.to_csv(OUTPUT_DIR / '发票明细.csv', index=False, encoding='utf-8-sig')
    df_failed.to_csv(OUTPUT_DIR / '解析失败清单.csv', index=False, encoding='utf-8-sig')
    reimbursement_df.to_csv(OUTPUT_DIR / '报销导入模板.csv', index=False, encoding='utf-8-sig')
    summary_df.to_csv(OUTPUT_DIR / '汇总统计.csv', index=False, encoding='utf-8-sig')
    category_summary_df.to_csv(OUTPUT_DIR / '分类统计.csv', index=False, encoding='utf-8-sig')
    df_verify_prepare.to_csv(VERIFY_PREPARED_FILE, index=False, encoding='utf-8-sig')
    df_verify_status.to_csv(VERIFY_STATUS_LEDGER, index=False, encoding='utf-8-sig')

    with RESULT_JSON.open('w', encoding='utf-8') as f:
        json.dump([asdict(r) for r in records], f, ensure_ascii=False, indent=2)

    with pd.ExcelWriter(OUTPUT_DIR / '发票汇总.xlsx', engine='openpyxl') as writer:
        df_main.to_excel(writer, sheet_name='发票主表', index=False)
        df_detail.to_excel(writer, sheet_name='发票明细', index=False)
        reimbursement_df.to_excel(writer, sheet_name='报销导入模板', index=False)
        summary_df.to_excel(writer, sheet_name='汇总统计', index=False)
        category_summary_df.to_excel(writer, sheet_name='分类统计', index=False)
        verify_prepare_summary_df.to_excel(writer, sheet_name='查验准备统计', index=False)
        verify_status_summary_df.to_excel(writer, sheet_name='查验状态统计', index=False)
        df_verify_prepare.to_excel(writer, sheet_name='查验准备清单', index=False)
        df_verify_status.to_excel(writer, sheet_name='查验状态台账', index=False)
        df_failed.to_excel(writer, sheet_name='异常待复核', index=False)
        df_dups.to_excel(writer, sheet_name='去重结果', index=False)

    total_amount_sum = round(sum(float(r.total_amount) if r.total_amount else 0 for r in records), 2)
    categories = {}
    for r in records:
        categories[r.expense_category] = categories.get(r.expense_category, 0) + 1

    ready_count = sum(1 for r in records if r.verify_required_fields_status == 'ready')
    partial_count = sum(1 for r in records if r.verify_required_fields_status == 'partial')
    missing_count = sum(1 for r in records if r.verify_required_fields_status == 'missing')

    rows_html = ''.join(
        f"<tr><td><a href='file://{quote(r.file_path)}'>{html.escape(r.file_name)}</a></td>"
        f"<td>{html.escape(r.invoice_number)}</td><td>{html.escape(r.invoice_date)}</td>"
        f"<td>{html.escape(r.seller_name)}</td><td>{html.escape(r.item_name)}</td>"
        f"<td>{html.escape(r.total_amount)}</td><td>{html.escape(r.expense_category)}</td>"
        f"<td>{html.escape(r.parse_status)}{' / 待复核' if r.review_needed else ''}</td>"
        f"<td>{html.escape(r.verify_required_fields_status)}</td><td>{html.escape(r.verify_status)}</td></tr>"
        for r in records
    )

    html_report = f"""<!doctype html>
<html lang='zh-CN'>
<head>
<meta charset='utf-8'>
<title>发票解析报告</title>
<style>
body{{font-family:Arial,'PingFang SC','Microsoft YaHei',sans-serif;margin:24px;color:#222;}}
.card{{display:inline-block;padding:16px 20px;margin:0 12px 12px 0;border-radius:12px;background:#f5f7fb;min-width:180px;}}
table{{border-collapse:collapse;width:100%;margin-top:16px;}}
th,td{{border:1px solid #ddd;padding:8px 10px;font-size:14px;text-align:left;vertical-align:top;}}
th{{background:#f2f2f2;}}
.warn{{color:#b54708;font-weight:600;}}
.ok{{color:#027a48;font-weight:600;}}
code{{background:#f4f4f4;padding:2px 6px;border-radius:6px;}}
a{{color:#175cd3;text-decoration:none;}}
a:hover{{text-decoration:underline;}}
</style>
</head>
<body>
<h1>电子发票解析报告</h1>
<p>输入目录：<code>{INPUT_DIR}</code></p>
<p>输出目录：<code>{OUTPUT_DIR}</code></p>
<p>生成时间：<code>{datetime.now().isoformat(timespec='seconds')}</code></p>
<div class='card'><div>发票总数</div><strong>{len(records)}</strong></div>
<div class='card'><div>解析成功</div><strong class='ok'>{sum(1 for r in records if r.parse_status == 'parsed')}</strong></div>
<div class='card'><div>待复核</div><strong class='warn'>{sum(1 for r in records if r.review_needed)}</strong></div>
<div class='card'><div>价税合计总额</div><strong>{total_amount_sum:.2f}</strong></div>
<h2>分类统计</h2>
<ul>{''.join(f'<li>{html.escape(k)}: {v}</li>' for k, v in sorted(categories.items()))}</ul>
<h2>查验准备情况</h2>
<ul>
<li>ready: {ready_count}</li>
<li>partial: {partial_count}</li>
<li>missing: {missing_count}</li>
</ul>
<p>查验相关文件：<code>查验准备清单.csv</code>、<code>查验状态台账.csv</code>、<code>查验辅助总览.html</code></p>
<h2>发票主表预览</h2>
<table>
<tr><th>文件名</th><th>发票号码</th><th>开票日期</th><th>销方</th><th>项目名称</th><th>价税合计</th><th>分类</th><th>解析状态</th><th>查验准备</th><th>查验状态</th></tr>
{rows_html}
</table>
<p>完整结果见同目录下：<code>发票汇总.xlsx</code>、<code>发票汇总.csv</code>、<code>报销导入模板.csv</code>、<code>发票解析结果.json</code>。</p>
</body>
</html>"""

    with (OUTPUT_DIR / '发票解析报告.html').open('w', encoding='utf-8') as f:
        f.write(html_report)


def main() -> None:
    global INPUT_DIR

    parser = argparse.ArgumentParser(description='解析指定目录下的电子发票并生成查验台账/报表')
    parser.add_argument('--input-dir', default=str(DEFAULT_INPUT_DIR), help='输入目录，默认财务/发票')
    parser.add_argument('--output-dir', default=str(DEFAULT_OUTPUT_DIR), help='输出目录根目录/兼容默认目录')
    args = parser.parse_args()

    INPUT_DIR = Path(args.input_dir)
    output_root = Path(args.output_dir).parent if Path(args.output_dir).name.startswith('发票_解析结果') else Path(args.output_dir)
    files = sorted([p for p in INPUT_DIR.iterdir() if p.is_file() and p.suffix.lower() in {'.pdf', '.xml', '.ofd'}])
    records = [parse_invoice(path) for path in files]

    buckets: dict[Path, list[InvoiceRecord]] = {}
    for rec in records:
        bucket_dir = resolve_output_dir_for_record(rec, output_parent=output_root)
        buckets.setdefault(bucket_dir, []).append(rec)

    total = 0
    for bucket_dir, bucket_records in buckets.items():
        set_parser_output_dir(bucket_dir)
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        existing_state = load_existing_verify_state()
        for rec in bucket_records:
            apply_existing_verify_state(rec, existing_state)
        build_outputs(bucket_records)
        total += len(bucket_records)
        print(f'Processed {len(bucket_records)} invoices -> {bucket_dir}')

    print(f'Total processed {total} invoices from {INPUT_DIR} into {len(buckets)} output dirs')


if __name__ == '__main__':
    main()
