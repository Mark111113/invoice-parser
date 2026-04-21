#!/usr/bin/env python3
import argparse
import json
from pathlib import Path

import pandas as pd

from verify.verify_contract import extract_update_from_result

DEFAULT_OUTPUT_DIR = Path('/mnt/fn/Download3/clawdbotfile/财务/发票_解析结果')
OUTPUT_DIR = DEFAULT_OUTPUT_DIR
LEDGER_FILE = OUTPUT_DIR / '查验状态台账.csv'
MANUAL_INPUT_CANDIDATES = [
    OUTPUT_DIR / '查验结果录入.csv',
    OUTPUT_DIR / '查验结果录入模板.csv',
]
REPORT_FILE = OUTPUT_DIR / '报销入库台账.csv'

VERIFY_COLS = [
    'verify_task_id', 'verify_status_code', 'verify_result_code',
    'verify_status', 'verify_time', 'verify_channel', 'verify_result_summary',
    'is_voided', 'is_abnormal', 'verify_screenshot_path', 'verify_raw_result_path'
]

RESULTS_DIR = OUTPUT_DIR / 'verify_results'


def read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path, dtype=str).fillna('')
    except pd.errors.EmptyDataError:
        return pd.DataFrame()


def pick_manual_file() -> Path | None:
    for p in MANUAL_INPUT_CANDIDATES:
        if p.exists():
            return p
    return None


def load_result_json_rows() -> pd.DataFrame:
    rows = []
    if not RESULTS_DIR.exists():
        return pd.DataFrame()
    for path in sorted(RESULTS_DIR.glob('*.json')):
        try:
            payload = json.loads(path.read_text(encoding='utf-8'))
            rows.append(extract_update_from_result(payload))
        except Exception:
            continue
    return pd.DataFrame(rows).fillna('') if rows else pd.DataFrame()


def merge_updates(ledger_df: pd.DataFrame, updates_df: pd.DataFrame) -> pd.DataFrame:
    if updates_df.empty:
        return ledger_df

    merged_df = ledger_df.copy()
    key_candidates = [
        ['verify_task_id'],
        ['file_hash'],
        ['duplicate_key'],
    ]

    for _, row in updates_df.iterrows():
        mask = pd.Series([False] * len(merged_df))
        for keys in key_candidates:
            usable = [k for k in keys if k in merged_df.columns and k in updates_df.columns and str(row.get(k, '')).strip()]
            if not usable:
                continue
            current = pd.Series([True] * len(merged_df))
            for key in usable:
                current = current & (merged_df[key].fillna('') == str(row.get(key, '')))
            if current.any():
                mask = current
                break

        if not mask.any():
            continue

        for col in VERIFY_COLS:
            if col in merged_df.columns and col in updates_df.columns and str(row.get(col, '')) != '':
                merged_df.loc[mask, col] = str(row.get(col, ''))

    return merged_df


def main() -> None:
    global OUTPUT_DIR, LEDGER_FILE, MANUAL_INPUT_CANDIDATES, REPORT_FILE, RESULTS_DIR

    parser = argparse.ArgumentParser(description='将 verifier 结果 JSON/CSV 回填到 parser 台账')
    parser.add_argument('--output-dir', default=str(DEFAULT_OUTPUT_DIR), help='parser 输出目录')
    args = parser.parse_args()

    OUTPUT_DIR = Path(args.output_dir)
    LEDGER_FILE = OUTPUT_DIR / '查验状态台账.csv'
    MANUAL_INPUT_CANDIDATES = [
        OUTPUT_DIR / '查验结果录入.csv',
        OUTPUT_DIR / '查验结果录入模板.csv',
    ]
    REPORT_FILE = OUTPUT_DIR / '报销入库台账.csv'
    RESULTS_DIR = OUTPUT_DIR / 'verify_results'

    ledger_df = read_csv(LEDGER_FILE)
    if ledger_df.empty:
        raise SystemExit(f'ledger not found: {LEDGER_FILE}')

    manual_file = pick_manual_file()
    merged = ledger_df.copy()

    if manual_file:
        manual_df = read_csv(manual_file)
        if not manual_df.empty:
            if not any(col in manual_df.columns for col in ['verify_task_id', 'file_hash', 'duplicate_key']):
                raise SystemExit('录入文件缺少 verify_task_id / file_hash / duplicate_key，无法匹配回写')

            match_cols = [c for c in ['verify_task_id', 'file_hash', 'duplicate_key'] if c in ledger_df.columns and c in manual_df.columns]
            merged = ledger_df.merge(
                manual_df[[c for c in set(match_cols + VERIFY_COLS) if c in manual_df.columns]],
                on=match_cols,
                how='left',
                suffixes=('', '_new')
            )

            for col in VERIFY_COLS:
                new_col = f'{col}_new'
                if new_col in merged.columns:
                    merged[col] = merged[new_col].where(merged[new_col] != '', merged[col])
                    merged.drop(columns=[new_col], inplace=True)

    json_result_df = load_result_json_rows()
    if manual_file is None and json_result_df.empty:
        raise SystemExit('未找到可回填来源：既没有 查验结果录入.csv/模板，也没有 verify_results/*.json')

    merged = merge_updates(merged, json_result_df)
    merged.to_csv(LEDGER_FILE, index=False, encoding='utf-8-sig')

    main_csv = read_csv(OUTPUT_DIR / '发票汇总.csv')
    reimbursement_csv = read_csv(OUTPUT_DIR / '报销导入模板.csv')
    if not main_csv.empty:
        verify_cols = [c for c in ['file_hash', 'duplicate_key'] + VERIFY_COLS + ['verify_status', 'verify_time', 'verify_channel', 'verify_result_summary', 'is_voided', 'is_abnormal'] if c in merged.columns]
        verify_subset = merged[[c for c in verify_cols if c in merged.columns]].copy()
        join_keys = [c for c in ['file_hash', 'duplicate_key'] if c in main_csv.columns and c in verify_subset.columns]
        if join_keys:
            updated_main = main_csv.drop(columns=[c for c in VERIFY_COLS + ['verify_status', 'verify_time', 'verify_channel', 'verify_result_summary', 'is_voided', 'is_abnormal'] if c in main_csv.columns], errors='ignore').merge(verify_subset, on=join_keys, how='left')
            updated_main.to_csv(OUTPUT_DIR / '发票汇总.csv', index=False, encoding='utf-8-sig')

    if not reimbursement_csv.empty:
        join_keys = [c for c in ['file_path'] if c in reimbursement_csv.columns and c in merged.columns]
        if join_keys:
            merged_small = merged[[c for c in ['file_path', 'verify_status', 'verify_time', 'verify_channel', 'verify_result_summary', 'is_voided', 'is_abnormal'] if c in merged.columns]].copy()
            updated_reimbursement = reimbursement_csv.drop(columns=[c for c in ['查验时间', '查验渠道', '查验结果摘要', '是否作废', '是否异常'] if c in reimbursement_csv.columns], errors='ignore').merge(
                merged_small,
                left_on='原文件路径',
                right_on='file_path',
                how='left'
            )
            updated_reimbursement.rename(columns={
                'verify_time': '查验时间',
                'verify_channel': '查验渠道',
                'verify_result_summary': '查验结果摘要',
                'is_voided': '是否作废',
                'is_abnormal': '是否异常',
            }, inplace=True)
            updated_reimbursement.drop(columns=['file_path'], inplace=True, errors='ignore')
            updated_reimbursement.to_csv(OUTPUT_DIR / '报销导入模板.csv', index=False, encoding='utf-8-sig')
            updated_reimbursement.to_csv(REPORT_FILE, index=False, encoding='utf-8-sig')

    source_desc = str(manual_file) if manual_file else 'verify_results/*.json'
    print(f'Applied verify results from {source_desc} -> {LEDGER_FILE}')


if __name__ == '__main__':
    main()
