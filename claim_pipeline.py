# Claim Resubmission Ingestion Pipeline
# Implements Case Study #1
# Steps covered
    # 1) Schema Normalization
    # 2) Resubmission Eligibility Logic
    # 3) Outputs resubmission claims to a file named resubmission_candidates and metrics to a log file
# Usage: The script ingests data in any of these three ways:
    # 1) csv only (alpha)
        # script.py <alpha.csv>
    # 2) json only (beta)
        # script.py <beta.json>
    # 3) You can pass files from both source systems at the same time
        # script.py <<alpha.py> <beta.json>
    # In all cases, claims schemas are standardized and all claims eligible for
    # resubmission are stored in a common output file

import sys
import json
import csv
import logging
from datetime import datetime, date, timedelta
from typing import Any, Dict, Iterable, List, Optional, Tuple

#-------------------------
# Logging setup
#-------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s ")
logger = logging.getLogger('Claim pipeline')

#-------------------------------------
# Config
#-----------------------------------------
TODAY = date(2025, 7, 30) # Fixed today from case study
RETRYABLE = {'missing modifier', 'incorrect npi', 'prior auth required'}
NON_RETRYABLE = {'authorization expired', 'incorrect provider type'}
RECOMMENDATIONS = {
    'missing modifier': 'Add correct CPT modifier, resubmit',
    'incorrect npi': 'Review provider NPI, correct and resubmit',
    'prior auth required': 'Obtain/attach prior authorization and resubmit',
    'incorrect procedure': 'Verify CPT/HCPCS code mapping, correct if needed and resubmit',
    'form incomplete': 'Fill missing fields and resubmit',
    'not billable': 'Confirm coverage/payer policy; update claim or appeal'
}

#------------------------------------
# Utility Functions
#--------------------------------------
def to_iso_date(date_str: str) -> Optional[str]:
    """Standardizes input date strings"""
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str, '%Y-%m-%d').date().isoformat()
    except ValueError:
        try:
            return datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S').date().isoformat()
        except ValueError:
            return None
    
def remove_whitespaces(input_str: Any) -> Optional[str]:
    """Removes whitespaces from input strings"""
    if input_str is None:
        return None
    norm_str = input_str.strip()
    return norm_str if norm_str else None

def to_lower(input_str: Optional[str]) -> Optional[str]:
    """Converts a string to lower case"""
    return input_str.lower() if input_str else None

def older_than(d_iso: Optional[str], days: int, today: date) -> bool:
    """Checks if date of current claim is older than a week from a fixed today date"""
    if not d_iso:
        return False
    d = datetime.strptime(d_iso, '%Y-%m-%d').date()
    return (today - d) > timedelta(days=days)

#-----------------------------------------
# Step 1: Schema Normalization
#-------------------------------------------
def load_alpha(file_path: str) -> Iterable[Dict[str, Any]]:
    """Process CSV data (Alpha source)"""
    with open(file_path, "r", newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Handle denial reason cleanly
            raw = row.get('denial_reason')
            val = remove_whitespaces(raw)
            denial = None if (val is None or val.lower() in {'none', ''}) else val

            yield {
                'claim_id': remove_whitespaces(row.get('claim_id')),
                'patient_id': remove_whitespaces(row.get('patient_id')),
                'procedure_code': remove_whitespaces(row.get('procedure_code')),
                'denial_reason': denial,
                'status': to_lower(remove_whitespaces(row.get('status'))),
                'submitted_at': to_iso_date(row.get('submitted_at')),
                'source_system': 'alpha'
            }

def load_beta(file_path: str) -> Iterable[Dict[str, Any]]:
    """Normalize json (beta)"""
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
        for row in data:
            yield {
                'claim_id': remove_whitespaces(row.get('id')),
                'patient_id': remove_whitespaces(row.get('member')),
                'procedure_code': remove_whitespaces(row.get('code')),
                'denial_reason': remove_whitespaces(row.get('error_msg')),
                'status': to_lower(remove_whitespaces(row.get('status'))),
                'submitted_at': to_iso_date(row.get('date')),
                'source_system': 'beta'
            }

#------------------------------
# Step 2: Eligibility Logic
#------------------------------

def classify_denial(reason: Optional[str]) -> str:
    """Classifies a claim as either retryable, non-retryable or ambiguous"""
    if reason is None:
        return 'ambiguous'
    r = reason.lower()
    if r in RETRYABLE:
        return 'retryable'
    if r in NON_RETRYABLE:
        return 'non-retryable'
    if any(kw in r for kw in {'incorrect procedure', 'form incomplete', 'not billable'}):
        return 'retryable'
    return 'ambiguous'


def is_eligible(claim: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
    """Determine whether a claim is eligible for resubmisson based
    on predefined business rules"""
    if claim['status'] != 'denied':
        return False, None
    if not claim['patient_id']:
        return False, None
    if not older_than(claim['submitted_at'], 7, TODAY):
        return False, None
    if classify_denial(claim['denial_reason']) != 'retryable':
        return False, None
    return True, claim['denial_reason']

def recommended_changes(reason: Optional[str]) -> str:
    if reason is None:
        return 'Review claim details, supply missing info and resubmit'
    return RECOMMENDATIONS.get(reason.lower(), 'Review claim details, supply missing info and resubmit')

#---------------------------------------------
# Step 3: Pipeline: Input -> Processing -> Output
#-----------------------------------------------------
def pipeline(input_files: List[str]) -> Dict[str, Any]:
    candidates: List[Dict[str, Any]] = [] # Will hold eligible claims

    # Metrics
    metrics = {
        'total_processed': 0,
        'by_source': {'alpha': 0, 'beta': 0},
        'flagged_for_resubmission': 0,
        'excluded_by_reason': {
            'not_denied_status': 0,
            'patient_id_missing': 0,
            'too_recent': 0,
            'non-retryable_or_ambiguous': 0,
            'malformed': 0
        }
    }

    for path in input_files:
        # Take input file from command line
        # Checks source origin
        # process
        try:
            if path.endswith(".csv"):
                records = load_alpha(path)
            elif path.endswith(".json"):
                records = load_beta(path)
            else:
                logger.warning("Unsupported file type: %s", path)
                continue

            for rec in records:
                metrics["total_processed"] += 1
                if rec["source_system"] in metrics["by_source"]:
                    metrics["by_source"][rec["source_system"]] += 1

                try:
                    ok, reason = is_eligible(rec) # Only flag eligible files for resubmission
                    if ok:
                        metrics["flagged_for_resubmission"] += 1
                        candidates.append({
                            "claim_id": rec["claim_id"],
                            "resubmission_reason": reason,
                            "source_system": rec["source_system"],
                            "recommended_changes": recommended_changes(reason),
                        })
                    else:
                        # Figure out why file is excluded from resubmission
                        # based on business rules and inference
                        if rec["status"] != "denied":
                            metrics["excluded_by_reason"]["not_denied"] += 1
                        elif not rec["patient_id"]:
                            metrics["excluded_by_reason"]["patient_missing"] += 1
                        elif not older_than(rec["submitted_at"], 7, TODAY):
                            metrics["excluded_by_reason"]["too_recent"] += 1
                        else:
                            metrics["excluded_by_reason"]["non_retryable_or_ambiguous"] += 1
                except Exception:
                    metrics["excluded_by_reason"]["malformed"] += 1
        except Exception as e:
            logger.exception("Failed to process file %s: %s", path, e)
            metrics["excluded_by_reason"]["malformed"] += 1

    #--------------Log resubmisson candidates----------
    output_path = 'resubmission_candidates.json'
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(candidates, f, indent=2)

    # -------------Log metrics----------------------------
    log_path = "pipeline_metrics.log"
    with open(log_path, "w", encoding="utf-8") as f:
        f.write("===== Pipeline Metrics Summary =====\n")
        f.write(f"Total processed: {metrics['total_processed']}\n")
        f.write(f"By source: {metrics['by_source']}\n")
        f.write(f"Flagged for resubmission: {metrics['flagged_for_resubmission']}\n")
        f.write("Excluded by reason:\n")
        for reason, count in metrics["excluded_by_reason"].items():
            f.write(f"  - {reason}: {count}\n")

    return {'output_path': output_path, 'candidates': candidates, "metrics_path": log_path}
    
if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('Usage error: python claim_data_pipeline.py <emr_alpha.csv> [emr_beta.json]')
        sys.exit(1)

    input_files = sys.argv[1:]
    result = pipeline(input_files)
    logger.info('Output saved to %s', result['output_path'])