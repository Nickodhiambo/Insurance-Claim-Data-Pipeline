# Claim Resubmission Ingestion Pipeline
# Implements Case Study #1
# Steps covered
    # 1) Schema Normalization
    # 2) Resubmission Eligibility Logic
    # 3) Outputs resubmission claims to a file named resubmission_candidates
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
RETRYABLE = {'Missing modifier', 'Incorrect NPI', 'Prior auth required'}
NON_RETRYABLE = {'Authorization expired', 'Incorrect provider type'}
RECOMMENDATIONS = {
    'Missing modifier': 'Add correct CPT modifier, resubmit',
    'Incorrect NPI': 'Review provider NPI, correct and resubmit',
    'Prior auth required': 'Obtain/attach prior authorization and resubmit',
    'Incorrect procedure': 'Verify CPT/HCPCS code mapping, correct if needed and resubmit',
    'Form incomplete': 'Fill missing fields and resubmit',
    'Not billable': 'Confirm coverage/payer policy; update claim or appeal'
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
            datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S').date().isoformat()
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
            yield {
                'claim_id': remove_whitespaces(row.get('claim_id')),
                'patient_id': remove_whitespaces(row.get('patient_id')),
                'procedure_code': remove_whitespaces(row.get('procedure_code')),
                'denial_reason': (
                    None if str(row.get('denial_reason').strip().lower in {'none', ''}) else
                    remove_whitespaces(row.get('denial_reason'))
                ),
                'status': to_lower(remove_whitespaces(row.get('status'))),
                'submitted_at': to_iso_date(row.get('submitted_at')),
                'source_system': 'alpha'
            }
def load_beta(file_path: str) -> Iterable[Dict[str, Any]]:
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
        for row in data:
            yield {
                'claim_id': remove_whitespaces(row.get('claim_id')),
                'patient_id': remove_whitespaces(row.get('patient_id')),
                'procedure_code': remove_whitespaces(row.get('procedure_code')),
                'denial_reason': remove_whitespaces(row.get('error_msg')),
                'status': to_lower(remove_whitespaces(row.get('status'))),
                'submitted_at': to_iso_date(row.get('submitted_at')),
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
    candidates = List[Dict[str, Any]] # Will hold eligible claims

    for path in input_files:
        if path.endswith('csv'):
            records = load_alpha(path)
        elif path.endswith('.json'):
            records = load_beta(path)
        else:
            logger.warning('Unsupported file type %s', path)
            continue
        
        for rec in records:
            flag, reason = is_eligible(rec)

            if flag:
                candidates.append({
                    'claim_id': rec['claim_id'],
                    'resubmission_reason': reason,
                    'source_system': rec['source_system'],
                    'recommended_changes': recommended_changes(reason)
                })
        output_path = 'resubmission_candidates.json'
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(candidates, f, indent=2)
        
        return {'output_path': output_path, 'candidates': candidates}
    
if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('Usage: python claim_data_pipeline.py <emr_alpha.csv> [emr_beta.json]')
        sys.exit(1)

    input_files = sys.argv[1:]
    result = pipeline(input_files)
    logger.info('Output saved to %s', result['output_path'])
    print(json.dumps(result['candidates']))