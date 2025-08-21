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