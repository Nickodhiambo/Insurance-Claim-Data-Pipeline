
---

# 📌 Claim Resubmission Ingestion Pipeline

## 📖 Overview

This is a Data Processing Pipeline for Insurance Claims
The pipeline ingests claim data from **two heterogeneous EMR sources** (`Alpha` in CSV and `Beta` in JSON), normalizes them into a common schema, evaluates **resubmission eligibility** using defined business rules, and outputs:

* A structured list of **eligible resubmission candidates** (`resubmission_candidates.json`)
* A **metrics log file** (`pipeline_metrics.log`) for auditing and monitoring

---

## ⚙️ Features

* **Schema Normalization**

  * Handles Alpha (CSV) and Beta (JSON) formats
  * Cleans whitespace, normalizes casing, and ensures consistent field names

* **Resubmission Eligibility Logic**

  * Claim must be **denied**
  * Patient ID must be present
  * Submission date must be **older than 7 days** (to avoid premature resubmission)
  * Denial reason must be **retryable** (e.g., missing modifier, incorrect NPI, prior auth required)

* **Output Deliverables**

  * `resubmission_candidates.json` → contains normalized claims flagged for resubmission
  * `pipeline_metrics.log` → detailed summary with processing statistics and exclusion reasons

* **Command-Line Interface (CLI)**

  * Run the script directly from the command line with flexible file input

---

## 🏗️ Project Structure

```
.
├── claim_pipeline.py        # Main pipeline script
├── emr_alpha.csv            # Sample Alpha source file (CSV)
├── emr_beta.json            # Sample Beta source file (JSON)
├── resubmission_candidates.json  # Generated output (eligible claims)
├── pipeline_metrics.log      # Generated metrics summary
└── README.md                # Documentation
```

---

## 🚀 Installation & Setup

1. Clone the repository or copy the script to your project directory.
2. Ensure you have **Python 3.8+** installed.
3. Install required dependencies (standard library only; no extra installs needed).

---

## ▶️ Usage

Run the script from the command line:

```bash
python claim_pipeline.py <input_file1> [<input_file2> ...]
```

### Examples

1. Process **Alpha only**:

```bash
python claim_pipeline.py emr_alpha.csv
```

2. Process **Beta only**:

```bash
python claim_pipeline.py emr_beta.json
```

3. Process **both Alpha & Beta**:

```bash
python claim_pipeline.py emr_alpha.csv emr_beta.json
```

---

## 📂 Outputs

### 1. **Eligible Candidates**

`resubmission_candidates.json`
Contains normalized claims flagged for resubmission. Example:

```json
[
  {
    "claim_id": "A123",
    "resubmission_reason": "missing modifier",
    "source_system": "alpha",
    "recommended_changes": "Add correct CPT modifier, resubmit"
  },
  {
    "claim_id": "B988",
    "resubmission_reason": "missing modifier",
    "source_system": "beta",
    "recommended_changes": "Add correct CPT modifier, resubmit"
  }
]
```

---

### 2. **Metrics Summary**

`pipeline_metrics.log`
Audit-friendly text file with pipeline statistics:

```
===== Pipeline Metrics Summary =====
Total processed: 9
By source: {'alpha': 5, 'beta': 4}
Flagged for resubmission: 4
Excluded by reason:
  - not_denied: 2
  - patient_missing: 1
  - too_recent: 0
  - non_retryable_or_ambiguous: 2
  - malformed: 0
```

---

## 🧪 Sample Data

Two sample input files are included for testing:

* **Alpha (`emr_alpha.csv`)**

```csv
claim_id,patient_id,procedure_code,denial_reason,submitted_at,status
A123,P001,99213,Missing modifier,2025-07-01,denied
A124,P002,99214,Incorrect NPI,2025-07-10,denied
A125,,99215,Authorization expired,2025-07-05,denied
A126,P003,99381,None,2025-07-15,approved
A127,P004,99401,Prior auth required,2025-07-20,denied
```

* **Beta (`emr_beta.json`)**

```json
[
  {"id": "B987", "member": "P010", "code": "99213", "error_msg": "Incorrect provider type", "date": "2025-07-03T00:00:00", "status": "denied"},
  {"id": "B988", "member": "P011", "code": "99214", "error_msg": "Missing modifier", "date": "2025-07-09T00:00:00", "status": "denied"},
  {"id": "B989", "member": "P012", "code": "99215", "error_msg": null, "date": "2025-07-10T00:00:00", "status": "approved"},
  {"id": "B990", "member": null, "code": "99401", "error_msg": "incorrect procedure", "date": "2025-07-01T00:00:00", "status": "denied"}
]
```

---

## 📊 Metrics Captured

* **Total processed claims**
* **Counts by source system (Alpha / Beta)**
* **Flagged for resubmission**
* **Excluded claims (broken down by reason)**:

  * Not denied
  * Patient ID missing
  * Too recent (within 7 days)
  * Non-retryable or ambiguous denial reason
  * Malformed records

---

## ✅ Key Advantages

* Works with **multiple data sources**
* **Memory-efficient** via generators (`yield`)
* **Auditable**: produces machine- and human-readable outputs
* **Extensible**: easy to add more EMR source loaders or new denial reason rules

---