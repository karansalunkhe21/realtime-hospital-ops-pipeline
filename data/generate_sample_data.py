# ============================================================
# data/generate_sample_data.py
# UNCHANGED from original project.
# Run with: python data/generate_sample_data.py
# ============================================================

import json
import random
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from faker import Faker

fake = Faker()

WARDS           = ["Emergency", "ICU", "Cardiology", "Oncology", "Pediatrics", "Surgery"]
DIAGNOSIS_CODES = {
    "I21.0": "Acute anterior myocardial infarction",
    "J18.9": "Pneumonia, unspecified organism",
    "E11.9": "Type 2 diabetes mellitus without complications",
    "N18.3": "Chronic kidney disease, stage 3",
    "I50.9": "Heart failure, unspecified",
    "J44.1": "COPD with acute exacerbation",
    "A41.9": "Sepsis, unspecified organism",
    "S72.0": "Fracture of femoral neck",
}
INSURANCE_TYPES = ["Medicare", "Medicaid", "Private", "Self-pay"]
PHYSICIAN_IDS   = [f"DR{str(i).zfill(3)}" for i in range(1, 21)]
BED_STATUSES    = ["OCCUPIED", "AVAILABLE", "CLEANING"]


def make_admission_event():
    admit_ts  = fake.date_time_between(start_date="-30d", end_date="now")
    discharge = admit_ts + timedelta(hours=random.randint(4, 240))
    diag_code = random.choice(list(DIAGNOSIS_CODES.keys()))
    return {
        "event_id":        str(uuid.uuid4()),
        "patient_id":      f"PT{random.randint(10000, 99999)}",
        "patient_name":    fake.name(),
        "age":             random.randint(18, 95),
        "gender":          random.choice(["M", "F"]),
        "ward":            random.choice(WARDS),
        "admit_timestamp": admit_ts.isoformat(),
        "discharge_ts":    discharge.isoformat(),
        "diagnosis_code":  diag_code,
        "physician_id":    random.choice(PHYSICIAN_IDS),
        "insurance_type":  random.choice(INSURANCE_TYPES),
    }


def make_bed_event():
    return {
        "event_id":        str(uuid.uuid4()),
        "bed_id":          f"BED{random.randint(100, 500)}",
        "ward":            random.choice(WARDS),
        "status":          random.choice(BED_STATUSES),
        "patient_id":      f"PT{random.randint(10000, 99999)}",
        "event_timestamp": datetime.now().isoformat(),
    }


def make_diagnosis_event():
    diag_code = random.choice(list(DIAGNOSIS_CODES.keys()))
    return {
        "event_id":       str(uuid.uuid4()),
        "patient_id":     f"PT{random.randint(10000, 99999)}",
        "diagnosis_code": diag_code,
        "diagnosis_desc": DIAGNOSIS_CODES[diag_code],
        "icd10_category": diag_code.split(".")[0],
        "recorded_at":    datetime.now().isoformat(),
    }


def generate_all(n: int = 100):
    output_dir = Path("data/sample")
    output_dir.mkdir(parents=True, exist_ok=True)
    for filename, generator in [
        ("admissions.json",  make_admission_event),
        ("bed_events.json",  make_bed_event),
        ("diagnoses.json",   make_diagnosis_event),
    ]:
        records  = [generator() for _ in range(n)]
        filepath = output_dir / filename
        with open(filepath, "w") as f:
            json.dump(records, f, indent=2)
        print(f"✅ Saved {n} records → {filepath}")


if __name__ == "__main__":
    generate_all(n=100)
    print("\nDone! Check data/sample/ for generated files.")
