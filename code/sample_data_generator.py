"""
Generate synthetic healthcare data for lineage demonstration.

Creates FHIR R4 patient records and HL7 v2 messages for testing
the data lineage pipeline (Bronze → Silver → Gold).
"""

import json
import random
from datetime import datetime, timedelta
from typing import List, Dict, Any
from faker import Faker

fake = Faker()


def generate_patient_id() -> str:
    """Generate realistic patient ID."""
    return f"PT{random.randint(100000, 999999)}"


def generate_mrn() -> str:
    """Generate Medical Record Number."""
    return f"MRN{random.randint(1000000, 9999999)}"


def generate_fhir_patient() -> Dict[str, Any]:
    """
    Generate synthetic FHIR R4 Patient resource.

    Returns:
        Dict containing FHIR Patient resource
    """
    patient_id = generate_patient_id()
    birth_date = fake.date_of_birth(minimum_age=18, maximum_age=90)

    return {
        "resourceType": "Patient",
        "id": patient_id,
        "identifier": [
            {
                "system": "urn:oid:2.16.840.1.113883.4.1",
                "value": fake.ssn()
            },
            {
                "system": "http://hospital.example.org/patients",
                "value": generate_mrn()
            }
        ],
        "active": True,
        "name": [
            {
                "use": "official",
                "family": fake.last_name(),
                "given": [fake.first_name()]
            }
        ],
        "gender": random.choice(["male", "female", "other"]),
        "birthDate": birth_date.strftime("%Y-%m-%d"),
        "address": [
            {
                "use": "home",
                "line": [fake.street_address()],
                "city": fake.city(),
                "state": fake.state_abbr(),
                "postalCode": fake.zipcode(),
                "country": "US"
            }
        ],
        "telecom": [
            {
                "system": "phone",
                "value": fake.phone_number(),
                "use": "mobile"
            },
            {
                "system": "email",
                "value": fake.email()
            }
        ]
    }


def generate_hl7_message(patient: Dict[str, Any]) -> str:
    """
    Generate HL7 v2.5 ADT^A01 message from FHIR patient.

    Args:
        patient: FHIR Patient resource

    Returns:
        HL7 v2 message string
    """
    mrn = patient["identifier"][1]["value"]
    name = patient["name"][0]
    family_name = name["family"]
    given_name = name["given"][0]
    birth_date = patient["birthDate"].replace("-", "")
    gender = patient["gender"][0].upper()
    address = patient["address"][0]

    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

    hl7_msg = f"""MSH|^~\\&|EPIC|Hospital^A|HL7^A|ROUTING^A|{timestamp}||ADT^A01|MSG{random.randint(10000, 99999)}|P|2.5
EVN|A01|{timestamp}
PID|1||{mrn}^^^Hospital^MRN||{family_name}^{given_name}||{birth_date}|{gender}||||||||||{patient["identifier"][0]["value"]}
PV1|1|I|Ward^101^01^Hospital||||||||||||||||Visit{random.randint(100000, 999999)}"""

    return hl7_msg


def generate_encounter(patient_id: str) -> Dict[str, Any]:
    """
    Generate FHIR Encounter resource.

    Args:
        patient_id: Patient ID

    Returns:
        Dict containing FHIR Encounter resource
    """
    encounter_date = fake.date_time_between(start_date="-1y", end_date="now")
    encounter_end = encounter_date + timedelta(hours=random.randint(1, 48))

    conditions = [
        "Hypertension",
        "Type 2 Diabetes",
        "Asthma",
        "COPD",
        "Coronary Artery Disease",
        "Heart Failure",
        "Pneumonia",
        "UTI"
    ]

    return {
        "resourceType": "Encounter",
        "id": f"ENC{random.randint(100000, 999999)}",
        "status": "finished",
        "class": {
            "system": "http://terminology.hl7.org/CodeSystem/v3-ActCode",
            "code": random.choice(["IMP", "AMB", "EMER"]),
            "display": random.choice(["inpatient", "ambulatory", "emergency"])
        },
        "subject": {
            "reference": f"Patient/{patient_id}"
        },
        "period": {
            "start": encounter_date.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "end": encounter_end.strftime("%Y-%m-%dT%H:%M:%SZ")
        },
        "reasonCode": [
            {
                "coding": [
                    {
                        "system": "http://snomed.info/sct",
                        "code": str(random.randint(100000, 999999)),
                        "display": random.choice(conditions)
                    }
                ]
            }
        ]
    }


def generate_observation(patient_id: str) -> Dict[str, Any]:
    """
    Generate FHIR Observation resource (vital signs).

    Args:
        patient_id: Patient ID

    Returns:
        Dict containing FHIR Observation resource
    """
    vitals = [
        {
            "code": "85354-9",
            "display": "Blood Pressure",
            "value": f"{random.randint(110, 140)}/{random.randint(70, 90)}",
            "unit": "mm[Hg]"
        },
        {
            "code": "8867-4",
            "display": "Heart Rate",
            "value": random.randint(60, 100),
            "unit": "beats/min"
        },
        {
            "code": "8310-5",
            "display": "Body Temperature",
            "value": round(random.uniform(36.5, 37.5), 1),
            "unit": "Cel"
        },
        {
            "code": "2708-6",
            "display": "Oxygen Saturation",
            "value": random.randint(95, 100),
            "unit": "%"
        }
    ]

    vital = random.choice(vitals)
    obs_date = fake.date_time_between(start_date="-1y", end_date="now")

    return {
        "resourceType": "Observation",
        "id": f"OBS{random.randint(100000, 999999)}",
        "status": "final",
        "category": [
            {
                "coding": [
                    {
                        "system": "http://terminology.hl7.org/CodeSystem/observation-category",
                        "code": "vital-signs",
                        "display": "Vital Signs"
                    }
                ]
            }
        ],
        "code": {
            "coding": [
                {
                    "system": "http://loinc.org",
                    "code": vital["code"],
                    "display": vital["display"]
                }
            ]
        },
        "subject": {
            "reference": f"Patient/{patient_id}"
        },
        "effectiveDateTime": obs_date.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "valueQuantity": {
            "value": vital["value"],
            "unit": vital["unit"],
            "system": "http://unitsofmeasure.org"
        }
    }


def generate_patient_bundle(include_encounters: bool = True) -> Dict[str, Any]:
    """
    Generate FHIR Bundle with patient and related resources.

    Args:
        include_encounters: Include encounters and observations

    Returns:
        Dict containing FHIR Bundle
    """
    patient = generate_fhir_patient()
    patient_id = patient["id"]

    entries = [
        {
            "fullUrl": f"urn:uuid:{patient_id}",
            "resource": patient
        }
    ]

    if include_encounters:
        # Add 1-3 encounters
        for _ in range(random.randint(1, 3)):
            encounter = generate_encounter(patient_id)
            entries.append({
                "fullUrl": f"urn:uuid:{encounter['id']}",
                "resource": encounter
            })

            # Add 2-4 observations per encounter
            for _ in range(random.randint(2, 4)):
                observation = generate_observation(patient_id)
                entries.append({
                    "fullUrl": f"urn:uuid:{observation['id']}",
                    "resource": observation
                })

    return {
        "resourceType": "Bundle",
        "type": "transaction",
        "entry": entries
    }


def generate_bronze_layer_data(num_patients: int = 100) -> List[str]:
    """
    Generate HL7 messages for Bronze layer (raw ingestion).

    Args:
        num_patients: Number of patient records to generate

    Returns:
        List of HL7 message strings
    """
    hl7_messages = []

    for _ in range(num_patients):
        patient = generate_fhir_patient()
        hl7_msg = generate_hl7_message(patient)
        hl7_messages.append(hl7_msg)

    return hl7_messages


def generate_silver_layer_data(num_patients: int = 100) -> List[Dict[str, Any]]:
    """
    Generate FHIR bundles for Silver layer (cleaned/transformed).

    Args:
        num_patients: Number of patient bundles to generate

    Returns:
        List of FHIR Bundle resources
    """
    bundles = []

    for _ in range(num_patients):
        bundle = generate_patient_bundle(include_encounters=True)
        bundles.append(bundle)

    return bundles


def save_bronze_data(output_dir: str = "sample_data/bronze", num_files: int = 10):
    """
    Save HL7 messages to files (Bronze layer simulation).

    Args:
        output_dir: Output directory path
        num_files: Number of files to create
    """
    import os

    os.makedirs(output_dir, exist_ok=True)

    date_str = datetime.now().strftime("%Y-%m-%d")

    for i in range(num_files):
        hl7_messages = generate_bronze_layer_data(num_patients=100)
        filename = f"{output_dir}/patients_{date_str}_batch{i+1:03d}.hl7"

        with open(filename, "w") as f:
            f.write("\n\n".join(hl7_messages))

        print(f"✓ Created {filename} ({len(hl7_messages)} patients)")


def save_silver_data(output_dir: str = "sample_data/silver", num_files: int = 5):
    """
    Save FHIR bundles to JSON files (Silver layer simulation).

    Args:
        output_dir: Output directory path
        num_files: Number of files to create
    """
    import os

    os.makedirs(output_dir, exist_ok=True)

    date_str = datetime.now().strftime("%Y-%m-%d")

    for i in range(num_files):
        bundles = generate_silver_layer_data(num_patients=200)
        filename = f"{output_dir}/fhir_patients_{date_str}_part{i+1:03d}.json"

        with open(filename, "w") as f:
            json.dump(bundles, f, indent=2)

        print(f"✓ Created {filename} ({len(bundles)} patient bundles)")


if __name__ == "__main__":
    print("Generating synthetic healthcare data...\n")

    # Generate Bronze layer (HL7 messages)
    print("=== Bronze Layer (Raw HL7) ===")
    save_bronze_data(num_files=10)

    print("\n=== Silver Layer (FHIR Bundles) ===")
    save_silver_data(num_files=5)

    print("\n✓ Sample data generation complete!")
    print("  - Bronze: sample_data/bronze/*.hl7")
    print("  - Silver: sample_data/silver/*.json")
