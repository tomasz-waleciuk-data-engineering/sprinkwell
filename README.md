```text
Repository structure:

sprinkwell/
├── .github/
│   └── workflows/
│       └── update_data.yml        ← UPDATED
├── utils/
│   └── electricity_to_buckets.py  ← MOVED & UPDATED
├── output/
│   └── electricity/
│       ├── bins/
│       │   └── processed_data.parquet   ← auto-generated
│       └── readings/
│           └── raw_readings.csv         ← auto-generated (NEW)
├── requirements.txt
└── README.md
```
