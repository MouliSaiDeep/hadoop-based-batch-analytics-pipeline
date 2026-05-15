#!/bin/bash
set -e

# Generate a python script to quickly output the CSV file
cat << 'EOF' > /data/generate.py
import csv
import random
from datetime import datetime, timedelta

total_records = 2000000
whale_records = int(total_records * 0.1)  # 200,000 records
normal_records = total_records - whale_records

whale_caller = "WHALE_CALLER_999"
tower_ids = [f"TOWER_{i:03d}" for i in range(1, 101)]
call_types = ["VOICE", "SMS", "DATA"]
output_file = "/data/cdr_data.csv"

start_date = datetime(2023, 10, 1)
def random_date():
    return start_date + timedelta(seconds=random.randint(0, 30*24*60*60))

print(f"Generating {total_records} CDR records to {output_file}...")

with open(output_file, mode='w', newline='') as f:
    writer = csv.writer(f)
    # Write header
    writer.writerow(["caller_id", "receiver_id", "duration_sec", "tower_id", "timestamp", "call_type", "charge_amount"])
    
    whale_left = whale_records
    normal_left = normal_records
    total_left = total_records
    
    for _ in range(total_records):
        is_whale = False
        if total_left > 0:
            if random.random() < (whale_left / total_left):
                is_whale = True
                
        if is_whale:
            writer.writerow([
                whale_caller,
                f"USER_{random.randint(1, 10000)}",
                random.randint(10, 3600),
                random.choice(tower_ids),
                random_date().isoformat(),
                random.choice(call_types),
                round(random.uniform(0.1, 5.0), 2)
            ])
            whale_left -= 1
        else:
            writer.writerow([
                f"USER_{random.randint(1, 50000)}",
                f"USER_{random.randint(1, 50000)}",
                random.randint(10, 3600),
                random.choice(tower_ids),
                random_date().isoformat(),
                random.choice(call_types),
                round(random.uniform(0.1, 5.0), 2)
            ])
            normal_left -= 1
        total_left -= 1

print(f"Successfully generated records.")
EOF

# Execute the python script
python3 /data/generate.py
