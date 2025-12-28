import polars as pl
from datetime import datetime, timedelta, time
import os

# ============================================
# CONFIGURATION - EASY TO CHANGE
# ============================================

SHEET_CSV_URL = "https://docs.google.com/spreadsheets/d/15LJer7zyH6wd_9qDHOl-PeU7FKx-neap9IwlCyCSSf4/export?format=csv&gid=0"

# First row to include (Google Sheets row number)
START_ROW = 305

# Output paths (relative to repo root)
OUTPUT_DIR_BINS = "output/electricity/bins"
OUTPUT_DIR_READINGS = "output/electricity/readings"

OUTPUT_FILE_PARQUET = os.path.join(OUTPUT_DIR_BINS, "processed_data.parquet")
OUTPUT_FILE_RAW_CSV = os.path.join(OUTPUT_DIR_READINGS, "raw_readings.csv")

# ============================================
# Create output directories if they don't exist
# ============================================
os.makedirs(OUTPUT_DIR_BINS, exist_ok=True)
os.makedirs(OUTPUT_DIR_READINGS, exist_ok=True)

# ============================================
# 1. Load raw data from Google Sheets
# ============================================
print("📥 Loading data from Google Sheets...")

df = pl.read_csv(SHEET_CSV_URL, has_header=False)

print("Shape (raw):", df.shape)

# Select columns by POSITION:
# Column B = column_2 → Date (Excel serial)
# Column C = column_3 → Time (Excel serial fraction)
# Column E = column_5 → P
# Column F = column_6 → OP

df = df.select([
    pl.col("column_2").alias("Date"),
    pl.col("column_3").alias("Time"),
    pl.col("column_5").alias("P"),
    pl.col("column_6").alias("OP"),
])

print("\nSelected columns (B, C, E, F):")
print(df.head(10))

# Skip rows before START_ROW
skip_count = START_ROW - 1
df = df.slice(skip_count, None)

print(f"\nShape after starting from row {START_ROW}:", df.shape)
print(df.head())

# ============================================
# 1.1 Save raw readings to CSV (human-readable)
# ============================================
print(f"\n💾 Preparing raw readings for export...")

def excel_serial_to_date_string(date_serial) -> str:
    """Convert Excel serial date to YYYY-MM-DD string"""
    try:
        date_serial = float(date_serial)
        excel_epoch = datetime(1899, 12, 30)
        date_part = excel_epoch + timedelta(days=date_serial)
        return date_part.strftime("%Y-%m-%d")
    except:
        return None

def excel_serial_to_time_string(time_serial) -> str:
    """Convert Excel serial time to HH:MM:SS string (using only fractional part)"""
    try:
        time_serial = float(time_serial) if time_serial else 0.0
        time_fraction = time_serial % 1  # Only decimal part
        total_seconds = round(time_fraction * 24 * 60 * 60)
        hours = int(total_seconds // 3600)
        minutes = int((total_seconds % 3600) // 60)
        seconds = int(total_seconds % 60)
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
    except:
        return None

# Convert to human-readable format
dates_readable = [excel_serial_to_date_string(d) for d in df['Date'].to_list()]
times_readable = [excel_serial_to_time_string(t) for t in df['Time'].to_list()]

df_raw_export = pl.DataFrame({
    'Date': dates_readable,
    'Time': times_readable,
    'P': df['P'].to_list(),
    'OP': df['OP'].to_list(),
})

# Remove rows where Time, P, or OP are null/empty
original_count = df_raw_export.height
df_raw_export = df_raw_export.filter(
    (pl.col("Time").is_not_null()) & 
    (pl.col("Time") != "") &
    (pl.col("P").is_not_null()) & 
    (pl.col("P") != "") &
    (pl.col("OP").is_not_null()) & 
    (pl.col("OP") != "")
)
removed_count = original_count - df_raw_export.height

if removed_count > 0:
    print(f"⚠️ Removed {removed_count} rows with null values")

# Cast P and OP to Float64 for delta calculation
df_raw_export = df_raw_export.with_columns([
    pl.col("P").cast(pl.Float64),
    pl.col("OP").cast(pl.Float64),
])

# Add Delta columns (difference from previous row)
df_raw_export = df_raw_export.with_columns([
    (pl.col("P") - pl.col("P").shift(1)).alias("Delta_P"),
    (pl.col("OP") - pl.col("OP").shift(1)).alias("Delta_OP"),
])

print(f"\nRaw readings sample:")
print(df_raw_export.head(10))

df_raw_export.write_csv(OUTPUT_FILE_RAW_CSV)
print(f"✅ Saved {df_raw_export.height:,} rows to {OUTPUT_FILE_RAW_CSV}")

# ============================================
# 2. BST/GMT conversion functions
# ============================================
def last_sunday(year: int, month: int) -> datetime:
    if month == 12:
        next_month = datetime(year + 1, 1, 1)
    else:
        next_month = datetime(year, month + 1, 1)
    last_day = next_month - timedelta(days=1)
    days_to_sunday = (last_day.weekday() + 1) % 7
    return last_day - timedelta(days=days_to_sunday)

def is_bst(dt: datetime) -> bool:
    if dt is None:
        return False
    year = dt.year
    bst_start = last_sunday(year, 3).replace(hour=1, minute=0, second=0)
    bst_end = last_sunday(year, 10).replace(hour=2, minute=0, second=0)
    return bst_start <= dt < bst_end

def excel_serial_to_datetime(date_serial, time_serial) -> datetime:
    """
    Convert Excel serial date + time to Python datetime.
    
    Time > 1: IGNORE integer part, use only fractional part for time.
    """
    if date_serial is None or time_serial is None:
        return None
    
    try:
        date_serial = float(date_serial)
        time_serial = float(time_serial) if time_serial else 0.0
        
        # ONLY use fractional part of time (discard integer part)
        time_fraction = time_serial % 1
        
        # Excel epoch: December 30, 1899
        excel_epoch = datetime(1899, 12, 30)
        
        # Date part: use date serial as-is
        date_part = excel_epoch + timedelta(days=date_serial)
        
        # Time part: fractional part only
        total_seconds = round(time_fraction * 24 * 60 * 60)
        hours = int(total_seconds // 3600)
        minutes = int((total_seconds % 3600) // 60)
        seconds = int(total_seconds % 60)
        
        return date_part.replace(hour=hours, minute=minutes, second=seconds)
    
    except (ValueError, TypeError) as e:
        print(f"Warning: Could not parse date={date_serial}, time={time_serial}: {e}")
        return None

def convert_to_utc(dt: datetime) -> datetime:
    """Convert local UK time to UTC"""
    if dt is None:
        return None
    if is_bst(dt):
        return dt - timedelta(hours=1)
    return dt

# ============================================
# 3. Create DateTime column
# ============================================
print("\n🔄 Converting Excel serial dates to datetime...")

dates = df['Date'].to_list()
times = df['Time'].to_list()
ps = df['P'].to_list()
ops = df['OP'].to_list()

local_datetimes = [excel_serial_to_datetime(d, t) for d, t in zip(dates, times)]
utc_datetimes = [convert_to_utc(dt) for dt in local_datetimes]

# Debug: print first few conversions
print("\nSample conversions (first 5):")
for i in range(min(5, len(dates))):
    print(f"  Excel: {dates[i]}, {times[i]} → Local: {local_datetimes[i]} → UTC: {utc_datetimes[i]}")

df_transformed = pl.DataFrame({
    'UTC_DateTime': utc_datetimes,
    'P': ps,
    'OP': ops
})

# Convert P and OP to float
df_transformed = df_transformed.with_columns([
    pl.col("P").cast(pl.Float64),
    pl.col("OP").cast(pl.Float64),
])

# Filter out rows where datetime conversion failed
original_count = df_transformed.height
df_transformed = df_transformed.filter(pl.col("UTC_DateTime").is_not_null())
filtered_count = df_transformed.height
if original_count != filtered_count:
    print(f"\n⚠️ Filtered out {original_count - filtered_count} rows with invalid dates")

print("\nAfter UTC conversion:")
print(df_transformed.head())

# ============================================
# 4. Sort and calculate deltas
# ============================================
df_transformed = df_transformed.sort("UTC_DateTime")
df_transformed = df_transformed.with_columns([
    (pl.col("P").shift(-1) - pl.col("P")).alias("Delta_P"),
    (pl.col("OP").shift(-1) - pl.col("OP")).alias("Delta_OP"),
    pl.col("UTC_DateTime").shift(-1).alias("Next_DateTime")
])

df_transformed = df_transformed.filter(pl.col("Next_DateTime").is_not_null())

print("\nAfter delta calculation:")
print(df_transformed.head())

# ============================================
# 5. Generate 1-minute grid and expand
# ============================================
def expand_minutes(row):
    """Expand a reading interval into 1-minute rows"""
    start = row["UTC_DateTime"]
    end = row["Next_DateTime"]
    delta_p = row["Delta_P"]
    delta_op = row["Delta_OP"]
    
    if end is None or start is None:
        return [], [], []
    
    peak_start = time(6, 30)
    peak_end = time(23, 30)
    
    # First pass: count P and OP minutes
    current = start + timedelta(minutes=1)
    p_mins = 0
    op_mins = 0
    
    temp = current
    while temp <= end:
        t = temp.time()
        if t > peak_start and t <= peak_end:
            p_mins += 1
        else:
            op_mins += 1
        temp += timedelta(minutes=1)
    
    # Calculate rates
    p_rate = float(delta_p) / p_mins if p_mins > 0 else 0.0
    op_rate = float(delta_op) / op_mins if op_mins > 0 else 0.0
    
    # Second pass: generate rows
    minute_grids = []
    p_values = []
    op_values = []
    
    current = start + timedelta(minutes=1)
    
    while current <= end:
        t = current.time()
        is_peak = t > peak_start and t <= peak_end
        
        minute_grids.append(current)
        p_values.append(p_rate if is_peak else float('nan'))
        op_values.append(op_rate if not is_peak else float('nan'))
        
        current += timedelta(minutes=1)
    
    return minute_grids, p_values, op_values

print("\n⏳ Expanding to 1-minute grid...")
all_minute_grids = []
all_p_values = []
all_op_values = []

total_rows = df_transformed.height

for i, row in enumerate(df_transformed.iter_rows(named=True)):
    if i % 100 == 0:
        print(f"  Processing row {i}/{total_rows}...")
    
    minute_grids, p_values, op_values = expand_minutes(row)
    all_minute_grids.extend(minute_grids)
    all_p_values.extend(p_values)
    all_op_values.extend(op_values)

print(f"\nTotal 1-minute rows: {len(all_minute_grids):,}")

minute_df = pl.DataFrame({
    "MinuteGrid": all_minute_grids,
    "P_Value": all_p_values,
    "OP_Value": all_op_values
})

minute_df = minute_df.with_columns([
    pl.when(pl.col("P_Value").is_nan())
      .then(None)
      .otherwise(pl.col("P_Value"))
      .alias("P_Value"),
    pl.when(pl.col("OP_Value").is_nan())
      .then(None)
      .otherwise(pl.col("OP_Value"))
      .alias("OP_Value")
])

print("\n1-minute data sample:")
print(minute_df.head(20))

# ============================================
# 6. Create 15-minute buckets
# ============================================
minute_df = minute_df.with_columns(
    (pl.col("MinuteGrid") - pl.duration(minutes=1))
    .dt.truncate("15m")
    .alias("Bucket")
)

# ============================================
# 7. Aggregate to 15-minute buckets
# ============================================
result = minute_df.group_by("Bucket").agg([
    pl.col("P_Value").sum().alias("P_Usage"),
    pl.col("OP_Value").sum().alias("OP_Usage"),
    pl.col("MinuteGrid").min().alias("MinDateTime"),
    pl.col("MinuteGrid").max().alias("MaxDateTime"),
    pl.col("MinuteGrid").count().alias("Minutes")
]).sort("Bucket")

print("\nFinal 15-minute buckets:")
print(result.head(20))

# ============================================
# 8. Verify bucket alignment at boundaries
# ============================================
print("\nVerifying 06:30 and 23:30 boundaries...")
boundary_check = result.filter(
    ((pl.col("Bucket").dt.hour() == 6) & (pl.col("Bucket").dt.minute() == 30)) |
    ((pl.col("Bucket").dt.hour() == 23) & (pl.col("Bucket").dt.minute() == 30)) |
    ((pl.col("Bucket").dt.hour() == 6) & (pl.col("Bucket").dt.minute() == 15)) |
    ((pl.col("Bucket").dt.hour() == 23) & (pl.col("Bucket").dt.minute() == 15))
)
print(boundary_check.head(20))

# ============================================
# 9. Export for Power BI (PARQUET)
# ============================================
result.write_parquet(OUTPUT_FILE_PARQUET)
print(f"\n✅ Saved to {OUTPUT_FILE_PARQUET} ({result.height:,} rows)")

# ============================================
# 10. Summary stats
# ============================================
print("\n" + "=" * 50)
print("SUMMARY")
print("=" * 50)
print(f"Total P Usage:  {result['P_Usage'].sum():,.2f}")
print(f"Total OP Usage: {result['OP_Usage'].sum():,.2f}")
print(f"Date Range:     {result['Bucket'].min()} to {result['Bucket'].max()}")
print(f"Total Buckets:  {result.height:,}")
print(f"Start Row:      {START_ROW}")
print(f"Parquet saved:  {OUTPUT_FILE_PARQUET}")
print(f"Raw CSV saved:  {OUTPUT_FILE_RAW_CSV}")
print("=" * 50)
