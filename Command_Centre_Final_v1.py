import os
import pandas as pd
from datetime import datetime
import re
from sqlalchemy import create_engine
import pytz

# Folder where Flask API saves uploaded files
UPLOAD_DIR = os.path.join(os.path.dirname(__file__), "uploads")

data_dump_path = os.path.join(UPLOAD_DIR, "Digital Dashboard Queue Names Data Dump.xlsx")
layout_path    = os.path.join(UPLOAD_DIR, "Digital Dashboard Layout + Requirements.xlsx")
reso_dump_path = os.path.join(UPLOAD_DIR, "Resolution Queue Volume Data.xlsx")
reso_map_path  = os.path.join(UPLOAD_DIR, "Reso Doc Types vs Processing Queue Names.xlsx")
boa_path       = os.path.join(UPLOAD_DIR, "BOA - Time Off Work.xlsm")

pro_pf_path  = os.path.join(UPLOAD_DIR, "Personal Folder.xlsx")
reso_pf_path = os.path.join(UPLOAD_DIR, "ECISS Resolutions Personal Folder.xlsx")

fte_target_path = os.path.join(UPLOAD_DIR, "CIF BOA Official Scorecard.xlsx")
attendance_path = os.path.join(UPLOAD_DIR, "Attendance.xlsx")
workqueue_mapping_path = os.path.join(UPLOAD_DIR, "Work Queue Team Mapping.xlsx")
reso_completed_path = os.path.join(UPLOAD_DIR, "Resolution Completed Volumes.xlsx")

calendar_path = os.path.join(UPLOAD_DIR, "Calendar of Events.xlsx")

prod_extract_path = os.path.join(UPLOAD_DIR, "Productivity Extract.csv")

last_updated_txt_path = os.path.join(UPLOAD_DIR, "last_updated.txt")

def clean_columns(df):
    df.columns = (
        df.columns.astype(str)
        .str.strip()
        .str.replace("\xa0", " ", regex=False)
        .str.replace(r"\s+", " ", regex=True)
    )
    return df

def safe_numeric(series_or_scalar, output_df):
    if isinstance(series_or_scalar, (int, float)):
        return pd.Series([series_or_scalar] * len(output_df), index=output_df.index)
    return pd.to_numeric(series_or_scalar, errors="coerce").fillna(0)

dump_wb = pd.ExcelFile(data_dump_path)
data_dump_df = pd.read_excel(dump_wb, sheet_name="ag-grid")

reso_df = pd.read_excel(reso_dump_path, sheet_name="ag-grid")
reso_map_df = pd.read_excel(reso_map_path, sheet_name="Added by Charmaine")

pro_pf_df = pd.read_excel(pro_pf_path)
reso_pf_df = pd.read_excel(reso_pf_path)

# --- Normalize Lock Status ---
data_dump_df["Lock Status"] = (
    data_dump_df["Lock Status"]
    .astype(str)
    .str.strip()
    .str.upper()
    .replace({"Y": "LOCKED", "LOCKED": "LOCKED"})
)

def get_last_modified_est_from_txt(txt_path):
    """
    Reads 'last_updated.txt', extracts 'Modified (PHT)' or 'Updated on (PHT)',
    converts it to EST (Eastern Standard Time), and returns as 'YYYY-MM-DD HH:MM:SS'.
    """
    try:
        with open(txt_path, 'r', encoding='utf-8') as f:
            content = f.read()

        # Prefer "Modified (PHT)" over "Updated on (PHT)" if both exist
        match = re.search(r"Modified \(PHT\):\s*([0-9:\-\s]+)", content)
        if not match:
            match = re.search(r"Updated on \(PHT\):\s*([0-9:\-\s]+)", content)

        if match:
            timestamp_str = match.group(1).strip()
            # Parse as naive datetime in PHT timezone
            pht_tz = pytz.timezone("Asia/Manila")
            est_tz = pytz.timezone("US/Eastern")

            dt_pht = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")
            dt_pht = pht_tz.localize(dt_pht)
            dt_est = dt_pht.astimezone(est_tz)

            return dt_est.strftime("%Y-%m-%d %H:%M:%S")
        else:
            print("Warning: No '(PHT)' timestamp found in last_updated.txt. Using current time (EST).")
            est_tz = pytz.timezone("US/Eastern")
            return datetime.now(est_tz).strftime("%Y-%m-%d %H:%M:%S")

    except Exception as e:
        print(f"Warning: Could not read last_updated.txt — {e}")
        est_tz = pytz.timezone("US/Eastern")
        return datetime.now(est_tz).strftime("%Y-%m-%d %H:%M:%S")

def map_personal_folder_counts_pro(folder_df):
    """
    Compute number of unique Document IDs per Queue directly from the Personal Folder dump.
    """
    folder_df = folder_df.copy()
    folder_df["Queue"] = folder_df["Queue"].astype(str).str.strip()

    counts = (
        folder_df.groupby("Queue")["Document ID"]
        .nunique()
        .reset_index(name="PFCount")
    )

    # Return as a dictionary {QueueName: Count}
    return counts.set_index("Queue")["PFCount"].to_dict()

def map_personal_folder_counts_reso(folder_df, reso_map_df):
    """
    For RESO Personal Folder dump — map Doc Type to Queue Description.
    Returns a dict: {Queue_Desc: unique document count}
    """
    folder_df = folder_df.copy()
    folder_df["Doc Type"] = folder_df["Doc Type"].astype(str).str.strip()
    reso_map_df["Doc_Type"] = reso_map_df["Doc_Type"].astype(str).str.strip()
    reso_map_df["Queue_Desc"] = reso_map_df["Queue_Desc"].astype(str).str.strip()

    counts = (
        folder_df.groupby("Doc Type")["Document ID"]
        .nunique()
        .reset_index(name="PFCount")
    )

    mapped = pd.merge(
        counts,
        reso_map_df[["Doc_Type", "Queue_Desc"]],
        left_on="Doc Type",
        right_on="Doc_Type",
        how="left"
    )

    return mapped.groupby("Queue_Desc")["PFCount"].sum().to_dict()

def map_resolutions_completed(reso_completed_path):
    """
    Load Resolutions Completed Volumes file and count unique Document IDs per Doc Type.
    Returns a dict: {Doc Type: unique count of Document ID}
    """
    try:
        df = pd.read_excel(reso_completed_path, usecols=[1, 2])  # Columns B and C
        df.columns = ["Document ID", "Doc Type"]
        df["Document ID"] = df["Document ID"].astype(str).str.strip()
        df["Doc Type"] = df["Doc Type"].astype(str).str.strip()

        # Count unique Document IDs per Doc Type
        counts = (
            df.groupby("Doc Type")["Document ID"]
            .nunique()
            .reset_index(name="ResolutionsCompletedCount")
        )

        return counts.set_index("Doc Type")["ResolutionsCompletedCount"].to_dict()

    except Exception as e:
        print("Warning: Could not load Resolutions Completed Volumes file:", e)
        return {}
    
def map_queue_aging(data_dump_df):
    """
    Compute count of documents older than 60 minutes for both PRO and QC queues.
    Returns two dicts: aging_pro_map and aging_qc_map
    """
    try:
        df = data_dump_df.copy()
        df["Queue"] = df["Queue"].astype(str).str.strip()
        df["Entry Date"] = pd.to_datetime(df["Entry Date"], errors="coerce")

        now = pd.Timestamp.now()

        # Compute age in minutes for each document
        df["AgingMinutes"] = (now - df["Entry Date"]).dt.total_seconds() / 60

        # Only consider entries older than 60 minutes
        aged_df = df[df["AgingMinutes"] > 60].copy()

        # Normalize keys
        aged_df["QueueKey"] = (
            aged_df["Queue"]
            .astype(str)
            .str.upper()
            .str.replace(r"\s+", "", regex=True)
            .str.strip()
        )

        # Split PRO and QC queues
        aged_df["Is_QC"] = aged_df["Queue"].str.endswith("QC", na=False)
        aged_df["BaseQueueKey"] = (
            aged_df["Queue"]
            .str.replace(r"QC$", "", regex=True)
            .str.upper()
            .str.replace(r"\s+", "", regex=True)
            .str.strip()
        )

        # Count aged documents for PRO and QC separately
        aging_pro_map = (
            aged_df[~aged_df["Is_QC"]]
            .groupby("QueueKey")["Document ID"]
            .nunique()
            .astype(int)
            .to_dict()
        )

        aging_qc_map = (
            aged_df[aged_df["Is_QC"]]
            .groupby("BaseQueueKey")["Document ID"]
            .nunique()
            .astype(int)
            .to_dict()
        )

        return aging_pro_map, aging_qc_map

    except Exception as e:
        print("Warning: Could not compute Aging values:", e)
        return {}, {}

# Helper to compute PRO and QC Backlogs (with totals by category)
def compute_backlogs(data_dump_df, layout_path, layout_type="", layout_sheet="Backlogs"):
    """
    Compute PRO and QC Backlogs with full queue details and accurate category totals.
    Includes threshold rules:
      - Financial: backlog if Entry Date ≠ today
      - Non-Financial:
          PRO backlog if Entry Date + 1 business day < today
          QC backlog if Entry Date + 2 business days < today
    """
    import pytz
    est = pytz.timezone("US/Eastern")

    df = data_dump_df.copy()
    df["Entry Date"] = pd.to_datetime(df["Entry Date"], errors="coerce")

    # Localize to EST if timezone-naive
    df["Entry Date"] = df["Entry Date"].apply(
        lambda x: est.localize(x) if pd.notna(x) and x.tzinfo is None else x
    )

    df["Queue"] = df["Queue"].astype(str).str.strip()
    df["Is_QC"] = df["Queue"].str.endswith("QC", na=False)

    today = pd.Timestamp.now(tz=est).normalize()

    # Determine Financial/Non-Financial by layout type
    if layout_type.upper() == "HNW":
        financial_keywords = ["INSTITUTIONAL", "APP INVESTMENT", "UNITED"]
    else:
        financial_keywords = ["FINANCIAL"]

    def get_category(queue):
        q = str(queue).upper()
        for word in financial_keywords:
            if word in q:
                return "FINANCIAL"
        return "NON-FINANCIAL"

    df["CategoryType"] = df["Queue"].apply(get_category)

    # Apply threshold logic
    backlog_flags = []
    for _, row in df.iterrows():
        entry = row["Entry Date"]
        if pd.isna(entry):
            backlog_flags.append(False)
            continue

        cat = row["CategoryType"]
        is_qc = row["Is_QC"]

        if cat == "FINANCIAL":
            backlog_flags.append(entry.normalize() < today)
        else:
            if not is_qc:  # PRO
                threshold_date = entry + pd.offsets.BDay(1)
            else:  # QC
                threshold_date = entry + pd.offsets.BDay(2)
            backlog_flags.append(threshold_date < today)

    df["IsBacklog"] = backlog_flags
    backlog_df = df[df["IsBacklog"]].copy()

    # Compute raw backlog counts
    pro_backlogs = (
        backlog_df[~backlog_df["Is_QC"]]
        .groupby("Queue")["Document ID"]
        .nunique()
        .reset_index(name="PRO Backlogs")
    )

    qc_backlogs = (
        backlog_df[backlog_df["Is_QC"]]
        .assign(BaseQueue=lambda x: x["Queue"].str.replace(r"QC$", "", regex=True).str.strip())
        .groupby("BaseQueue")["Document ID"]
        .nunique()
        .reset_index(name="QC Backlogs")
    )

    backlog_summary = pd.merge(
        pro_backlogs.rename(columns={"Queue": "QueueName"}),
        qc_backlogs.rename(columns={"BaseQueue": "QueueName"}),
        on="QueueName",
        how="outer"
    ).fillna(0)

    backlog_summary["PRO Backlogs"] = backlog_summary["PRO Backlogs"].astype(int)
    backlog_summary["QC Backlogs"] = backlog_summary["QC Backlogs"].astype(int)
    backlog_summary["QueueKey"] = (
        backlog_summary["QueueName"].astype(str).str.upper().str.replace(r"\s+", "", regex=True)
    )

    # Load layout
    layout_wb = pd.ExcelFile(layout_path)
    layout_df = pd.read_excel(layout_wb, sheet_name=layout_sheet, header=None)
    header_row_idx = layout_df[
        layout_df.apply(lambda row: row.astype(str).str.contains("PRO Backlogs", case=False).any(), axis=1)
    ].index[0]

    layout_columns = [str(c).strip().replace("\xa0", " ") for c in layout_df.iloc[header_row_idx].tolist()]
    formatted_backlogs = layout_df.iloc[header_row_idx + 1:].reset_index(drop=True)
    formatted_backlogs.columns = layout_columns

    # Identify queue column
    queue_col = None
    for col in formatted_backlogs.columns:
        if "queue" in col.lower():
            queue_col = col
            break
    if not queue_col:
        queue_col = "QueueName"

    formatted_backlogs.rename(columns={queue_col: "QueueName"}, inplace=True)
    formatted_backlogs["QueueName"] = formatted_backlogs["QueueName"].astype(str).str.strip()
    formatted_backlogs["QueueKey"] = (
        formatted_backlogs["QueueName"].astype(str).str.upper().str.replace(r"\s+", "", regex=True)
    )

    # Map backlog counts
    formatted_backlogs["PRO Backlogs"] = formatted_backlogs["QueueKey"].map(
        backlog_summary.set_index("QueueKey")["PRO Backlogs"]
    ).fillna(0).astype(int)

    formatted_backlogs["QC Backlogs"] = formatted_backlogs["QueueKey"].map(
        backlog_summary.set_index("QueueKey")["QC Backlogs"]
    ).fillna(0).astype(int)

    # --- Recalculate totals by category ---
    if layout_type.upper() == "HNW":
        category_headers = [
            "INSTITUTIONAL - Total",
            "APP INVESTMENT - Total",
            "UNITED FINANCIALS - Total",
            "Other - Total",
        ]
    else:
        category_headers = [
            "FINANCIAL - Total",
            "QUASI NON-FINANCIAL - Total",
            "NON-FINANCIAL - Total",
            "Other - Total",
        ]

    queue_list = list(formatted_backlogs["QueueName"])

    for i, q in enumerate(queue_list):
        if q in category_headers:
            next_index = None
            for j in range(i + 1, len(queue_list)):
                if queue_list[j] in category_headers or queue_list[j] == "Grand Total by Queue:":
                    next_index = j
                    break
            if next_index:
                child_rows = formatted_backlogs.iloc[i + 1:next_index]
            else:
                child_rows = formatted_backlogs.iloc[i + 1:]

            formatted_backlogs.loc[formatted_backlogs["QueueName"] == q, "PRO Backlogs"] = child_rows["PRO Backlogs"].sum()
            formatted_backlogs.loc[formatted_backlogs["QueueName"] == q, "QC Backlogs"] = child_rows["QC Backlogs"].sum()

    # Compute "Other - Total" section totals
    other_rows = [
        "Other - Total",
        "Incoming Fax Queue",
        "Incoming Email Queue",
        "Index Queue",
        "Doc Translation",
        "Reso Validation",
        "RMA",
        "United Doc Translation"
    ]
    other_totals = formatted_backlogs.loc[
        formatted_backlogs["QueueName"].isin(other_rows[1:])
    ][["PRO Backlogs", "QC Backlogs"]].sum(numeric_only=True)

    for col in ["PRO Backlogs", "QC Backlogs"]:
        formatted_backlogs.loc[formatted_backlogs["QueueName"] == "Other - Total", col] = other_totals[col]

    # Compute Grand Total
    total_rows = formatted_backlogs[
        formatted_backlogs["QueueName"].isin(category_headers)
    ]
    grand_pro = total_rows["PRO Backlogs"].sum()
    grand_qc = total_rows["QC Backlogs"].sum()

    formatted_backlogs.loc[
        formatted_backlogs["QueueName"].str.strip().str.lower() == "grand total by queue:",
        ["PRO Backlogs", "QC Backlogs"]
    ] = [grand_pro, grand_qc]

    formatted_backlogs = formatted_backlogs.fillna(0)
    formatted_backlogs = formatted_backlogs[["QueueName", "PRO Backlogs", "QC Backlogs"]]
    formatted_backlogs.reset_index(drop=True, inplace=True)

    return formatted_backlogs

# Helper to load and summarize Productivity Extract
def load_productivity_extract():
    try:
        prod_df = pd.read_csv(prod_extract_path, header=None)

        # Columns: D (QueueName), E (Accepted), F (Processed), W (QC Pass), X (QC Error)
        prod_df = prod_df.iloc[:, [3, 4, 5, 22, 23]]
        prod_df.columns = [
            "QueueName",
            "Accepted Volumes",
            "Processed Volumes",
            "QC Pass",
            "QC Error",
        ]

        # --- Clean queue names ---
        prod_df["QueueName"] = (
            prod_df["QueueName"].astype(str).str.strip().str.replace(r"\s+", " ", regex=True)
        )

        # --- Clean numeric fields ---
        for col in ["Accepted Volumes", "Processed Volumes", "QC Pass", "QC Error"]:
            prod_df[col] = (
                prod_df[col]
                .astype(str)
                .str.replace(",", "", regex=False)
                .str.replace("%", "", regex=False)
                .str.strip()
            )
            prod_df[col] = pd.to_numeric(prod_df[col], errors="coerce").fillna(0)

        # --- Combine QC Pass + QC Error ---
        prod_df["QC'ed Volumes"] = prod_df["QC Pass"] + prod_df["QC Error"]

        # --- Aggregate totals per Queue ---
        summary = (
            prod_df.groupby("QueueName", as_index=False)[
                ["Accepted Volumes", "Processed Volumes", "QC'ed Volumes"]
            ]
            .sum()
        )

        return summary

    except Exception as e:
        print("Warning: Could not load Productivity Extract file:", e)
        return pd.DataFrame(
            columns=["QueueName", "Accepted Volumes", "Processed Volumes", "QC'ed Volumes"]
        )

def load_fte_target_times():
    try:
        target_df = pd.read_excel(fte_target_path, sheet_name="Target Ave Time", header=None)
        queue_col = target_df.iloc[:, 0]
        avg_time_col = target_df.iloc[:, 5]

        df = pd.DataFrame({"QueueName": queue_col, "AvgTime": avg_time_col}).dropna(subset=["QueueName", "AvgTime"])

        # Normalize queue names: uppercase, remove extra spaces, unify case
        df["QueueName"] = (
            df["QueueName"]
            .astype(str)
            .str.upper()
            .str.strip()
            .str.replace(r"\s+", "", regex=True)  # remove all spaces
        )

        df["AvgTime"] = pd.to_numeric(df["AvgTime"], errors="coerce").fillna(0)
        return dict(zip(df["QueueName"], df["AvgTime"]))

    except Exception as e:
        print("Warning: Could not load FTE target times:", e)
        return {}

def process_capacity_table(df_gdc):
    """
    Build Capacity table using Attendance and Work Queue Team Mapping.
    Columns: Team, Projected, Required, Actuals
    """
    try:
        # --- Load Attendance ---
        attendance_df = pd.read_excel(attendance_path)
        attendance_df.columns = [c.strip() for c in attendance_df.columns]
        attendance_df["Attendance (Y/N)"] = attendance_df["Attendance (Y/N)"].astype(str).str.upper().str.strip()

        # Count only 'Y' entries
        attendance_df = attendance_df[attendance_df["Attendance (Y/N)"].str.startswith("Y")]
        actuals = (
            attendance_df.groupby("Department")["Processors"]
            .nunique()
            .reset_index()
            .rename(columns={"Department": "Team", "Processors": "Actuals"})
        )

        # --- Load Work Queue Team Mapping (no headers in sheet) ---
        team_map_df = pd.read_excel(workqueue_mapping_path, header=None, usecols=[0, 1])
        team_map_df.columns = ["QueueName", "Team"]

        # Drop blanks and accidental headers
        team_map_df = team_map_df.dropna(subset=["QueueName", "Team"])

        # Clean and normalize text
        team_map_df["QueueName"] = team_map_df["QueueName"].astype(str).str.strip()
        team_map_df["Team"] = team_map_df["Team"].astype(str).str.strip()

        # Remove header-like rows (e.g., 'QueueName' or 'Team')
        team_map_df = team_map_df[
            ~team_map_df["QueueName"].str.upper().isin(["QUEUENAME", "QUEUE", "TEAM"])
        ]
        team_map_df = team_map_df[
            ~team_map_df["Team"].str.upper().isin(["TEAM", "DEPARTMENT", "GROUP"])
        ]

        team_map_df["QueueKey"] = (
            team_map_df["QueueName"].astype(str).str.upper().str.replace(r"\s+", "", regex=True)
        )


        # --- Compute Required (sum of FTE per queue → grouped by team) ---
        fte_target_map = load_fte_target_times()

        df_gdc = df_gdc.reset_index()
        df_gdc["QueueKey"] = df_gdc["QueueName"].astype(str).str.upper().str.replace(r"\s+", "", regex=True)
        df_gdc["PRO Queue"] = pd.to_numeric(df_gdc["PRO Queue"], errors="coerce").fillna(0)

        def compute_fte(row):
            avg_time = fte_target_map.get(row["QueueKey"], 0)
            if avg_time > 0 and row["PRO Queue"] > 0:
                return round((row["PRO Queue"] * avg_time) / 420, 3)
            return 0

        df_gdc["QueueFTE"] = df_gdc.apply(compute_fte, axis=1)

        merged = pd.merge(df_gdc, team_map_df, on="QueueKey", how="left")
        required = merged.groupby("Team")["QueueFTE"].sum().reset_index().rename(columns={"QueueFTE": "Required"})

        # --- Combine both ---
        capacity_df = pd.merge(required, actuals, on="Team", how="outer").fillna(0)
        capacity_df["Projected"] = 0
        capacity_df = capacity_df[["Team", "Projected", "Required", "Actuals"]]

        return capacity_df

    except Exception as e:
        print("Warning: Could not build Capacity table:", e)
        return pd.DataFrame(columns=["Team", "Projected", "Required", "Actuals"])

# --- Processing function for layout sheets (GDC & HNW) ---
def process_layout_sheet(sheet_name, category_headers):
    layout_wb = pd.ExcelFile(layout_path)
    layout_df = pd.read_excel(layout_wb, sheet_name=sheet_name, header=None)

    header_row_idx = layout_df[layout_df.apply(
        lambda row: row.astype(str).str.contains("PRO Queue", case=False).any(),
        axis=1
    )].index[0]

    columns = layout_df.iloc[header_row_idx].tolist()
    columns = [str(c).strip().replace("\xa0", " ") for c in columns]

    output_df = layout_df.iloc[header_row_idx+1:].reset_index(drop=True)
    output_df.columns = columns
    output_df = clean_columns(output_df)

    if "QueueName" not in output_df.columns:
        output_df.insert(0, "QueueName", layout_df.iloc[header_row_idx+1:, 0].reset_index(drop=True))

    output_df["QueueName"] = (
        output_df["QueueName"].astype(str)
        .str.strip()
        .str.replace(r"\s+", " ", regex=True)
    )

    # Normalize both Queue and QueueName for matching
    pro_entries = data_dump_df[~data_dump_df["Queue"].astype(str).str.endswith("QC", na=False)].copy()
    pro_entries["QueueKey"] = (
        pro_entries["Queue"].astype(str)
        .str.upper()
        .str.replace(r"\s+", "", regex=True)
        .str.strip()
)

    # Locked PRO entries
    pro_locked_entries = pro_entries[pro_entries["Lock Status"] == "LOCKED"].copy()
    pro_locked_entries["QueueKey"] = pro_locked_entries["QueueKey"]

    # Count total and locked PROs
    pro_counts = pro_entries.groupby("QueueKey")["Document ID"].nunique().to_dict()
    pro_locked_counts = pro_locked_entries.groupby("QueueKey")["Document ID"].nunique().to_dict()

    # Normalize layout queue names
    output_df["QueueKey"] = (
        output_df["QueueName"].astype(str)
        .str.upper()
        .str.replace(r"\s+", "", regex=True)
        .str.strip()
    )

    # Map counts
    output_df["PRO Queue"] = output_df["QueueKey"].map(pro_counts).fillna(0).astype(int)
    output_df["User Locked PRO"] = output_df["QueueKey"].map(pro_locked_counts).fillna(0).astype(int)


    # --- Normalize QC queues (BaseQueueKey)
    qc_entries = data_dump_df[data_dump_df["Queue"].astype(str).str.endswith("QC", na=False)].copy()
    qc_entries["BaseQueueKey"] = (
        qc_entries["Queue"].astype(str)
        .str.replace(r"QC$", "", regex=True)
        .str.upper()
        .str.replace(r"\s+", "", regex=True)
        .str.strip()
    )

    # Count QC volumes
    qc_counts = qc_entries.groupby("BaseQueueKey")["Document ID"].nunique().to_dict()

    # Count locked QC volumes (using same normalized key)
    qc_locked_entries = qc_entries[qc_entries["Lock Status"] == "LOCKED"].copy()
    qc_locked_counts = qc_locked_entries.groupby("BaseQueueKey")["Document ID"].nunique().to_dict()

    # Map both to layout queues
    output_df["QC Queue"] = output_df["QueueKey"].map(qc_counts).fillna(0).astype(int)
    output_df["User Locked QC"] = output_df["QueueKey"].map(qc_locked_counts).fillna(0).astype(int)



    processed_counts = data_dump_df.groupby("Queue")["Document ID"].nunique().to_dict()
    output_df["Processed Volumes"] = output_df["QueueName"].map(processed_counts).fillna(0).astype(int)


    prod_summary = load_productivity_extract()
    if not prod_summary.empty:
        merged = pd.merge(output_df, prod_summary, on="QueueName", how="left", suffixes=("", "_prod"))
        if "Accepted Volumes_prod" in merged.columns:
            merged["Accepted Volumes"] = pd.to_numeric(
                merged["Accepted Volumes_prod"], errors="coerce"
            ).fillna(pd.to_numeric(merged["Accepted Volumes"], errors="coerce")).fillna(0).astype(int)

        if "Processed Volumes_prod" in merged.columns:
            merged["Processed Volumes"] = pd.to_numeric(
                merged["Processed Volumes_prod"], errors="coerce"
            ).fillna(pd.to_numeric(merged["Processed Volumes"], errors="coerce")).fillna(0).astype(int)

        if "QC'ed Volumes_prod" in merged.columns:
            merged["QC'ed Volumes"] = pd.to_numeric(
                merged["QC'ed Volumes_prod"], errors="coerce"
            ).fillna(pd.to_numeric(merged.get("QC'ed Volumes", 0), errors="coerce")).fillna(0).astype(int)


        merged = merged.drop(columns=[c for c in merged.columns if c.endswith("_prod")], errors="ignore")
        output_df = merged


    if sheet_name == "CC Full View of GDC+GTA screen1":
        reso_counts = reso_df.groupby("Doc Type")["Doc ID"].nunique().reset_index(name="ResoCount")
        reso_with_desc = pd.merge(reso_counts, reso_map_df[["Doc_Type", "Queue_Desc"]],
                                  left_on="Doc Type", right_on="Doc_Type", how="left")
        reso_final_counts = reso_with_desc.groupby("Queue_Desc")["ResoCount"].sum().to_dict()
        output_df["Reso Queue"] = output_df["QueueName"].map(reso_final_counts).fillna(0).astype(int)


    pro_pf_counts = map_personal_folder_counts_pro(pro_pf_df)
    if "PRO Personal Folders" in output_df.columns:
        output_df["PRO Personal Folders"] = (
            output_df["QueueName"].map(pro_pf_counts).fillna(0).astype(int)
        )


    reso_pf_counts = map_personal_folder_counts_reso(reso_pf_df, reso_map_df)
    if "RESO Personal Folders" in output_df.columns:
        output_df["RESO Personal Folders"] = (
            output_df["QueueName"].map(reso_pf_counts).fillna(0).astype(int)
        )

    # Compute Aging counts separately for PRO and QC queues (older than 60 mins)
    aging_pro_map, aging_qc_map = map_queue_aging(data_dump_df)

    output_df["Aging_PRO"] = (
        output_df["QueueKey"]
        .map(aging_pro_map)
        .fillna(0)
        .astype(int)
    )

    output_df["Aging_QC"] = (
        output_df["QueueKey"]
        .map(aging_qc_map)
        .fillna(0)
        .astype(int)
    )


    reso_completed_map = map_resolutions_completed(reso_completed_path)

    # Normalize Queue names to uppercase (same key style as reso_completed_map)
    output_df["Resolutions Completed Volumes"] = (
        output_df["QueueName"]
        .astype(str)
        .str.strip()
        .map(lambda q: reso_completed_map.get(q, 0))
    )


    queue_to_category = {}
    current_cat = None
    for q in output_df["QueueName"]:
        if q in category_headers:
            current_cat = q
        elif q == "Grand Total by Queue:":
            continue
        elif current_cat is not None:
            queue_to_category[q] = current_cat
    output_df["Category"] = output_df["QueueName"].map(queue_to_category)

    numeric_metrics = [c for c in [
        "PRO Queue", "QC Queue", "User Locked PRO", "User Locked QC",
        "Processed Volumes", "Reso Queue",
        "PRO Personal Folders", "RESO Personal Folders",
        "Accepted Volumes", "QC'ed Volumes",
        "Resolutions Completed Volumes",
        "Aging_PRO", "Aging_QC"
    ] if c in output_df.columns]


    fte_target_map = load_fte_target_times()

    def compute_fte_required(row):
        queue = str(row["QueueName"]).upper().replace(" ", "").strip()
        pro_count = row.get("PRO Queue", 0)
        avg_time = fte_target_map.get(queue, 0)
        if avg_time > 0 and pro_count > 0:
            # exact computation without rounding
            fte = (pro_count * avg_time) / 420
            return round(fte, 2)  # keep two decimals for precision
        return 0


    output_df["FTE Required"] = output_df.apply(compute_fte_required, axis=1)

    # --- Roll up totals safely per category (using position-based slicing)
    queue_list = list(output_df["QueueName"])

    for i, q in enumerate(queue_list):
        if q in category_headers:
            # find where the next category header or 'Grand Total' starts
            next_index = None
            for j in range(i + 1, len(queue_list)):
                if queue_list[j] in category_headers or queue_list[j] == "Other - Total" or queue_list[j] == "Grand Total by Queue:":
                    next_index = j
                    break

            # define slice (rows between this header and the next header)
            if next_index:
                child_rows = output_df.iloc[i + 1:next_index]
            else:
                child_rows = output_df.iloc[i + 1:]

            # Compute sums for all numeric metrics including Aging (since Aging is now a count)
            totals = child_rows[numeric_metrics].sum(numeric_only=True)

            # Write the totals back to the parent row (including Aging)
            for col in numeric_metrics:
                output_df.loc[output_df["QueueName"] == q, col] = totals.get(col, 0)

            # roll up FTE Required safely
            if "FTE Required" in output_df.columns:
                output_df.loc[output_df["QueueName"] == q, "FTE Required"] = child_rows["FTE Required"].sum()


    special_map = {
        "Doc Translation": "DocTranslation",
        "Reso Validation": "ResolutionValidation",
        "RMA": "ResolutionManagerApproval",
        "Index Queue": "General Index"
    }
    special_counts = (
        data_dump_df[data_dump_df["Queue"].isin(special_map.values())]
        .groupby("Queue")["Document ID"].nunique().to_dict()
    )
    for label, queue_val in special_map.items():
        if label in output_df["QueueName"].values:
            output_df.loc[output_df["QueueName"] == label, "PRO Queue"] = special_counts.get(queue_val, 0)

    other_section_rows = [
        "Other - Total",
        "Incoming Fax Queue",
        "Incoming Email Queue",
        "Index Queue",
        "Doc Translation",
        "Reso Validation",
        "RMA",
        "United Doc Translation"
    ]
    other_totals = output_df.loc[output_df["QueueName"].isin(other_section_rows[1:]), numeric_metrics].sum(numeric_only=True)
    for col in numeric_metrics:
        output_df.loc[output_df["QueueName"] == "Other - Total", col] = other_totals.get(col, 0)
    for row in other_section_rows:
        for col in numeric_metrics:
            if col != "PRO Queue":
                output_df.loc[output_df["QueueName"] == row, col] = 0

    grand_total_sources = set(category_headers) | {"Other - Total"}
    grand_totals = output_df.loc[output_df["QueueName"].isin(grand_total_sources), numeric_metrics].sum(numeric_only=True)
    for col in numeric_metrics:
        output_df.loc[output_df["QueueName"] == "Grand Total by Queue:", col] = grand_totals.get(col, 0)

    # Include FTE Required in Grand Total
    if "FTE Required" in output_df.columns:
        output_df.loc[output_df["QueueName"] == "Grand Total by Queue:", "FTE Required"] = \
            output_df.loc[output_df["QueueName"].isin(category_headers), "FTE Required"].sum()


    if "Total" in output_df.columns:
        output_df["Total"] = (
            safe_numeric(output_df.get("PRO Queue", 0), output_df)
            + safe_numeric(output_df.get("QC Queue", 0), output_df)
        )
    if "Total_1" in output_df.columns and "Reso Queue" in output_df.columns:
        output_df["Total_1"] = safe_numeric(output_df.get("Reso Queue", 0), output_df)
    if "Total_2" in output_df.columns:
        output_df["Total_2"] = (
            safe_numeric(output_df.get("PRO Personal Folders", 0), output_df)
            + safe_numeric(output_df.get("PRO FTE Locked", 0), output_df)
            + safe_numeric(output_df.get("RESO Personal Folders", 0), output_df)
            + safe_numeric(output_df.get("RESO FTE Locked", 0), output_df)
        )


    time_now = datetime.now().strftime("%I:%M %p")
    output_df = output_df.rename(
        columns={col: f"Bulletin Board (Generated at {time_now})"
                 for col in output_df.columns if "Bulletin Board" in col}
    )

    if "Category" in output_df.columns:
        output_df = output_df.drop(columns=["Category"])
    output_df = output_df.loc[:, ~output_df.columns.str.contains("nan", case=False)]
    output_df = clean_columns(output_df)

    if "QueueName" in output_df.columns:
        output_df.index = output_df["QueueName"]
        output_df = output_df.drop(columns=["QueueName"])

    return output_df

def process_executive_view(df_gdc, df_hnw):
    time_now = datetime.now().strftime("%I:%M %p")
    columns = [
        "Executive View",
        "Total Outstanding Processing Volumes",
        "Total Outstanding Quality Control Volumes",
        f"Bulletin Board (Generated at {time_now})"
    ]
    exec_rows = []

    # --- GDC/GTA ---
    gdc_cats = ["FINANCIAL - Total", "QUASI NON-FINANCIAL - Total", "NON-FINANCIAL - Total"]
    gdc_proc = df_gdc.loc[gdc_cats, "PRO Queue"].sum()
    gdc_qc   = df_gdc.loc[gdc_cats, "QC Queue"].sum()
    exec_rows.append(["GDC/GTA Volumes", gdc_proc, gdc_qc, ""])
    for cat in gdc_cats:
        exec_rows.append([cat.replace(" - Total", ""), df_gdc.loc[cat, "PRO Queue"], df_gdc.loc[cat, "QC Queue"], ""])

    # --- HNW ---
    hnw_cats = ["INSTITUTIONAL - Total", "APP INVESTMENT - Total", "UNITED FINANCIALS - Total"]
    hnw_proc = df_hnw.loc[hnw_cats, "PRO Queue"].sum()
    hnw_qc   = df_hnw.loc[hnw_cats, "QC Queue"].sum()
    exec_rows.append(["HNW Volumes", hnw_proc, hnw_qc, ""])
    for cat in hnw_cats:
        exec_rows.append([cat.replace(" - Total", ""), df_hnw.loc[cat, "PRO Queue"], df_hnw.loc[cat, "QC Queue"], ""])

    # --- RESOLUTION NIGO ---
    if "Reso Queue" in df_gdc.columns:
        reso_proc = df_gdc.loc[gdc_cats, "Reso Queue"].sum()
        exec_rows.append(["RESOLUTION NIGO Volumes", reso_proc, 0, ""])
        for cat in gdc_cats:
            exec_rows.append([cat.replace(" - Total", ""), df_gdc.loc[cat, "Reso Queue"], 0, ""])

    # --- Other Queues ---
    other_rows = [
        "Other - Total",
        "Incoming Email Queue",
        "Incoming Fax Queue",
        "Index Queue",
        "Doc Translation",
        "Reso Validation",
        "RMA"
    ]
    if "Other - Total" in df_gdc.index:
        exec_rows.append(["Other Queues", df_gdc.loc["Other - Total", "PRO Queue"], 0, ""])
        for row in other_rows[1:]:
            if row in df_gdc.index:
                exec_rows.append([row, df_gdc.loc[row, "PRO Queue"], 0, ""])
            else:
                exec_rows.append([row, 0, 0, ""])

    df_exec = pd.DataFrame(exec_rows, columns=columns)
    return df_exec


def process_users_productivity():
    # --- Load USERS_Productivity screen2 from layout ---
    layout_df = pd.read_excel(layout_path, sheet_name="USERS_Productivity screen2", header=None)

    
    columns = layout_df.iloc[0].tolist()
    layout_df = layout_df[1:].reset_index(drop=True)
    layout_df.columns = [str(c).strip() for c in columns]

    # --- Load BOA MasterList ---
    boa_df = pd.read_excel(boa_path,
        sheet_name="MasterList_of_Members"
    )

    # --- Normalization helpers ---
    def normalize_agent_name(name):
        return str(name).strip().upper()

    def normalize_supervisor_name(name):
        base = re.sub(r"-.*", "", str(name)).strip()  
        if "," in base:  
            parts = base.split(",")
            last = parts[0].strip()
            first = parts[1].strip().split()[0]  
            return f"{first} {last}".upper()
        else:
            return base.upper()

    
    boa_df["Member_clean"] = boa_df["Member Name"].astype(str).apply(normalize_agent_name)
    boa_df["Supervisor_clean"] = boa_df["Supervisor"].astype(str).apply(normalize_supervisor_name)

    layout_df["Name_clean"] = layout_df[layout_df.columns[0]].astype(str).apply(normalize_agent_name)
    layout_df["Supervisor_clean"] = layout_df[layout_df.columns[0]].astype(str).apply(normalize_supervisor_name)

    shift_map = boa_df.set_index("Member_clean")["Shift Schedule"].to_dict()
    supervisor_shift_map = boa_df.set_index("Supervisor_clean")["Shift Schedule"].to_dict()

    layout_df["Shift Schedule"] = layout_df["Name_clean"].map(shift_map)
    layout_df["Shift Schedule"] = layout_df["Shift Schedule"].fillna(
        layout_df["Supervisor_clean"].map(supervisor_shift_map)
    )

    layout_df = layout_df.drop(columns=["Name_clean", "Supervisor_clean"], errors="ignore")

    # Update Bulletin Board column with timestamp
    time_now = datetime.now().strftime("%I:%M %p")
    layout_df = layout_df.rename(
        columns={col: f"Bulletin Board (Generated at {time_now})"
                 for col in layout_df.columns if "Bulletin Board" in col}
    )

    return layout_df

def process_calendar_events():
    cal_df = pd.read_excel(calendar_path, sheet_name="Events")

    cal_df.columns = [str(c).strip() for c in cal_df.columns]

    cal_df["Start Date"] = pd.to_datetime(
        cal_df["Start Day (YYYY-MM-DD)"].astype(str).str.strip() + " " + cal_df["Start Time (HH:MM)"].astype(str).str.strip(),
        errors="coerce"
    ).dt.strftime("%Y-%m-%dT%H:%M")

    cal_df["End Date"] = pd.to_datetime(
        cal_df["End Day (YYYY-MM-DD)"].astype(str).str.strip() + " " + cal_df["End Time (HH:MM)"].astype(str).str.strip(),
        errors="coerce"
    ).dt.strftime("%Y-%m-%dT%H:%M")

    cal_df = cal_df[["Event", "Start Date", "End Date"]]

    return cal_df

def process_announcements():
    ann_df = pd.read_excel(calendar_path, sheet_name="Announcements")

    # Clean column names
    ann_df.columns = [str(c).strip() for c in ann_df.columns]

    # Create standardized datetime strings
    ann_df["Start Date"] = pd.to_datetime(
        ann_df["Start Day (YYYY-MM-DD)"].astype(str).str.strip() + " " +
        ann_df["Start Time (HH:MM)"].astype(str).str.strip(),
        errors="coerce"
    ).dt.strftime("%Y-%m-%dT%H:%M")

    ann_df["End Date"] = pd.to_datetime(
        ann_df["End Day (YYYY-MM-DD)"].astype(str).str.strip() + " " +
        ann_df["End Time (HH:MM)"].astype(str).str.strip(),
        errors="coerce"
    ).dt.strftime("%Y-%m-%dT%H:%M")

    # Keep the key columns
    ann_df = ann_df[[
        "Title",
        "Message",
        "Description",
        "Severity (Warning, Info, Success)",
        "Start Date",
        "End Date"
    ]]

    # Clean missing or invalid data
    ann_df = ann_df.fillna("")

    return ann_df

df_gdc = process_layout_sheet(
    sheet_name="CC Full View of GDC+GTA screen1",
    category_headers={"FINANCIAL - Total", "QUASI NON-FINANCIAL - Total", "NON-FINANCIAL - Total"}
)

df_hnw = process_layout_sheet(
    sheet_name="CC Full View of HNW Qs1bis",
    category_headers={"INSTITUTIONAL - Total", "APP INVESTMENT - Total", "UNITED FINANCIALS - Total"}
)

# --- Executive View (depends on GDC + HNW) ---
df_exec = process_executive_view(df_gdc, df_hnw)

# --- Users Productivity ---
df_users = process_users_productivity()

df_calendar = process_calendar_events()
df_announcements = process_announcements()

# --- Capacity Table ---
df_capacity = process_capacity_table(df_gdc)

# --- Backlogs ---
df_backlogs_gdc = compute_backlogs(data_dump_df, layout_path, layout_type="GDC", layout_sheet="Backlogs_GDC")
df_backlogs_hnw = compute_backlogs(data_dump_df, layout_path, layout_type="HNW", layout_sheet="Backlogs_HNW")

# --- Ensure column names are consistent before filtering ---
if "QueueName" not in df_backlogs_gdc.columns and "CI GTA & GDC Queue Volumes" in df_backlogs_gdc.columns:
    df_backlogs_gdc.rename(columns={"CI GTA & GDC Queue Volumes": "QueueName"}, inplace=True)

if "QueueName" not in df_backlogs_hnw.columns and "CI GTA & GDC Queue Volumes" in df_backlogs_hnw.columns:
    df_backlogs_hnw.rename(columns={"CI GTA & GDC Queue Volumes": "QueueName"}, inplace=True)

last_modified_est = get_last_modified_est_from_txt(last_updated_txt_path)

last_updated_records = [
    ["CC Full View of GDC+GTA screen1", "Digital Dashboard Layout + Requirements.xlsx", last_modified_est],
    ["CC Full View of HNW Qs1bis", "Digital Dashboard Layout + Requirements.xlsx", last_modified_est],
    ["Executive View", "Derived from GDC + HNW tables", last_modified_est],
    ["USERS_Productivity screen2", "Digital Dashboard Layout + Requirements.xlsx / BOA - Time Off Work.xlsm", last_modified_est],
    ["Capacity", "Attendance.xlsx / Work Queue Team Mapping.xlsx / CIF BOA Official Scorecard.xlsx", last_modified_est],
    ["Backlogs", "Digital Dashboard Queue Names Data Dump.xlsx / Digital Dashboard Layout + Requirements.xlsx", last_modified_est],
    ["Calendar of Events", "Calendar of Events.xlsx", last_modified_est],
    ["Announcements", "Calendar of Events.xlsx", last_modified_est],
]

df_last_updated = pd.DataFrame(
    last_updated_records,
    columns=["Table Name", "Source File(s)", "Last Updated (Timestamp)"]
)

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
output_path = rf"C:\Users\edmichaeljoil.fajard\Documents\CBPS - Command Centre Dashboard\Processed_Dashboard_Output.xlsx"

# Add unique ID columns to GDC, HNW, and Backlogs
df_gdc.insert(0, "Row_ID", range(1, len(df_gdc) + 1))
df_hnw.insert(0, "Row_ID", range(1, len(df_hnw) + 1))
df_backlogs_gdc.insert(0, "Row_ID", range(1, len(df_backlogs_gdc) + 1))
df_backlogs_hnw.insert(0, "Row_ID", range(1, len(df_backlogs_hnw) + 1))

with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
    df_gdc.to_excel(writer, index=False, sheet_name="CC Full View of GDC+GTA screen1")
    df_hnw.to_excel(writer, index=False, sheet_name="CC Full View of HNW Qs1bis")
    df_users.to_excel(writer, index=False, sheet_name="USERS_Productivity screen2")
    df_exec.to_excel(writer, index=False, sheet_name="Executive View")
    df_backlogs_gdc.to_excel(writer, index=False, sheet_name="Backlogs_GDC")
    df_backlogs_hnw.to_excel(writer, index=False, sheet_name="Backlogs_HNW")
    df_calendar.to_excel(writer, index=False, sheet_name="Calendar of Events")
    df_announcements.to_excel(writer, index=False, sheet_name="Announcements")
    df_last_updated.to_excel(writer, index=False, sheet_name="Last Updated")
    
print(f"Processed dashboard saved to {output_path}")

# --- Export to both SQLite and PostgreSQL via SQLAlchemy ---

# --- SQLite (local backup) ---
sqlite_path = os.path.join(os.path.dirname(__file__), "data", "Processed_Data_DB.db")
sqlite_engine = create_engine(f"sqlite:///{sqlite_path}", echo=False)

# --- PostgreSQL (Minikube database) ---
POSTGRES_USER = "cc_pipeline_user"
POSTGRES_PASSWORD = "admin"
POSTGRES_DB = "command_centre"
POSTGRES_HOST = "192.168.49.2"    # Python runs on the host
POSTGRES_PORT = "30032"        # NodePort from your YAML

try:
    postgres_engine = create_engine(
        f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}",
        echo=False
    )
    print("PostgreSQL connection initialized.")
except Exception as e:
    postgres_engine = None
    print("Could not connect to PostgreSQL:", e)

# --- Clean up column names ---
def sanitize_columns(df):
    df = df.copy()
    df.columns = [
        f"col_{i}" if (not str(col).strip() or str(col).lower() == "nan")
        else str(col).strip().replace(" ", "_").replace("-", "_")
        for i, col in enumerate(df.columns)
    ]
    return df

# --- Prepare all DataFrames ---
df_dict = {
    "gdc_gta": sanitize_columns(df_gdc),
    "hnw": sanitize_columns(df_hnw),
    "users_productivity": sanitize_columns(df_users),
    "executive_view": sanitize_columns(df_exec),
    "calendar_of_events": sanitize_columns(df_calendar),
    "backlogs_gdc": sanitize_columns(df_backlogs_gdc),
    "backlogs_hnw": sanitize_columns(df_backlogs_hnw),
    "capacity": sanitize_columns(df_capacity),
    "announcements": sanitize_columns(df_announcements),
    "last_updated": sanitize_columns(df_last_updated)
}

# --- Function to save to both databases ---
def save_to_databases(df_dict, sqlite_engine, postgres_engine=None):
    for name, df in df_dict.items():
        try:
            # Save to SQLite
            df.to_sql(name, sqlite_engine, if_exists="replace", index=False)
            #print(f"Saved '{name}' to SQLite")

            # Save to PostgreSQL (if available)
            if postgres_engine:
                df.to_sql(name, postgres_engine, if_exists="replace", index=False)
                print(f"Saved '{name}' to PostgreSQL")
        except Exception as e:
            print(f"Error saving '{name}':", e)

# --- Execute save ---
save_to_databases(df_dict, sqlite_engine, postgres_engine)

print(f"SQLite database saved to {sqlite_path}")
if postgres_engine:
    print("PostgreSQL export completed successfully.")

