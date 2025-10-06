import os
import pandas as pd
from datetime import datetime
import re
from sqlalchemy import create_engine

# Folder where Flask API saves uploaded files
UPLOAD_DIR = os.path.join(os.path.dirname(__file__), "uploads")

data_dump_path = os.path.join(UPLOAD_DIR, "Digital Dashboard Queue Names Data Dump.xlsx")
layout_path    = os.path.join(UPLOAD_DIR, "Digital Dashboard Layout + Requirements.xlsx")
reso_dump_path = os.path.join(UPLOAD_DIR, "Resolution Queue Volume Data.xlsx")
reso_map_path  = os.path.join(UPLOAD_DIR, "Reso Doc Types vs Processing Queue Names.xlsx")
boa_path       = os.path.join(UPLOAD_DIR, "BOA - Time Off Work.xlsm")

pro_pf_path  = os.path.join(UPLOAD_DIR, "Personal Folder.xlsx")
reso_pf_path = os.path.join(UPLOAD_DIR, "ECISS Resolutions Personal Folder.xlsx")

calendar_path = os.path.join(UPLOAD_DIR, "Calendar of Events.xlsx")

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
data_dump_df = pd.read_excel(dump_wb, sheet_name="original data dump")

reso_df = pd.read_excel(reso_dump_path, sheet_name="Original Data Dump")
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

def map_personal_folder_counts(folder_df, reso_map_df):
    counts = folder_df.groupby("Doc Type")["Document ID"].nunique().reset_index(name="PFCount")
    mapped = pd.merge(
        counts,
        reso_map_df[["Doc_Type", "Queue_Desc"]],
        left_on="Doc Type",
        right_on="Doc_Type",
        how="left"
    )
    return mapped.groupby("Queue_Desc")["PFCount"].sum().to_dict()

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

    # --- PRO QUEUES ---
    pro_entries = data_dump_df[~data_dump_df["Queue"].astype(str).str.endswith("QC", na=False)].copy()
    pro_counts = pro_entries.groupby("Queue")["Document ID"].nunique().to_dict()
    pro_locked_counts = pro_entries[pro_entries["Lock Status"] == "LOCKED"].groupby("Queue")["Document ID"].nunique().to_dict()

    output_df["PRO Queue"] = output_df["QueueName"].map(pro_counts).fillna(0).astype(int)
    output_df["User Locked PRO"] = output_df["QueueName"].map(pro_locked_counts).fillna(0).astype(int)

    # --- QC QUEUES ---
    qc_entries = data_dump_df[data_dump_df["Queue"].astype(str).str.endswith("QC", na=False)].copy()
    qc_entries["BaseQueue"] = qc_entries["Queue"].astype(str).str.replace(r"QC$", "", regex=True).str.strip()

    qc_counts = qc_entries.groupby("BaseQueue")["Document ID"].nunique().to_dict()
    qc_locked_counts = qc_entries[qc_entries["Lock Status"] == "LOCKED"].groupby("BaseQueue")["Document ID"].nunique().to_dict()

    output_df["QC Queue"] = output_df["QueueName"].map(qc_counts).fillna(0).astype(int)
    output_df["User Locked QC"] = output_df["QueueName"].map(qc_locked_counts).fillna(0).astype(int)

    # --- Processed Volumes ---
    processed_counts = data_dump_df.groupby("Queue")["Document ID"].nunique().to_dict()
    output_df["Processed Volumes"] = output_df["QueueName"].map(processed_counts).fillna(0).astype(int)

    # --- Reso Queue Mapping (only for GDC) ---
    if sheet_name == "CC Full View of GDC+GTA screen1":
        reso_counts = reso_df.groupby("Doc Type")["Doc ID"].nunique().reset_index(name="ResoCount")
        reso_with_desc = pd.merge(reso_counts, reso_map_df[["Doc_Type", "Queue_Desc"]],
                                  left_on="Doc Type", right_on="Doc_Type", how="left")
        reso_final_counts = reso_with_desc.groupby("Queue_Desc")["ResoCount"].sum().to_dict()
        output_df["Reso Queue"] = output_df["QueueName"].map(reso_final_counts).fillna(0).astype(int)

    # PRO Personal Folders
    pro_pf_counts = map_personal_folder_counts(pro_pf_df, reso_map_df)
    if "PRO Personal Folders" in output_df.columns:
        output_df["PRO Personal Folders"] = output_df["QueueName"].map(pro_pf_counts).fillna(0).astype(int)

    # RESO Personal Folders
    reso_pf_counts = map_personal_folder_counts(reso_pf_df, reso_map_df)
    if "RESO Personal Folders" in output_df.columns:
        output_df["RESO Personal Folders"] = output_df["QueueName"].map(reso_pf_counts).fillna(0).astype(int)

    output_df["Accepted Volumes"] = 0
    output_df["QC'ed Volumes"] = 0
    output_df["Resolutions Completed Volumes"] = 0
    output_df["SLA % Completed"] = 0

    # --- Build Queue → Category mapping ---
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
        "PRO Personal Folders", "RESO Personal Folders",  # added into numeric metrics
        "Accepted Volumes", "QC'ed Volumes", "Resolutions Completed Volumes"
    ] if c in output_df.columns]

    for cat in category_headers:
        child_rows = output_df[output_df["Category"] == cat]
        totals = child_rows[numeric_metrics].sum(numeric_only=True)
        for col in numeric_metrics:
            output_df.loc[output_df["QueueName"] == cat, col] = totals.get(col, 0)

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

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
output_path = rf"C:\Users\edmichaeljoil.fajard\Documents\CBPS - Command Centre Dashboard\Processed_Dashboard_Output_{timestamp}.xlsx"

with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
    df_gdc.to_excel(writer, index=False, sheet_name="CC Full View of GDC+GTA screen1")
    df_hnw.to_excel(writer, index=False, sheet_name="CC Full View of HNW Qs1bis")
    df_users.to_excel(writer, index=False, sheet_name="USERS_Productivity screen2")
    df_exec.to_excel(writer, index=False, sheet_name="Executive View")
    df_calendar.to_excel(writer, index=False, sheet_name="Calendar of Events")
    
print(f"Processed dashboard saved to {output_path}")

# --- Export all to SQLite via SQLAlchemy ---
db_path = r"C:\Users\edmichaeljoil.fajard\Documents\CBPS - Command Centre Dashboard\Processed_Data_DB.db"

engine = create_engine(f"sqlite:///{db_path}", echo=False)

def sanitize_columns(df):
    df = df.copy()
    df.columns = [
        f"col_{i}" if (not str(col).strip() or str(col).lower() == "nan")
        else str(col).strip().replace(" ", "_").replace("-", "_")
        for i, col in enumerate(df.columns)
    ]
    return df

df_gdc_sql   = sanitize_columns(df_gdc)
df_hnw_sql   = sanitize_columns(df_hnw)
df_users_sql = sanitize_columns(df_users)
df_exec_sql  = sanitize_columns(df_exec)
df_calendar_sql = sanitize_columns(df_calendar)

# overwrite tables each run
df_gdc_sql.to_sql("gdc_gta", engine, if_exists="replace", index=False)
df_hnw_sql.to_sql("hnw", engine, if_exists="replace", index=False)
df_users_sql.to_sql("users_productivity", engine, if_exists="replace", index=False)
df_exec_sql.to_sql("executive_view", engine, if_exists="replace", index=False)
df_calendar_sql.to_sql("calendar_of_events", engine, if_exists="replace", index=False)

print(f"SQLite database saved to {db_path}")


