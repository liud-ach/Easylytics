import pandas as pd
from flask import Flask, render_template_string, request, redirect, url_for, jsonify, flash, session, render_template
import json
import os
import sys
import functools
from datetime import datetime
import glob
import xml.etree.ElementTree as ET
import webbrowser

# Handle PyInstaller frozen state for template paths
if getattr(sys, 'frozen', False):
    template_folder = os.path.join(sys._MEIPASS, 'templates')
else:
    template_folder = 'templates'

app = Flask(__name__, template_folder=template_folder)
app.secret_key = 'your_secret_key'

# Define the absolute path to the Parquet files, handling PyInstaller frozen state
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__)) if not getattr(sys, 'frozen', False) else sys._MEIPASS
IP_DATA_PARQUET_PATH = os.path.join(CURRENT_DIR, "ip_data.parquet")
AMB_DATA_PARQUET_PATH = os.path.join(CURRENT_DIR, "amb_data.parquet")

# Cache for the dataframes
_cache = {
    'ip_df': None,
    'amb_df': None,
    'last_loaded': None,
    'needs_reload': True
}

def get_cached_dataframes(force_reload=False):
    """Get cached dataframes, loading them if necessary."""
    if force_reload or _cache['needs_reload'] or _cache['ip_df'] is None or _cache['amb_df'] is None:
        try:
            print("Loading fresh data from parquet files...")
            amb_df, ip_df = load_or_create_dataframes()
            _cache['amb_df'] = amb_df
            _cache['ip_df'] = ip_df
            _cache['last_loaded'] = datetime.now()
            _cache['needs_reload'] = False
        except Exception as e:
            print(f"Error loading dataframes: {e}")
            if _cache['ip_df'] is not None and _cache['amb_df'] is not None:
                print("Using cached data due to load error")
                return _cache['amb_df'], _cache['ip_df']
            raise
    else:
        print("Using cached dataframes")
    
    return _cache['amb_df'], _cache['ip_df']

def invalidate_cache():
    """Mark the cache as needing a reload."""
    _cache['needs_reload'] = True

def import_excel_xml_data_from_folder(folder_path):
    """Import Excel XML data from folder into DataFrame."""
    xml_files = glob.glob(os.path.join(folder_path, "*.xml"))
    if not xml_files:
        raise FileNotFoundError(f"No XML files found in {folder_path!r}")
    
    dfs = []
    ns = {'ss': 'urn:schemas-microsoft-com:office:spreadsheet'}
    
    for file_path in xml_files:
        tree = ET.parse(file_path)
        root = tree.getroot()
        table = root.find(".//ss:Table", ns)
        if table is None:
            print(f"  → no <Table> found in {file_path}, skipping.")
            continue
            
        rows = table.findall("ss:Row", ns)
        if not rows:
            print(f"  → no <Row> entries in {file_path}, skipping.")
            continue
            
        header_cells = rows[0].findall("ss:Cell", ns)
        headers = [cell.find("ss:Data", ns).text for cell in header_cells]
        
        data = []
        for row in rows[1:]:
            cells = row.findall("ss:Cell", ns)
            row_vals = []
            for i in range(len(headers)):
                try:
                    data_elem = cells[i].find("ss:Data", ns)
                    row_vals.append(data_elem.text if data_elem is not None else None)
                except IndexError:
                    row_vals.append(None)
            data.append(row_vals)
            
        df = pd.DataFrame(data, columns=headers)
        df['source_file'] = os.path.basename(file_path)
        dfs.append(df)
        print(f"  → imported {df.shape[0]} rows from {os.path.basename(file_path)}")
        
    if not dfs:
        raise ValueError("No data was successfully imported from any file.")
        
    combined = pd.concat(dfs, ignore_index=True)
    print(f"Combined DataFrame: {combined.shape[0]} rows × {combined.shape[1]} columns")
    return combined

def load_or_create_dataframes():
    """Load data from parquet files or create from XML files."""
    base_dir = os.path.dirname(sys.executable) if getattr(sys, 'frozen', False) else os.path.dirname(os.path.abspath(__file__))
    amb_folder = os.path.join(base_dir, 'AMB')
    ip_folder = os.path.join(base_dir, 'IP')
    
    def needs_rebuild(folder, parquet_file):
        if not os.path.exists(parquet_file):
            return True
        parquet_mtime = os.path.getmtime(parquet_file)
        return any(os.path.getmtime(f) > parquet_mtime for f in glob.glob(os.path.join(folder, "*.xml")))
    
    # Load or create AMB data
    if needs_rebuild(amb_folder, AMB_DATA_PARQUET_PATH):
        print("Creating new AMB dataframe from XML files...")
        amb_df = import_excel_xml_data_from_folder(amb_folder)
        amb_df.to_parquet(AMB_DATA_PARQUET_PATH)
    else:
        print("Loading AMB data from parquet file...")
        amb_df = pd.read_parquet(AMB_DATA_PARQUET_PATH)
        
    # Load or create IP data
    if needs_rebuild(ip_folder, IP_DATA_PARQUET_PATH):
        print("Creating new IP dataframe from XML files...")
        ip_df = import_excel_xml_data_from_folder(ip_folder)
        ip_df.to_parquet(IP_DATA_PARQUET_PATH)
    else:
        print("Loading IP data from parquet file...")
        ip_df = pd.read_parquet(IP_DATA_PARQUET_PATH)
        
    # Convert Value columns to numeric
    amb_df.Value = pd.to_numeric(amb_df.Value)
    ip_df.Value = pd.to_numeric(ip_df.Value)
    
    return amb_df, ip_df

def load_user_profiles():
    """Load and process user profiles from cached data."""
    try:
        amb_df, ip_df = get_cached_dataframes()
        
        ip_df_processed = ip_df[['Clinician Name', 'Specialty', 'Login Department', 'User Type']].rename(
            columns={
                'Clinician Name': 'name',
                'Login Department': 'department',
                'Specialty': 'specialty',
                'User Type': 'user_type'
            }
        )
        
        amb_df_processed = amb_df[['Clinician Name', 'Specialty', 'Department', 'User Type']].rename(
            columns={
                'Clinician Name': 'name',
                'Department': 'department',
                'Specialty': 'specialty',
                'User Type': 'user_type'
            }
        )
        
        combined_df = pd.concat([ip_df_processed, amb_df_processed], ignore_index=True)
        users_df = combined_df.drop_duplicates(subset=['name', 'specialty', 'department', 'user_type']).copy()
        
        for col in ['name', 'specialty', 'department', 'user_type']:
            users_df.loc[:, col] = users_df[col].fillna('N/A')
            users_df.loc[:, col] = users_df[col].astype(str)
            
        return users_df.sort_values(by='name').reset_index(drop=True)
        
    except Exception as e:
        print(f"Error in load_user_profiles: {e}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame(columns=['name', 'specialty', 'department', 'user_type'])

def get_reporting_periods_for_selected_users(selected_names):
    """Get reporting periods for selected users using cached data."""
    try:
        amb_df, ip_df = get_cached_dataframes()
        
        ip_df = ip_df[['Clinician Name', 'Reporting Period Start Date', 'Reporting Period End Date']]
        amb_df = amb_df[['Clinician Name', 'Reporting Period Start Date', 'Reporting Period End Date']]
        
        if 'Clinician Name' not in ip_df.columns or 'Clinician Name' not in amb_df.columns:
            print("Error: 'Clinician Name' column not found")
            return [], {}
            
        ip_selected_df = ip_df[ip_df['Clinician Name'].isin(selected_names)]
        amb_selected_df = amb_df[amb_df['Clinician Name'].isin(selected_names)]
        
        combined_dates_df = pd.concat([
            ip_selected_df[['Reporting Period Start Date', 'Reporting Period End Date']],
            amb_selected_df[['Reporting Period Start Date', 'Reporting Period End Date']]
        ]).drop_duplicates().dropna()
        
        combined_dates_df['Reporting Period Start Date'] = pd.to_datetime(
            combined_dates_df['Reporting Period Start Date']
        ).dt.strftime('%Y-%m-%d')
        combined_dates_df['Reporting Period End Date'] = pd.to_datetime(
            combined_dates_df['Reporting Period End Date']
        ).dt.strftime('%Y-%m-%d')
        
        combined_dates_df = combined_dates_df.sort_values(
            by=['Reporting Period Start Date', 'Reporting Period End Date']
        ).reset_index(drop=True)
        
        date_pairs_for_table = []
        start_to_end_map = {}
        
        for _, row in combined_dates_df.iterrows():
            start_date = row['Reporting Period Start Date']
            end_date = row['Reporting Period End Date']
            date_pairs_for_table.append({'start_date': start_date, 'end_date': end_date})
            start_to_end_map[start_date] = end_date
            
        unique_date_pairs_for_table = [
            dict(t) for t in {tuple(d.items()) for d in date_pairs_for_table}
        ]
        unique_date_pairs_for_table = sorted(
            unique_date_pairs_for_table,
            key=lambda x: (x['start_date'], x['end_date'])
        )
        
        return unique_date_pairs_for_table, start_to_end_map
        
    except Exception as e:
        print(f"Error in get_reporting_periods: {e}")
        import traceback
        traceback.print_exc()
        return [], {}

def categorize_metric(metric_name, metric_type):
    """Categorize a metric based on its name and type."""
    name_lower = metric_name.lower()
    
    if "in basket" in name_lower:
        return "In Basket"
    if "turnaround time" in name_lower:
        return "Turnaround Time"
    if "order" in name_lower:
        return "Orders"
    if "note" in name_lower or "documentation" in name_lower:
        return "Notes & Documentation"
    if "time in" in name_lower or "seconds per" in name_lower:
        return "Time-Based Metrics"
    if "appointment" in name_lower or "schedule" in name_lower:
        return "Appointments & Scheduling"
    if "level of service" in name_lower:
        return "Patient Level of Service"
    if "quickaction" in name_lower:
        return "QuickActions"
    if "system" in name_lower or "chart search" in name_lower:
        return "System Usage & Preferences"
    if "mobile" in name_lower:
        return "Mobile Usage"
    if "secure chat" in name_lower:
        return "Secure Chat"
    if "message" in name_lower:
        return "Messaging"
    return "Other"

def get_all_metrics_from_parquet():
    """Get metrics list from cached data."""
    try:
        print("Loading metrics from cached data...")
        amb_df, ip_df = get_cached_dataframes()
        
        ip_metrics_list = ip_df['Metric'].dropna().unique().tolist()
        amb_metrics_list = amb_df['Metric'].dropna().unique().tolist()
        
        print(f"Found {len(ip_metrics_list)} IP metrics and {len(amb_metrics_list)} AMB metrics")
        return ip_metrics_list, amb_metrics_list
        
    except Exception as e:
        print(f"Error loading metrics: {e}")
        import traceback
        traceback.print_exc()
        return [], []

def get_all_metrics_categorized():
    """Get all metrics categorized using cached data."""
    ip_metrics_list, amb_metrics_list = get_all_metrics_from_parquet()
    
    all_unique_metrics = sorted(list(set(ip_metrics_list + amb_metrics_list)))
    print(f"Total unique metrics: {len(all_unique_metrics)}")
    
    sidebar_metrics = {}
    for metric_name in all_unique_metrics:
        metric_type = (
            "IP/AMB" if metric_name in ip_metrics_list and metric_name in amb_metrics_list
            else "IP" if metric_name in ip_metrics_list
            else "AMB"
        )
        
        category = categorize_metric(metric_name, metric_type)
        if category not in sidebar_metrics:
            sidebar_metrics[category] = []
            
        sidebar_metrics[category].append({
            'name': metric_name,
            'type': metric_type
        })
        
    # Sort metrics within each category
    for category in sidebar_metrics:
        sidebar_metrics[category].sort(key=lambda x: x['name'])
        
    metric_categories = sorted(list(sidebar_metrics.keys()))
    return sidebar_metrics, all_unique_metrics, metric_categories

def perform_analysis(selected_clinicians, pre_start_str, pre_end_str, post_start_str, post_end_str, all_metrics_list):
    """Perform analysis using cached data."""
    results_table = []
    try:
        print("Loading from cached data...")
        amb_df, ip_df = get_cached_dataframes()
        
        ip_metrics_list, amb_metrics_list = get_all_metrics_from_parquet()
        
        ip_data_df_filtered = ip_df[[
            'Clinician Name', 'Metric', 'Value',
            'Reporting Period Start Date', 'Reporting Period End Date'
        ]].copy()
        
        amb_data_df_filtered = amb_df[[
            'Clinician Name', 'Metric', 'Value',
            'Reporting Period Start Date', 'Reporting Period End Date'
        ]].copy()
        
        combined_df = pd.concat([ip_data_df_filtered, amb_data_df_filtered], ignore_index=True)
        analysis_df = combined_df[combined_df['Clinician Name'].isin(selected_clinicians)].copy()
        
        analysis_df.loc[:, 'Reporting Period Start Date'] = pd.to_datetime(
            analysis_df['Reporting Period Start Date'],
            errors='coerce'
        )
        analysis_df.loc[:, 'Reporting Period End Date'] = pd.to_datetime(
            analysis_df['Reporting Period End Date'],
            errors='coerce'
        )
        
        pre_start_dt = pd.to_datetime(pre_start_str, errors='coerce')
        pre_end_dt = pd.to_datetime(pre_end_str, errors='coerce')
        post_start_dt = pd.to_datetime(post_start_str, errors='coerce')
        post_end_dt = pd.to_datetime(post_end_str, errors='coerce')
        
        if pd.NaT in [pre_start_dt, pre_end_dt, post_start_dt, post_end_dt]:
            flash("Date conversion error for analysis periods.", "error")
            return []
            
        pre_df = analysis_df[
            (analysis_df['Reporting Period Start Date'] >= pre_start_dt) &
            (analysis_df['Reporting Period End Date'] <= pre_end_dt)
        ].copy()
        
        post_df = analysis_df[
            (analysis_df['Reporting Period Start Date'] >= post_start_dt) &
            (analysis_df['Reporting Period End Date'] <= post_end_dt)
        ].copy()
        
        if len(pre_df) == 0 or len(post_df) == 0:
            flash("No data found for the selected time periods.", "error")
            
        for metric_name in all_metrics_list:
            metric_pre_df = pre_df[pre_df['Metric'] == metric_name].copy()
            metric_post_df = post_df[post_df['Metric'] == metric_name].copy()
            
            metric_type = (
                "IP/AMB" if metric_name in ip_metrics_list and metric_name in amb_metrics_list
                else "IP" if metric_name in ip_metrics_list
                else "AMB"
            )
            
            metric_pre_df.loc[:, 'Value'] = pd.to_numeric(
                metric_pre_df['Value'],
                errors='coerce'
            )
            metric_post_df.loc[:, 'Value'] = pd.to_numeric(
                metric_post_df['Value'],
                errors='coerce'
            )
            
            pre_median = metric_pre_df['Value'].median() if not metric_pre_df.empty else None
            post_median = metric_post_df['Value'].median() if not metric_post_df.empty else None
            
            pre_median_display = f"{pre_median:.2f}" if pd.notna(pre_median) else 'N/A'
            post_median_display = f"{post_median:.2f}" if pd.notna(post_median) else 'N/A'
            
            abs_diff = None
            pct_diff = None
            
            if pd.notna(pre_median) and pd.notna(post_median):
                abs_diff = post_median - pre_median
                if pre_median != 0:
                    pct_diff = (abs_diff / pre_median) * 100
                elif abs_diff == 0:
                    pct_diff = 0.0
                    
            abs_diff_display = f"{abs_diff:.3f}" if pd.notna(abs_diff) else 'N/A'
            pct_diff_display = f"{pct_diff:.2f}%" if pd.notna(pct_diff) else 'N/A'
            
            if (pct_diff == 0.0 and pd.notna(pre_median) and pre_median == 0
                and pd.notna(post_median) and post_median == 0):
                pct_diff_display = "0.00%"
                
            results_table.append({
                'name': metric_name,
                'type': metric_type,
                'pre_median': pre_median_display,
                'post_median': post_median_display,
                'abs_diff': abs_diff_display,
                'pct_diff': pct_diff_display,
                'category': categorize_metric(metric_name, metric_type)
            })
            
    except Exception as e:
        flash(f"Analysis error: {str(e)}", "error")
        print(f"Error in perform_analysis: {e}")
        import traceback
        traceback.print_exc()
        return []
        
    return results_table

@app.route('/', methods=['GET'])
def index():
    users_df = load_user_profiles()
    
    name_query = request.args.get('name_query', '').strip().lower()
    specialty_filter = request.args.get('specialty', '').strip().lower()
    department_filter = request.args.get('department', '').strip().lower()
    user_type_filter = request.args.get('user_type', '').strip().lower()
    
    persisted_selected_names = request.args.getlist('selected_users')
    
    filtered_users = users_df.copy()
    
    if name_query:
        filtered_users = filtered_users[filtered_users['name'].str.lower().str.contains(name_query)]
    if specialty_filter:
        filtered_users = filtered_users[filtered_users['specialty'].str.lower() == specialty_filter]
    if department_filter:
        filtered_users = filtered_users[filtered_users['department'].str.lower() == department_filter]
    if user_type_filter:
        filtered_users = filtered_users[filtered_users['user_type'].str.lower() == user_type_filter]
        
    if persisted_selected_names:
        selected_users = users_df[users_df['name'].isin(persisted_selected_names)]
        filtered_users = pd.concat([filtered_users, selected_users]).drop_duplicates(subset=['name'])
        
    specialties = sorted(users_df['specialty'].unique())
    departments = sorted(users_df['department'].unique())
    user_types = sorted(users_df['user_type'].unique())
    
    filtered_users_list = filtered_users.to_dict(orient='records')
    for i, user in enumerate(filtered_users_list):
        user['id'] = f"user_checkbox_{i}"
        
    return render_template(
        'index.html',
        users=filtered_users_list,
        specialties=specialties,
        departments=departments,
        user_types=user_types,
        request=request,
        persisted_selected_names=persisted_selected_names
    )

@app.route('/select_dates', methods=['GET', 'POST'])
def select_dates_page():
    if request.method == 'POST':
        selected_user_names = request.form.getlist('selected_users')
        if not selected_user_names:
            flash('No clinicians were selected.', 'error')
            return redirect(url_for('index'))
        session['selected_users'] = selected_user_names
    else:
        selected_user_names = session.get('selected_users', [])
        if not selected_user_names:
            flash('No clinicians selected in session.', 'error')
            return redirect(url_for('index'))
            
    date_pairs, _ = get_reporting_periods_for_selected_users(selected_user_names)
    back_link_params = session.get('last_filter_params', {})
    
    return render_template(
        'select_dates.html',
        selected_names=selected_user_names,
        all_reporting_periods=date_pairs,
        back_link_params=back_link_params,
        request=request
    )

@app.route('/analyze_metrics', methods=['POST'])
def analyze_metrics():
    selected_users = session.get('selected_users', [])
    if not selected_users:
        flash("Clinician selection is missing.", 'error')
        return redirect(url_for('index'))
        
    pre_period_strings = request.form.getlist('pre_selected_periods')
    post_period_strings = request.form.getlist('post_selected_periods')
    
    if not pre_period_strings or not post_period_strings:
        flash("Please select periods for both pre and post.", 'error')
        return redirect(url_for('select_dates_page'))
        
    try:
        pre_start_date_str, _ = pre_period_strings[0].split(' to ')
        _, pre_end_date_str = pre_period_strings[-1].split(' to ')
        post_start_date_str, _ = post_period_strings[0].split(' to ')
        _, post_end_date_str = post_period_strings[-1].split(' to ')
    except ValueError as e:
        flash(f"Error parsing period strings: {e}", 'error')
        return redirect(url_for('select_dates_page'))
        
    sidebar_metrics_data, all_unique_metrics_list, metric_categories = get_all_metrics_categorized()
    
    metrics_results_table = perform_analysis(
        selected_users,
        pre_start_date_str, pre_end_date_str,
        post_start_date_str, post_end_date_str,
        all_unique_metrics_list
    )
    
    persisted_selected_metrics = session.get('selected_metrics', [])
    
    if persisted_selected_metrics:
        for category in sidebar_metrics_data:
            for metric in sidebar_metrics_data[category]:
                metric['selected'] = metric['name'] in persisted_selected_metrics
                
    return render_template(
        'analysis_page.html',
        selected_clinicians=selected_users,
        pre_start=pre_start_date_str,
        pre_end=pre_end_date_str,
        post_start=post_start_date_str,
        post_end=post_end_date_str,
        sidebar_metrics=sidebar_metrics_data,
        metric_categories=metric_categories,
        results=metrics_results_table,
        persisted_selected_metrics=persisted_selected_metrics
    )

@app.route('/update_selected_metrics', methods=['POST'])
def update_selected_metrics():
    selected_metrics = request.form.getlist('metrics[]')
    session['selected_metrics'] = selected_metrics
    return jsonify({'status': 'success', 'selected_metrics': selected_metrics})

if __name__ == '__main__':
    url = 'http://0.0.0.0:5001'
    # Open browser after a slight delay to ensure server is running
    webbrowser.open(url)
    app.run(host='0.0.0.0', port=5001, debug=True)
