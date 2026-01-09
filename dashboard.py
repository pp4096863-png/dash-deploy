import dash
from dash import dcc, html, Input, Output, State, callback_context, dash_table
import plotly.express as px
import pandas as pd
import numpy as np
from datetime import datetime
import io
import json
import traceback
import os
import time
import zipfile
import threading
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# Global variables for monitoring
last_modified_time = 0
data_file_path = "data from db.xlsx"
transformed_file_path = "excel_data_model_fixed.xlsx"
monitoring_active = True

# Google Sheets Configuration
GOOGLE_SHEET_ID = "15G9U072EJkvkuePmWWKgIvYwfTfvGOVMdqL6AIMUwVA"
SHEET_NAME = "Sheet1"  # Change this if your sheet has a different name
CREDENTIALS_FILE = "google_credentials.json"

def get_google_sheets_data():
    """
    Fetch data from Google Sheets using service account credentials.
    Returns DataFrame with the data, or None if error occurs.
    """
    try:
        # Load credentials from file or environment variable
        if os.path.exists(CREDENTIALS_FILE):
            creds_dict = json.load(open(CREDENTIALS_FILE))
        else:
            # For Render: credentials stored as environment variable
            creds_json = os.environ.get('GOOGLE_CREDENTIALS')
            if not creds_json:
                print("Warning: Google credentials not found (local or env)")
                return None
            creds_dict = json.loads(creds_json)
        
        # Authenticate with Google Sheets API
        credentials = Credentials.from_service_account_info(
            creds_dict,
            scopes=['https://www.googleapis.com/auth/spreadsheets.readonly']
        )
        service = build('sheets', 'v4', credentials=credentials)
        
        # Fetch data from sheet
        sheet = service.spreadsheets()
        result = sheet.values().get(spreadsheetId=GOOGLE_SHEET_ID, range=f"{SHEET_NAME}").execute()
        values = result.get('values', [])
        
        if not values:
            print("No data found in Google Sheet")
            return None
        
        # Convert to DataFrame
        df = pd.DataFrame(values[1:], columns=values[0])
        print(f"✅ Loaded {len(df)} rows from Google Sheets")
        return df
        
    except Exception as e:
        print(f"❌ Error fetching from Google Sheets: {e}")
        traceback.print_exc()
        return None

def transform_data():
    """
    Transform data from Google Sheets and create dimension/fact tables.
    This function contains the logic from transform_data.py
    """
    try:
        print("Starting data transformation from Google Sheets...")
        
        # Fetch data from Google Sheets
        raw_data = get_google_sheets_data()
        if raw_data is None or raw_data.empty:
            print("❌ Failed to fetch data from Google Sheets")
            return False
        
        print(f"   Raw data shape: {raw_data.shape}")

        print("\n Creating Dim Tables...")
        customer_dim = pd.DataFrame({
            'Customer': sorted(raw_data['Customer'].dropna().unique()),
            'CustomerID': range(1, len(raw_data['Customer'].dropna().unique()) + 1)
        })
        print(f"   Customer_Dim: {customer_dim.shape}")

        project_dim = pd.DataFrame({
            'Project': sorted(raw_data['Project'].dropna().unique()),
            'ProjectID': range(1, len(raw_data['Project'].dropna().unique()) + 1)
        })
        print(f"   Project_Dim: {project_dim.shape}")

        sm_dim = pd.DataFrame({
            'SM': sorted(raw_data['SM'].dropna().unique()),
            'SMID': range(1, len(raw_data['SM'].dropna().unique()) + 1)
        })
        print(f"   SM_Dim: {sm_dim.shape}")

        unique_dates = sorted(raw_data['Month'].dropna().unique())
        date_dim = pd.DataFrame({
            'Date': unique_dates,
            'DateID': range(1, len(unique_dates) + 1),
            'Year': [d.year for d in unique_dates],
            'Month': [d.month for d in unique_dates],
            'Quarter': [(d.month - 1) // 3 + 1 for d in unique_dates],
            'YearMonth': [d.strftime('%Y-%m') for d in unique_dates]
        })
        print(f"   Date_Dim: {date_dim.shape}")

        po_ref_dim = pd.DataFrame({
            'PO REF': sorted(raw_data['PO REF'].dropna().unique()),
            'PO REF ID': range(1, len(raw_data['PO REF'].dropna().unique()) + 1)
        })
        print(f"   PO REF_Dim: {po_ref_dim.shape}")

        region_dim = pd.DataFrame({
            'Region': sorted(raw_data['Region'].dropna().unique()),
            'Region_ID': range(1, len(raw_data['Region'].dropna().unique()) + 1)
        })
        print(f"   Region_Dim: {region_dim.shape}")

        print("\n Creating lookup dict...")
        customer_lookup = dict(zip(customer_dim['Customer'], customer_dim['CustomerID']))
        project_lookup = dict(zip(project_dim['Project'], project_dim['ProjectID']))
        sm_lookup = dict(zip(sm_dim['SM'], sm_dim['SMID']))
        region_lookup = dict(zip(region_dim['Region'], region_dim['Region_ID']))
        po_ref_lookup = dict(zip(po_ref_dim['PO REF'], po_ref_dim['PO REF ID']))

        # Handle NaN values in lookups
        def safe_lookup(lookup_dict, value):
            if pd.isna(value):
                return None
            return lookup_dict.get(value, None)

        orders_fact = pd.DataFrame({
            'SM': raw_data['SM'],
            'Month': raw_data['Month'],
            'Customer': raw_data['Customer'],
            'Project': raw_data['Project'],
            'PO REF': raw_data['PO REF'],
            'Order Amount': raw_data['Order Amount'],
            'Revenue Amount': raw_data['Revenue Amount'],
            'Cash Amount': raw_data['Cash Amount'],
            'Pending Amount': raw_data['Pending Amount'],
            'Backlog Amount': raw_data['Backlog Amount'],
            'CustomerID': [safe_lookup(customer_lookup, cust) for cust in raw_data['Customer']],
            'ProjectID': [safe_lookup(project_lookup, proj) for proj in raw_data['Project']],
            'SMID': [safe_lookup(sm_lookup, sm) for sm in raw_data['SM']],
            'OrderDateID': range(1, len(raw_data) + 1),
            'PO REF ID': [safe_lookup(po_ref_lookup, po) for po in raw_data['PO REF']],
            'Region': raw_data['Region'],
            'Region_ID': [safe_lookup(region_lookup, reg) for reg in raw_data['Region']]
        })
        print(f"   Orders_Fact: {orders_fact.shape}")

        revenues_fact = pd.DataFrame({
            'UserID': range(1336346, 1336346 + len(raw_data)),
            'Customer': raw_data['Customer'],
            'Month': raw_data['Month'],
            'Project': raw_data['Project'],
            'SM': raw_data['SM'],
            'PO REF': raw_data['PO REF'],
            'Revenue Amount': raw_data['Revenue Amount'],
            'Region': raw_data['Region'],
            'CustomerID': [safe_lookup(customer_lookup, cust) for cust in raw_data['Customer']],
            'ProjectID': [safe_lookup(project_lookup, proj) for proj in raw_data['Project']],
            'SMID': [safe_lookup(sm_lookup, sm) for sm in raw_data['SM']],
            'RevenueDateID': range(1, len(raw_data) + 1),
            'PO REF ID': [safe_lookup(po_ref_lookup, po) for po in raw_data['PO REF']],
            'Region_ID': [safe_lookup(region_lookup, reg) for reg in raw_data['Region']]
        })
        print(f"   Revenues_Fact: {revenues_fact.shape}")

        cash_fact = pd.DataFrame({
            'UserID': range(2000000, 2000000 + len(raw_data)),
            'Customer': raw_data['Customer'],
            'Month': raw_data['Month'],
            'Project': raw_data['Project'],
            'SM': raw_data['SM'],
            'PO REF': raw_data['PO REF'],
            'Cash Amount': raw_data['Cash Amount'],
            'Region': raw_data['Region'],
            'CustomerID': [safe_lookup(customer_lookup, cust) for cust in raw_data['Customer']],
            'ProjectID': [safe_lookup(project_lookup, proj) for proj in raw_data['Project']],
            'SMID': [safe_lookup(sm_lookup, sm) for sm in raw_data['SM']],
            'CashDateID': range(1, len(raw_data) + 1),
            'PO REF ID': [safe_lookup(po_ref_lookup, po) for po in raw_data['PO REF']],
            'Region_ID': [safe_lookup(region_lookup, reg) for reg in raw_data['Region']]
        })
        print(f"   Cash_Fact: {cash_fact.shape}")

        output_filename = transformed_file_path

        with pd.ExcelWriter(output_filename, engine='openpyxl') as writer:
            customer_dim.to_excel(writer, sheet_name='Customer_Dim', index=False)
            project_dim.to_excel(writer, sheet_name='Project_Dim', index=False)
            sm_dim.to_excel(writer, sheet_name='SM_Dim', index=False)
            date_dim.to_excel(writer, sheet_name='Date_Dim', index=False)
            po_ref_dim.to_excel(writer, sheet_name='PO REF_Dim', index=False)
            region_dim.to_excel(writer, sheet_name='Region_Dim', index=False)
            
            orders_fact.to_excel(writer, sheet_name='Orders_Fact', index=False)
            revenues_fact.to_excel(writer, sheet_name='Revenues_Fact', index=False)
            cash_fact.to_excel(writer, sheet_name='Cash_Fact', index=False)

        print(f"\n Transformation completed successfully and file saved as {output_filename}!")
        
        # Create a flag file to indicate data was updated
        with open('data_updated.txt', 'w') as f:
            f.write(str(datetime.now()))
            
        return True
    except Exception as e:
        print(f"Error in transform_data: {e}")
        traceback.print_exc()
        return False

def monitor_data_file():
    """
    Monitor data_from_db.xlsx for changes using polling
    This function runs in a separate thread
    """
    global last_modified_time, monitoring_active
    
    while monitoring_active:
        try:
            if os.path.exists(data_file_path):
                current_mtime = os.path.getmtime(data_file_path)
                
                if current_mtime > last_modified_time:
                    print(f"\nData file modified at {datetime.fromtimestamp(current_mtime)}")
                    last_modified_time = current_mtime
                    
                    # Run transformation
                    if transform_data():
                        print("Data transformation completed successfully!")
                    else:
                        print("Data transformation failed!")
                else:
                    time.sleep(2)  # Poll every 2 seconds
            else:
                print(f"Warning: {data_file_path} not found. Waiting for file...")
                time.sleep(5)  # Wait longer if file doesn't exist
        except Exception as e:
            print(f"Error in monitor_data_file: {e}")
            time.sleep(5)

def load_data():
    """
    Load data from excel_data_model_fixed.xlsx with retry/backoff to avoid reading while file is being written.
    """
    max_retries = 6
    delay = 0.2
    for attempt in range(1, max_retries + 1):
        try:
            orders = pd.read_excel(transformed_file_path, sheet_name="Orders_Fact")
            revenues = pd.read_excel(transformed_file_path, sheet_name="Revenues_Fact")
            cash = pd.read_excel(transformed_file_path, sheet_name="Cash_Fact")

            orders['Month'] = pd.to_datetime(orders['Month'], errors='coerce')
            revenues['Month'] = pd.to_datetime(revenues['Month'], errors='coerce')
            cash['Month'] = pd.to_datetime(cash['Month'], errors='coerce')
            orders['Year'] = orders['Month'].dt.year.fillna(0).astype(int)
            revenues['Year'] = revenues['Month'].dt.year.fillna(0).astype(int)
            cash['Year'] = cash['Month'].dt.year.fillna(0).astype(int)

            common_cols = [col for col in ["Customer", "Project", "Month", "SM", "PO REF", "Region"] if col in orders.columns and col in revenues.columns and col in cash.columns]
            merged = orders.merge(revenues, on=common_cols, how="outer", suffixes=("_order", "_revenue"))
            merged = merged.merge(cash, on=common_cols, how="outer", suffixes=("", "_cash"))
            merged['Year'] = merged['Month'].dt.year.fillna(0).astype(int)

            measure_cols = {
                "Order Amount": "Order Amount",
                "Revenue Amount": "Revenue Amount",
                "Cash Amount": "Cash Amount",
                "Backlog Amount": "Backlog Amount",
                "Pending Amount": "Pending Amount"     
            }
            if "Revenue Amount_revenue" in merged.columns:
                measure_cols["Revenue Amount"] = "Revenue Amount_revenue"
            elif "Revenue Amount" in merged.columns:
                measure_cols["Revenue Amount"] = "Revenue Amount"
            elif "Revenue Amount_order" in merged.columns:
                measure_cols["Revenue Amount"] = "Revenue Amount_order"

            if "Cash Amount_cash" in merged.columns:
                measure_cols["Cash Amount"] = "Cash Amount_cash"
            elif "Cash Amount" in merged.columns:
                measure_cols["Cash Amount"] = "Cash Amount"
            elif "Cash Amount_order" in merged.columns:
                measure_cols["Cash Amount"] = "Cash Amount_order"

            return orders, revenues, cash, merged, measure_cols

        except (zipfile.BadZipFile, OSError, ValueError) as e:
            print(f"Attempt {attempt}/{max_retries} - error reading transformed file: {e}")
            if attempt == max_retries:
                print("Max retries reached. Returning empty dataframes.")
                empty_df = pd.DataFrame()
                return empty_df, empty_df, empty_df, empty_df, {}
            time.sleep(delay)
            delay *= 2
        except Exception as e:
            print(f"Error in load_data: {e}")
            empty_df = pd.DataFrame()
            return empty_df, empty_df, empty_df, empty_df, {}

def is_data_updated(flag_file="data_updated.txt"):
    """Check if data was updated"""
    return os.path.exists(flag_file)

# Initialize data (load only; heavy initialization and monitoring are performed only when running the script directly)
print("Initializing dashboard...")
print("Attempting to fetch data from Google Sheets and transform...")

# Try to transform data once at startup
if get_google_sheets_data() is not None:
    transform_data()
    print("✅ Transformation completed successfully")
else:
    print("⚠️ Could not fetch data from Google Sheets, will try on next request")

orders, revenues, cash, merged, measure_cols = load_data()

# Helper functions for safe access when data is empty or missing columns (important for platform imports)
def safe_unique(column):
    """Return sorted unique values for column from merged, or empty list if not available."""
    try:
        if isinstance(merged, pd.DataFrame) and column in merged.columns:
            return sorted(merged[column].dropna().unique())
    except Exception:
        pass
    return []

def safe_month_range():
    """Return (min, max) of Month column or (today, today) when unavailable."""
    try:
        if isinstance(merged, pd.DataFrame) and 'Month' in merged.columns and not merged['Month'].dropna().empty:
            return merged['Month'].min(), merged['Month'].max()
    except Exception:
        pass
    today = pd.Timestamp.today()
    return today, today

def safe_years():
    """Return sorted list of valid years (positive ints) from merged."""
    try:
        if isinstance(merged, pd.DataFrame) and 'Year' in merged.columns:
            years = sorted([y for y in merged['Year'].dropna().unique() if y and y > 0])
            return years
    except Exception:
        pass
    return []

dropdown_cols = ["Customer", "Project", "SM", "PO REF"]

# Single Dash app instance
app = dash.Dash(__name__, suppress_callback_exceptions=True)
app.title = "SM Insight Board"
server = app.server

# Health endpoint for platform checks (useful for Railway / monitoring)
@server.route("/health")
def health():
    return "OK", 200

def dropdown_filter(id, column):
    opts = safe_unique(column)
    return dcc.Dropdown(
        id=id,
        options=[{"label": val, "value": val} for val in opts],
        placeholder=f"Select {column}",
        searchable=True,
        clearable=True,
        style={'width': '100%', 'fontSize': '14px', 'height': '34px', 'verticalAlign': 'middle'}
    )

def date_range_filter(id):
    start_date, end_date = safe_month_range()
    return dcc.DatePickerRange(
        id=id,
        start_date=start_date,
        end_date=end_date,
        display_format='YYYY-MM-DD',
        style={'width': '100%'}
    )

def year_filter(id):
    years = safe_years()
    return dcc.Dropdown(
        id=id,
        options=[{"label": str(int(year)), "value": int(year)} for year in years],
        placeholder="Select Year",
        searchable=True,
        clearable=True,
        style={'width': '100%', 'fontSize': '14px', 'height': '34px', 'verticalAlign': 'middle'}
    )

def period_filter(id):
    return dcc.Dropdown(
        id=id,
        options=[
            {"label": "Quarterly", "value": "Quarterly"},
            {"label": "Monthly", "value": "Monthly"}
        ],
        placeholder="Select Period",
        searchable=False,
        clearable=True,
        style={'width': '100%', 'fontSize': '14px', 'height': '34px', 'verticalAlign': 'middle'}
    )

def region_filter(id):
    regions = safe_unique('Region')
    region_opts = [{"label": "All Regions", "value": "All"}] + [{"label": region, "value": region} for region in regions] if regions else [{"label": "All Regions", "value": "All"}]
    return dcc.Dropdown(
        id=id,
        options=region_opts,
        value="All",
        placeholder="Select Region",
        searchable=True,
        clearable=True,
        style={'width': '100%', 'fontSize': '14px', 'height': '34px', 'verticalAlign': 'middle'}
    )

def create_chart_card(fig, chart_id):
    title = fig.layout.title.text or "Chart"
    return html.Div([
        dcc.Graph(figure=fig, config={'displayModeBar': False}, style={'height': '300px'}),
        html.Button(f"Export '{title}' Data", id=f"export-{chart_id}", style={'marginTop': '6px', 'fontSize': '0.75rem', 'padding': '2px 6px'})
    ], style={'width': '36%', 'display': 'inline-block', 'verticalAlign': 'top', 'padding': '6px', 'boxShadow': '0 0 8px #ccc', 'borderRadius': '10px', 'marginBottom': '14px'})

def create_kpi_card(title, value_id, card_id):
    return html.Div(
        id=card_id,
        children=[
            html.H3(title, style={'fontWeight': 'normal', 'margin': '0', 'fontSize': '0.85rem', 'whiteSpace': 'nowrap', 'padding': '0'}),
            html.H2(id=value_id, style={'margin': '0', 'fontSize': '1.1rem', 'padding': '0'})
        ],
        style={'padding': '4px', 'textAlign': 'center', 'cursor': 'pointer'}, # Reduced padding
        n_clicks=0
    )

def create_page1_layout():
    return html.Div([
        # Use actual column name from measure_cols if available to keep store consistent with dataframe columns
        dcc.Store(id='measure-store', data=measure_cols.get("Order Amount", "Order Amount")),
        dcc.Store(id='p1-chart1-store'),
        dcc.Store(id='p1-chart2-store'),
        dcc.Store(id='p1-filter-store'),
        dcc.Download(id="download-p1-chart1"),
        dcc.Download(id="download-p1-chart2"),
        html.Div([
            html.Div([
                html.Div([
                    html.Div([dropdown_filter('dropdown1', dropdown_cols[0])], style={'width': '30%', 'display': 'inline-block', 'padding': '3px'}),
                    html.Div([dropdown_filter('dropdown3', dropdown_cols[2])], style={'width': '30%', 'display': 'inline-block', 'padding': '3px'}),
                    html.Div([dropdown_filter('dropdown2', dropdown_cols[1])], style={'width': '30%', 'display': 'inline-block', 'padding': '3px'}),
                ]),
                html.Div([
                    html.Div([dropdown_filter('dropdown4', dropdown_cols[3])], style={'width': '30%', 'display': 'inline-block', 'padding': '3px'}),
                    html.Div([year_filter('year-filter1')], style={'width': '30%', 'display': 'inline-block', 'padding': '3px'}),
                    html.Div([region_filter('p1-region-filter')], style={'width': '30%', 'display': 'inline-block', 'padding': '3px'}),
                ]),
                html.Div([
                    html.Div([period_filter('p1-period-filter')], style={'width': '30%', 'display': 'inline-block', 'padding': '3px'})
                ])
            ], style={'flex': '0.7'}),
           
            html.Div([
                html.Div([
                    # Row 1
                    html.Div([
                        html.Div(create_kpi_card("Order Amount", "orders-card-value", "orders-card"), style={'flex': 1, 'borderRight': '1px solid #ddd'}),
                        html.Div(create_kpi_card("Revenue Amount", "revenues-card-value", "revenues-card"), style={'flex': 1, 'borderRight': '1px solid #ddd'}),
                        html.Div(create_kpi_card("Cash Amount", "cash-card-value", "cash-card"), style={'flex': 1}),
                    ], style={'display': 'flex', 'width': '100%', 'borderBottom': '1px solid #ddd'}),
                    # Row 2
                    html.Div([
                        html.Div(create_kpi_card("Backlog Amount", "backlog-card-value", "backlog-card"), style={'flex': 1, 'borderRight': '1px solid #ddd'}),
                        html.Div(create_kpi_card("Pending Amount", "pending-card-value", "pending-card"), style={'flex': 1, 'borderRight': '1px solid #ddd'}),
                        html.Div(create_kpi_card("PO Count", "po-count-value", "po-count-card"), style={'flex': 1}),
                    ], style={'display': 'flex', 'width': '100%'}),
                ], style={'border': '1px solid #ddd', 'borderRadius': '5px', 'overflow': 'hidden'})
            ], style={'display': 'flex', 'flex': '0.3', 'flexDirection': 'column', 'alignItems': 'stretch', 'justifyContent': 'center'}),

        ], style={'display': 'flex', 'flexDirection': 'row'}),
        
        html.Hr(style={'margin': '8px 0'}),
        dcc.Loading(type="circle", children=html.Div(id="page1-cards")),
        dcc.Loading(type="circle", children=html.Div(id="sm-summary-table"))
    ], style={'backgroundColor': '#f8f9fa', 'padding': '8px', 'borderRadius': '6px', 'border': '1px solid #dee2e6'})

def create_page2_layout():
    return html.Div([
        # Default to actual column name for Revenue Amount
        dcc.Store(id='region-measure-store', data=measure_cols.get("Revenue Amount", "Revenue Amount")),
        dcc.Store(id='p2-chart1-store'),
        dcc.Store(id='p2-chart2-store'),
        dcc.Store(id='p2-filter-store'),
        dcc.Download(id="download-p2-chart1"),
        dcc.Download(id="download-p2-chart2"),
        html.Div([
            html.Div([
                html.Div([
                    html.Div([dropdown_filter('region-dropdown1', dropdown_cols[0])], style={'width': '30%', 'display': 'inline-block', 'padding': '3px'}),
                    html.Div([dropdown_filter('region-dropdown3', dropdown_cols[2])], style={'width': '30%', 'display': 'inline-block', 'padding': '3px'}),
                    html.Div([dropdown_filter('region-dropdown2', dropdown_cols[1])], style={'width': '30%', 'display': 'inline-block', 'padding': '3px'}),
                ]),
                html.Div([
                    html.Div([dropdown_filter('region-dropdown4', dropdown_cols[3])], style={'width': '30%', 'display': 'inline-block', 'padding': '3px'}),
                    html.Div([year_filter('region-year-filter')], style={'width': '30%', 'display': 'inline-block', 'padding': '3px'}),
                    html.Div([region_filter('specific-region-filter')], style={'width': '30%', 'display': 'inline-block', 'padding': '3px'}),
                ]),
                html.Div([
                    html.Div([period_filter('p2-period-filter')], style={'width': '30%', 'display': 'inline-block', 'padding': '3px'})
                ])
            ], style={'flex': '0.7'}),
            html.Div([
                html.Div([
                    # Row 1
                    html.Div([
                        html.Div(create_kpi_card("Order Amount", "region-orders-card-value", "region-orders-card"), style={'flex': 1, 'borderRight': '1px solid #ddd'}),
                        html.Div(create_kpi_card("Revenue Amount", "region-revenue-card-value", "region-revenue-card"), style={'flex': 1, 'borderRight': '1px solid #ddd'}),
                        html.Div(create_kpi_card("Cash Amount", "region-cash-card-value", "region-cash-card"), style={'flex': 1}),
                    ], style={'display': 'flex', 'width': '100%', 'borderBottom': '1px solid #ddd'}),
                    # Row 2
                    html.Div([
                        html.Div(create_kpi_card("Backlog Amount", "region-backlog-card-value", "region-backlog-card"), style={'flex': 1, 'borderRight': '1px solid #ddd'}),
                        html.Div(create_kpi_card("Pending Amount", "region-pending-card-value", "region-pending-card"), style={'flex': 1, 'borderRight': '1px solid #ddd'}),
                        html.Div(create_kpi_card("PO Count", "region-po-count-value", "region-po-count-card"), style={'flex': 1}),
                    ], style={'display': 'flex', 'width': '100%'}),
                ], style={'border': '1px solid #ddd', 'borderRadius': '5px', 'overflow': 'hidden'})
            ], id="region-kpi-cards-container", style={'display': 'flex', 'flex': '0.3', 'flexDirection': 'column', 'alignItems': 'stretch', 'justifyContent': 'center'}),
        ], style={'display': 'flex', 'flexDirection': 'row'}),
        
        html.Hr(style={'margin': '8px 0'}),
        dcc.Loading(type="circle", children=html.Div(id="region-charts")),
        dcc.Loading(type="circle", children=html.Div(id="region-summary-table"))
    ], style={'backgroundColor': '#f8f9fa', 'padding': '8px', 'borderRadius': '6px', 'border': '1px solid #dee2e6'})

def create_page3_layout():
    return html.Div([
        dcc.Store(id='sm-measure-store', data=measure_cols.get("Order Amount", "Order Amount")),
        dcc.Store(id='p3-chart1-store'),
        dcc.Store(id='p3-chart2-store'),
        dcc.Store(id='p3-filter-store'),
        dcc.Download(id="download-p3-chart1"),
        dcc.Download(id="download-p3-chart2"),
        html.Div([
            html.Div([
                html.Div([
                    html.Div([dropdown_filter('sm-dropdown1', dropdown_cols[0])], style={'width': '30%', 'display': 'inline-block', 'padding': '3px'}),
                    html.Div([dropdown_filter('sm-dropdown3', dropdown_cols[2])], style={'width': '30%', 'display': 'inline-block', 'padding': '3px'}),
                    html.Div([dropdown_filter('sm-dropdown2', dropdown_cols[1])], style={'width': '30%', 'display': 'inline-block', 'padding': '3px'}),
                ]),
                html.Div([
                    html.Div([dropdown_filter('sm-dropdown4', dropdown_cols[3])], style={'width': '30%', 'display': 'inline-block', 'padding': '3px'}),
                    html.Div([year_filter('sm-year-filter')], style={'width': '30%', 'display': 'inline-block', 'padding': '3px'}),
                    html.Div([region_filter('sm-region-filter')], style={'width': '30%', 'display': 'inline-block', 'padding': '3px'}),
                ]),
                html.Div([
                    html.Div([period_filter('p3-period-filter')], style={'width': '30%', 'display': 'inline-block', 'padding': '3px'})
                ])
            ], style={'flex': '0.7'}),
            html.Div([
                html.Div([
                    # Row 1
                    html.Div([
                        html.Div(create_kpi_card("Order Amount", "sm-orders-card-value", "sm-orders-card"), style={'flex': 1, 'borderRight': '1px solid #ddd'}),
                        html.Div(create_kpi_card("Revenue Amount", "sm-revenue-card-value", "sm-revenue-card"), style={'flex': 1, 'borderRight': '1px solid #ddd'}),
                        html.Div(create_kpi_card("Cash Amount", "sm-cash-card-value", "sm-cash-card"), style={'flex': 1}),
                    ], style={'display': 'flex', 'width': '100%', 'borderBottom': '1px solid #ddd'}),
                    # Row 2
                    html.Div([
                        html.Div(create_kpi_card("Backlog Amount", "sm-backlog-card-value", "sm-backlog-card"), style={'flex': 1, 'borderRight': '1px solid #ddd'}),
                        html.Div(create_kpi_card("Pending Amount", "sm-pending-card-value", "sm-pending-card"), style={'flex': 1, 'borderRight': '1px solid #ddd'}),
                        html.Div(create_kpi_card("PO Count", "sm-po-count-value", "sm-po-count-card"), style={'flex': 1}),
                    ], style={'display': 'flex', 'width': '100%'}),
                ], style={'border': '1px solid #ddd', 'borderRadius': '5px', 'overflow': 'hidden'})
            ], id="sm-kpi-cards-container", style={'display': 'flex', 'flex': '0.3', 'flexDirection': 'column', 'alignItems': 'stretch', 'justifyContent': 'center'}),
        ], style={'display': 'flex', 'flexDirection': 'row'}),
        
        html.Hr(style={'margin': '8px 0'}),
        dcc.Loading(type="circle", children=html.Div(id="sm-charts")),
        dcc.Loading(type="circle", children=html.Div(id="sm-summary-table-page3"))
    ], style={'backgroundColor': '#f8f9fa', 'padding': '8px', 'borderRadius': '6px', 'border': '1px solid #dee2e6'})

def create_page4_layout():
    return html.Div([
        dcc.Store(id='year-measure-store', data=measure_cols.get("Revenue Amount", "Revenue Amount")),
        dcc.Store(id='p4-chart1-store'),
        dcc.Store(id='p4-chart2-store'),
        dcc.Store(id='p4-filter-store'),
        dcc.Download(id="download-p4-chart1"),
        dcc.Download(id="download-p4-chart2"),
        html.Div([
            html.Div([
                html.Div([
                    html.Div([dropdown_filter('year-dropdown1', dropdown_cols[0])], style={'width': '30%', 'display': 'inline-block', 'padding': '3px'}),
                    html.Div([dropdown_filter('year-dropdown3', dropdown_cols[2])], style={'width': '30%', 'display': 'inline-block', 'padding': '3px'}),
                    html.Div([dropdown_filter('year-dropdown2', dropdown_cols[1])], style={'width': '30%', 'display': 'inline-block', 'padding': '3px'}),
                ]),
                html.Div([
                    html.Div([dropdown_filter('year-dropdown4', dropdown_cols[3])], style={'width': '30%', 'display': 'inline-block', 'padding': '3px'}),
                    html.Div([year_filter('p4-year-filter')], style={'width': '30%', 'display': 'inline-block', 'padding': '3px'}),
                    html.Div([region_filter('year-region-filter')], style={'width': '30%', 'display': 'inline-block', 'padding': '3px'}),
                ]),
                html.Div([
                    html.Div([period_filter('p4-period-filter')], style={'width': '30%', 'display': 'inline-block', 'padding': '3px'})
                ])
            ], style={'flex': '0.7'}),
            html.Div([
                html.Div([
                    # Row 1
                    html.Div([
                        html.Div(create_kpi_card("Order Amount", "year-orders-card-value", "year-orders-card"), style={'flex': 1, 'borderRight': '1px solid #ddd'}),
                        html.Div(create_kpi_card("Revenue Amount", "year-revenue-card-value", "year-revenue-card"), style={'flex': 1, 'borderRight': '1px solid #ddd'}),
                        html.Div(create_kpi_card("Cash Amount", "year-cash-card-value", "year-cash-card"), style={'flex': 1}),
                    ], style={'display': 'flex', 'width': '100%', 'borderBottom': '1px solid #ddd'}),
                    # Row 2
                    html.Div([
                        html.Div(create_kpi_card("Backlog Amount", "year-backlog-card-value", "year-backlog-card"), style={'flex': 1, 'borderRight': '1px solid #ddd'}),
                        html.Div(create_kpi_card("Pending Amount", "year-pending-card-value", "year-pending-card"), style={'flex': 1, 'borderRight': '1px solid #ddd'}),
                        html.Div(create_kpi_card("PO Count", "year-po-count-value", "year-po-count-card"), style={'flex': 1}),
                    ], style={'display': 'flex', 'width': '100%'}),
                ], style={'border': '1px solid #ddd', 'borderRadius': '5px', 'overflow': 'hidden'})
            ], id="year-kpi-cards-container", style={'display': 'flex', 'flex': '0.3', 'flexDirection': 'column', 'alignItems': 'stretch', 'justifyContent': 'center'}),
        ], style={'display': 'flex', 'flexDirection': 'row'}),
        
        html.Hr(style={'margin': '8px 0'}),
        dcc.Loading(type="circle", children=html.Div(id="year-charts")),
        dcc.Loading(type="circle", children=html.Div(id="year-summary-table"))
    ], style={'backgroundColor': '#f8f9fa', 'padding': '8px', 'borderRadius': '6px', 'border': '1px solid #dee2e6'})

def create_main_dashboard_layout():
    return html.Div([
        dcc.Store(id='main-chart1-store'),
        dcc.Store(id='main-chart2-store'),
        dcc.Store(id='main-filter-store'),
        dcc.Download(id="download-main-chart1"),
        dcc.Download(id="download-main-chart2"),
        html.Div([
            html.Div([
                html.Div([
                    html.Div([dcc.Dropdown(id='main-year', placeholder='Select Year', clearable=True, style={'width': '100%', 'fontSize': '14px', 'height': '34px'})], style={'width': '30%', 'display': 'inline-block', 'padding': '3px'}),
                    html.Div([dcc.Dropdown(id='main-region', placeholder='Select Region', clearable=True, style={'width': '100%', 'fontSize': '14px', 'height': '34px'})], style={'width': '30%', 'display': 'inline-block', 'padding': '3px'}),
                    html.Div([dcc.Dropdown(id='main-sm', placeholder='Select SM', clearable=True, style={'width': '100%', 'fontSize': '14px', 'height': '34px'})], style={'width': '30%', 'display': 'inline-block', 'padding': '3px'}),
                ]),
                html.Div([
                    html.Div([dcc.Dropdown(id='main-period', options=[{'label': 'Quarterly', 'value': 'Quarterly'}, {'label': 'Monthly', 'value': 'Monthly'}], placeholder='Select Period', clearable=True, style={'width': '100%', 'fontSize': '14px', 'height': '34px'})], style={'width': '30%', 'display': 'inline-block', 'padding': '3px'}),
                ])
            ], style={'flex': '0.7'}),
            html.Div([
                html.Div([
                    html.Div([
                        html.Div(create_kpi_card("Order Amount", "main-orders", "main-orders-card"), style={'flex': 1, 'borderRight': '1px solid #ddd'}),
                        html.Div(create_kpi_card("Revenue Amount", "main-revenue", "main-revenue-card"), style={'flex': 1, 'borderRight': '1px solid #ddd'}),
                        html.Div(create_kpi_card("Cash Amount", "main-cash", "main-cash-card"), style={'flex': 1}),
                    ], style={'display': 'flex', 'width': '100%', 'borderBottom': '1px solid #ddd'}),
                    html.Div([
                        html.Div(create_kpi_card("Backlog Amount", "main-backlog", "main-backlog-card"), style={'flex': 1, 'borderRight': '1px solid #ddd'}),
                        html.Div(create_kpi_card("Pending Amount", "main-pending", "main-pending-card"), style={'flex': 1, 'borderRight': '1px solid #ddd'}),
                        html.Div(create_kpi_card("PO Count", "main-po-count", "main-po-count-card"), style={'flex': 1}),
                    ], style={'display': 'flex', 'width': '100%'}),
                ], style={'border': '1px solid #ddd', 'borderRadius': '5px', 'overflow': 'hidden'})
            ], style={'display': 'flex', 'flex': '0.3', 'flexDirection': 'column', 'alignItems': 'stretch', 'justifyContent': 'center'}),
        ], style={'display': 'flex', 'flexDirection': 'row'}),
        html.Hr(style={'margin': '8px 0'}),
        dcc.Loading(type="circle", children=html.Div(id='main-charts')),
        dcc.Loading(type="circle", children=html.Div(id='main-summary-table'))
    ], style={'backgroundColor': '#f8f9fa', 'padding': '8px', 'borderRadius': '6px', 'border': '1px solid #dee2e6'})

app.layout = html.Div([
    dcc.Location(id='url', refresh=False),
    dcc.Store(id='shared-dropdowns', data={}),
    html.Div([
        # Header
        html.Div([
            # Logo (left)
            html.Img(src='/assets/gif1.gif', style={'height': '48px', 'marginRight': '18px', 'marginTop': '-8px'}),
            # Titles and glassy line (right)
            html.Div([
                html.H1(
                    "SM Insight Board",
                    style={
                        'textAlign': 'center',
                        'margin': '0',
                        'marginTop': '0',
                        'padding': '0',
                        'paddingTop': '0',
                        'color': '#2c3e50',
                        'fontSize': '1.3rem',
                        'fontWeight': 'bold',
                        'fontFamily': 'Lora, serif',
                    }
                ),
                html.Div(style={
                    'height': '5px',
                    'width': '100%',
                    'background': 'linear-gradient(90deg, #1976d2 0%, #40bfff 100%)',
                    'borderRadius': '6px',
                    'boxShadow': '0 2px 12px 0 rgba(25, 118, 210, 0.25)',
                    'backdropFilter': 'blur(2px)',
                    'opacity': 0.85,
                    'margin': '0.5rem 0 1.2rem 0',
                }),
            ], style={'flexGrow': 1}),
        ], style={'width': '100%', 'display': 'flex', 'alignItems': 'center', 'marginBottom': '10px', 'marginTop': '0', 'paddingTop': '0'}),
        # Navigation
        html.Div([
            html.Div(id='navigation-links', style={'flex': '1', 'display': 'flex', 'alignItems': 'center'})
        ], style={'display': 'flex', 'alignItems': 'center', 'justifyContent': 'flex-start', 'marginBottom': '0'}),
        html.Hr(style={'margin': '8px 0 10px 0', 'borderColor': '#dee2e6'}),
        # Main content area - all pages are rendered here but hidden/shown by a callback
        html.Div(id='page-content', children=[
            # Page 1: Main Dashboard (mounted at '/')
            html.Div(create_main_dashboard_layout(), id='page-1-layout', style={'display': 'block'}),
            # Page 2: Region Analysis (mounted at '/page-2')
            html.Div(create_page2_layout(), id='page-2-layout', style={'display': 'none'}),
            # Page 3: SM Analysis (mounted at '/page-3')
            html.Div(create_page3_layout(), id='page-3-layout', style={'display': 'none'}),
            # Page 4: Year-wise Analysis (mounted at '/page-4')
            html.Div(create_page4_layout(), id='page-4-layout', style={'display': 'none'}),
            # Page 5: PO Analysis (mounted at '/page-5')
            html.Div(create_page1_layout(), id='page-5-layout', style={'display': 'none'})
        ])
    ], className='dash-container'),
    dcc.Interval(id='data-refresh-interval', interval=600*1000, n_intervals=0),  # every 600 seconds [10 minutes]
    # hidden fast interval (enabled when data update flag is present)
    dcc.Interval(id='fast-data-refresh-interval', interval=5*1000, n_intervals=0, disabled=True, max_intervals=-1),  # 5 seconds
    # short-check interval used to detect the data_updated.txt flag quickly and enable fast polling
    dcc.Interval(id='flag-check-interval', interval=3*1000, n_intervals=0)
], style={
    'backgroundColor': '#f5f5f5',
    'minHeight': '100vh',
    'margin': '0',
    'padding': '0'
})

@app.callback(
    Output('navigation-links', 'children'),
    Input('url', 'pathname')
)
def update_navigation(pathname):
    nav_items = [
        {'name': '🏠 Main Dashboard', 'href': '/', 'active': pathname == '/' or pathname is None},
        {'name': '🌍 Region Analysis', 'href': '/page-2', 'active': pathname == '/page-2'},
        {'name': '👥 SM Analysis', 'href': '/page-3', 'active': pathname == '/page-3'},
        {'name': '📈 Year-wise Analysis', 'href': '/page-4', 'active': pathname == '/page-4'},
        {'name': '📋 PO Analysis', 'href': '/page-5', 'active': pathname == '/page-5'}
    ]
    navigation_links = []
    for i, item in enumerate(nav_items):
        if item['active']:
            link_style = {
                'color': 'white',
                'backgroundColor': '#1976d2',
                'fontWeight': 'bold',
                'textDecoration': 'none',
                'padding': '6.5px 18px',
                'borderRadius': '22px',
                'border': 'none',
                'boxShadow': '0 2px 8px rgba(25, 118, 210, 0.10)',
                'fontSize': '0.98rem',
            }
        else:
            link_style = {
                'color': '#1976d2',
                'backgroundColor': 'white',
                'fontWeight': 'bold',
                'textDecoration': 'none',
                'padding': '6.5px 18px',
                'borderRadius': '22px',
                'border': '1.5px solid #1976d2',
                'boxShadow': '0 2px 8px rgba(25, 118, 210, 0.05)',
                'fontSize': '0.98rem',
            }
        navigation_links.append(
            dcc.Link(item['name'], href=item['href'], style=link_style, className='nav-bubble')
        )
    return html.Div(
        navigation_links,
        style={
            'textAlign': 'left',
            'marginBottom': '0',
            'display': 'flex',
            'justifyContent': 'flex-start',
            'gap': '20px',
            'paddingLeft': '8px'
        }
    )

@app.callback(
    [Output('page-1-layout', 'style'),
     Output('page-2-layout', 'style'),
     Output('page-3-layout', 'style'),
     Output('page-4-layout', 'style'),
     Output('page-5-layout', 'style')],
    Input('url', 'pathname')
)
def display_page(pathname):
    hide = {'display': 'none'}
    show = {'display': 'block'}
    
    if pathname == '/page-2':
        # Region Analysis
        return hide, show, hide, hide, hide
    elif pathname == '/page-3':
        # SM Analysis
        return hide, hide, show, hide, hide
    elif pathname == '/page-4':
        # Year-wise Analysis
        return hide, hide, hide, show, hide
    elif pathname == '/page-5':
        # PO Analysis
        return hide, hide, hide, hide, show
    else:
        # Default (root): Main Dashboard
        return show, hide, hide, hide, hide
        
@app.callback(
    Output('measure-store', 'data'),
    [Input('orders-card', 'n_clicks'),
     Input('revenues-card', 'n_clicks'),
     Input('cash-card', 'n_clicks'),
     Input('backlog-card', 'n_clicks'),
     Input('pending-card', 'n_clicks')],
    prevent_initial_call=True
)
def update_selected_measure(orders_clicks, revenues_clicks, cash_clicks, backlog_clicks, pending_clicks):
    ctx = callback_context
    if not ctx.triggered:
        return dash.no_update
    
    button_id = ctx.triggered[0]['prop_id'].split('.')[0]
    
    if button_id == 'orders-card':
        return measure_cols["Order Amount"]
    elif button_id == 'revenues-card':
        return measure_cols["Revenue Amount"]
    elif button_id == 'cash-card':
        return measure_cols["Cash Amount"]
    elif button_id == 'backlog-card':
        return measure_cols["Backlog Amount"]
    elif button_id == 'pending-card':
        return measure_cols["Pending Amount"]
    return dash.no_update

@app.callback(
    [Output("page1-cards", "children"),
     Output("orders-card-value", "children"),
     Output("revenues-card-value", "children"),
     Output("cash-card-value", "children"),
     Output("backlog-card-value", "children"),
     Output("pending-card-value", "children"),
     Output("po-count-value", "children"),
     Output('orders-card', 'style'),
     Output('revenues-card', 'style'),
     Output('cash-card', 'style'),
     Output('backlog-card', 'style'),
     Output('pending-card', 'style'),
     Output('po-count-card', 'style'),
     Output("sm-summary-table", "children"),
     Output('p1-chart1-store', 'data'),
     Output('p1-chart2-store', 'data'),
     Output('p1-filter-store', 'data')],
    [Input('dropdown1', 'value'),
     Input('dropdown2', 'value'),
     Input('dropdown3', 'value'),
     Input('dropdown4', 'value'),
     Input('p1-region-filter', 'value'),
     Input('year-filter1', 'value'),
     Input('p1-period-filter', 'value'),
     Input('measure-store', 'data'),
     Input('data-refresh-interval', 'n_intervals'),
     Input('fast-data-refresh-interval', 'n_intervals')]
)
def update_page_content(d1, d2, d3, d4, region, year_filter, p1_period, selected_measure, n_intervals, fast_n_intervals):
    if is_data_updated():
        orders, revenues, cash, merged, measure_cols = load_data()
        try:
            os.remove('data_updated.txt')
        except Exception:
            pass
    else:
        orders, revenues, cash, merged, measure_cols = load_data()
    try:
        filtered_df = merged.copy()
        filters = [d1, d2, d3, d4]
        for i, col in enumerate(dropdown_cols):
            if filters[i]:
                filtered_df = filtered_df[filtered_df[col] == filters[i]]
        if year_filter:
            filtered_df = filtered_df[filtered_df['Year'] == year_filter]
        if region and region != "All":
            filtered_df = filtered_df[filtered_df['Region'] == region]
        if filtered_df.empty or selected_measure not in filtered_df.columns:
            default = "N/A"
            default_style = {'padding': '10px', 'textAlign': 'center', 'cursor': 'pointer', 'border': '1px solid #ddd', 'borderRadius': '5px', 'margin': '0 5px'}
            empty_table = dash_table.DataTable(columns=[], data=[])
            return [
                [],  # charts
                default, default, default, default, default, default,  # card values
                default_style, default_style, default_style, default_style, default_style, default_style,  # card styles
                [empty_table],  # sm-summary-table.children (should be a list)
                None, None, None  # chart store data, filter store data
            ]
        total_orders = filtered_df[measure_cols["Order Amount"]].sum()
        total_revenues = filtered_df[measure_cols["Revenue Amount"]].sum()
        total_cash = filtered_df[measure_cols["Cash Amount"]].sum()
        total_backlog = filtered_df[measure_cols["Backlog Amount"]].sum()
        total_pending = filtered_df[measure_cols["Pending Amount"]].sum()
        po_count = filtered_df['PO REF'].nunique()
        
        period_label = None
        if p1_period:
            if p1_period == 'Quarterly':
                filtered_df['Period'] = filtered_df['Month'].dt.to_period('Q').astype(str)
                period_label = 'Period'
            elif p1_period == 'Monthly':
                filtered_df['Period'] = filtered_df['Month'].dt.to_period('M').astype(str)
                period_label = 'Period'

        df1 = filtered_df.groupby(dropdown_cols[0], as_index=False)[selected_measure].sum().sort_values(selected_measure, ascending=False)
        
        try:
            if not is_valid_for_plot(df1, selected_measure):
                fig1 = px.bar(title="No data available")
            else:
                # Clean data to ensure no invalid values
                clean_df1 = df1.copy()
                clean_df1[selected_measure] = pd.to_numeric(clean_df1[selected_measure], errors='coerce')
                clean_df1 = clean_df1.dropna(subset=[selected_measure])
                
                if clean_df1.empty:
                    fig1 = px.bar(title="No valid data available")
                else:
                    fig1 = px.bar(
                        clean_df1, 
                        x=dropdown_cols[0], 
                        y=selected_measure, 
                        title=f"{dropdown_cols[0]} Breakdown"
                    )
        except Exception as e:
            print(f"Error creating chart 1: {e}")
            fig1 = px.bar(title="Error loading chart")
        
        fig1.update_layout(title_font_size=12, title_x=0.5, margin=dict(t=40, b=20, l=20, r=20))
        
        # Second chart: if Period selected, show Period comparison; else keep existing dimension breakdown
        if period_label:
            df2 = filtered_df.groupby('Period', as_index=False)[selected_measure].sum().sort_values(selected_measure, ascending=False)
        else:
            df2 = filtered_df.groupby(dropdown_cols[1], as_index=False)[selected_measure].sum().sort_values(selected_measure, ascending=False)
        
        try:
            if not is_valid_for_plot(df2, selected_measure):
                fig2 = px.pie(title="No data available") if not period_label else px.bar(title="No data available")
            else:
                clean_df2 = df2.copy()
                clean_df2[selected_measure] = pd.to_numeric(clean_df2[selected_measure], errors='coerce')
                clean_df2 = clean_df2.dropna(subset=[selected_measure])
                
                if clean_df2.empty:
                    fig2 = px.pie(title="No valid data available") if not period_label else px.bar(title="No valid data available")
                else:
                    if period_label:
                        fig2 = px.bar(
                            clean_df2,
                            x='Period',
                            y=selected_measure,
                            title=f"{selected_measure} by {p1_period}"
                        )
                    else:
                        fig2 = px.pie(
                            clean_df2, 
                            names=dropdown_cols[1], 
                            values=selected_measure, 
                            title=f"{dropdown_cols[1]} Breakdown"
                        )
        except Exception as e:
            print(f"Error creating chart 2: {e}")
            fig2 = px.pie(title="Error loading chart")
        
        fig2.update_layout(title_font_size=12, title_x=0.5, margin=dict(t=40, b=20, l=20, r=20))
        charts = html.Div([
            create_chart_card(fig1, "p1-chart1"), 
            create_chart_card(fig2, "p1-chart2")
        ], style={'display': 'flex', 'justifyContent': 'center', 'gap': '50px'})
        default_style = {'padding': '10px', 'textAlign': 'center', 'cursor': 'pointer', 'border': '1px solid #ddd', 'borderRadius': '5px', 'margin': '0 5px'}
        active_style = default_style.copy()
        active_style['border'] = '2px solid #007BFF'
        active_style['boxShadow'] = '0 0 5px #007BFF'
        orders_style = active_style if selected_measure == measure_cols["Order Amount"] else default_style
        revenues_style = active_style if selected_measure == measure_cols["Revenue Amount"] else default_style
        cash_style = active_style if selected_measure == measure_cols["Cash Amount"] else default_style
        backlog_style = active_style if selected_measure == "Backlog Amount" else default_style
        pending_style = active_style if selected_measure == "Pending Amount" else default_style
        po_count_style = default_style
        formatted_orders = f"€{total_orders:,.0f}"
        formatted_revenues = f"€{total_revenues:,.0f}"
        formatted_cash = f"€{total_cash:,.0f}"
        formatted_backlog = f"€{total_backlog:,.0f}"
        formatted_pending = f"€{total_pending:,.0f}"
        formatted_po_count = f"{po_count:,}"
        group_keys = ["SM", "Project"]
        if period_label:
            group_keys = ['Period'] + group_keys
        summary_df = filtered_df.groupby(group_keys, as_index=False).agg({
            measure_cols["Order Amount"]: "sum",
            measure_cols["Revenue Amount"]: "sum",
            measure_cols["Cash Amount"]: "sum",
            "Backlog Amount": "sum",
            "Pending Amount": "sum"
        }).rename(columns={
            measure_cols["Order Amount"]: "Order Amount",
            measure_cols["Revenue Amount"]: "Revenue Amount",
            measure_cols["Cash Amount"]: "Cash Amount",
            "Backlog Amount": "Backlog Amount",
            "Pending Amount": "Pending Amount"
        }).sort_values(by="Order Amount", ascending=False)
        table_data = summary_df.to_dict("records")
        table_columns = []
        if period_label:
            table_columns.append({"name": "Period", "id": "Period"})
        table_columns.extend([
            {"name": "SM", "id": "SM"},
            {"name": "Project", "id": "Project"},
            {"name": "Order Amount", "id": "Order Amount", "type": "numeric", "format": {"specifier": ",.0f"}},
            {"name": "Revenue Amount", "id": "Revenue Amount", "type": "numeric", "format": {"specifier": ",.0f"}},
            {"name": "Cash Amount", "id": "Cash Amount", "type": "numeric", "format": {"specifier": ",.0f"}},
            {"name": "Backlog Amount", "id": "Backlog Amount", "type": "numeric", "format": {"specifier": ",.0f"}},
            {"name": "Pending Amount", "id": "Pending Amount", "type": "numeric", "format": {"specifier": ",.0f"}},
        ])
        table = dash_table.DataTable(
            columns=table_columns,
            data=table_data,
            style_table={'overflowX': 'auto', 'borderRadius': '8px', 'border': '1px solid #1976d2'},
            style_cell={'textAlign': 'center', 'padding': '8px'},
            style_header={'fontWeight': 'bold', 'backgroundColor': '#1976d2', 'color': 'white', 'border': 'none'},
            style_data_conditional=[{'if': {'row_index': 'even'}, 'backgroundColor': '#e3f2fd'}],
            export_format='csv',
        )
        chart1_data = {'df': df1.to_json(orient='split'), 'title': fig1.layout.title.text}
        chart2_data = {'df': df2.to_json(orient='split'), 'title': fig2.layout.title.text}
        active_filters = {dropdown_cols[i]: filters[i] for i, f in enumerate(filters) if f}
        if region and region != "All":
            active_filters['Region'] = region
        if year_filter:
            active_filters['Year'] = year_filter
        if p1_period:
            active_filters['Period'] = p1_period
        return charts, formatted_orders, formatted_revenues, formatted_cash, formatted_backlog, formatted_pending, formatted_po_count, orders_style, revenues_style, cash_style, backlog_style, pending_style, po_count_style, [html.Div([table], style={'marginLeft': '2cm', 'marginRight': '2cm'})], json.dumps(chart1_data), json.dumps(chart2_data), json.dumps(active_filters)
    except Exception as e:
        print(f"Exception in update_page_content: {e}")
        traceback.print_exc()
        default = "N/A"
        default_style = {'padding': '10px', 'textAlign': 'center', 'cursor': 'pointer', 'border': '1px solid #ddd', 'borderRadius': '5px', 'margin': '0 5px'}
        empty_table = dash_table.DataTable(columns=[], data=[])
        return [
            [], default, default, default, default, default, default,
            default_style, default_style, default_style, default_style, default_style, default_style,
            [html.Div([empty_table], style={'marginLeft': '2cm', 'marginRight': '2cm'})], None, None, None
        ]

def _create_export_data(n_clicks, chart_json, filter_json):
    """Helper function to create CSV data for download."""
    if not n_clicks or not chart_json:
        raise dash.exceptions.PreventUpdate
    
    chart_data = json.loads(chart_json)
    df = pd.read_json(chart_data['df'], orient='split')
    title = chart_data['title'] or "chart"

    output = io.StringIO()
    
    if filter_json:
        filters = json.loads(filter_json)
        if filters:
            output.write('="Applied Filters:"\n')
            for key, value in filters.items():
                s_key = str(key).replace('"', '""')
                s_value = str(value).replace('"', '""')
                output.write(f'=" - {s_key}: {s_value}"\n')
            output.write('\n')

    s_title = str(title).replace('"', '""')
    output.write(f'="Chart Data: {s_title}"\n\n')
    df.to_csv(output, index=False)
    
    safe_title = "".join(c for c in title if c.isalnum() or c in (' ', '_')).rstrip()
    filename = f"{safe_title.replace(' ', '_').lower()}.csv"
    
    return dict(content=output.getvalue(), type="text/csv", filename=filename)

# --- Page 1 Export Callbacks ---
@app.callback(Output("download-p1-chart1", "data"), Input("export-p1-chart1", "n_clicks"), State("p1-chart1-store", "data"), State("p1-filter-store", "data"), prevent_initial_call=True)
def export_p1_chart1(n_clicks, chart_json, filter_json):
    return _create_export_data(n_clicks, chart_json, filter_json)

@app.callback(Output("download-p1-chart2", "data"), Input("export-p1-chart2", "n_clicks"), State("p1-chart2-store", "data"), State("p1-filter-store", "data"), prevent_initial_call=True)
def export_p1_chart2(n_clicks, chart_json, filter_json):
    return _create_export_data(n_clicks, chart_json, filter_json)

# --- Page 2 Export Callbacks ---
@app.callback(Output("download-p2-chart1", "data"), Input("export-p2-chart1", "n_clicks"), State("p2-chart1-store", "data"), State("p2-filter-store", "data"), prevent_initial_call=True)
def export_p2_chart1(n_clicks, chart_json, filter_json):
    return _create_export_data(n_clicks, chart_json, filter_json)

@app.callback(Output("download-p2-chart2", "data"), Input("export-p2-chart2", "n_clicks"), State("p2-chart2-store", "data"), State("p2-filter-store", "data"), prevent_initial_call=True)
def export_p2_chart2(n_clicks, chart_json, filter_json):
    return _create_export_data(n_clicks, chart_json, filter_json)

# --- Page 3 Export Callbacks ---
@app.callback(Output("download-p3-chart1", "data"), Input("export-p3-chart1", "n_clicks"), State("p3-chart1-store", "data"), State("p3-filter-store", "data"), prevent_initial_call=True)
def export_p3_chart1(n_clicks, chart_json, filter_json):
    return _create_export_data(n_clicks, chart_json, filter_json)

@app.callback(Output("download-p3-chart2", "data"), Input("export-p3-chart2", "n_clicks"), State("p3-chart2-store", "data"), State("p3-filter-store", "data"), prevent_initial_call=True)
def export_p3_chart2(n_clicks, chart_json, filter_json):
    return _create_export_data(n_clicks, chart_json, filter_json)

# --- Page 4 Export Callbacks ---
@app.callback(Output("download-p4-chart1", "data"), Input("export-p4-chart1", "n_clicks"), State("p4-chart1-store", "data"), State("p4-filter-store", "data"), prevent_initial_call=True)
def export_p4_chart1(n_clicks, chart_json, filter_json):
    return _create_export_data(n_clicks, chart_json, filter_json)

@app.callback(Output("download-p4-chart2", "data"), Input("export-p4-chart2", "n_clicks"), State("p4-chart2-store", "data"), State("p4-filter-store", "data"), prevent_initial_call=True)
def export_p4_chart2(n_clicks, chart_json, filter_json):
    return _create_export_data(n_clicks, chart_json, filter_json)

@app.callback(
    Output('region-measure-store', 'data'),
    [Input('region-orders-card', 'n_clicks'),
     Input('region-revenue-card', 'n_clicks'),
     Input('region-backlog-card', 'n_clicks'),
     Input('region-cash-card', 'n_clicks'),
     Input('region-pending-card', 'n_clicks')],
    prevent_initial_call=True
)
def update_region_selected_measure(orders_clicks, revenue_clicks, backlog_clicks, cash_clicks, pending_clicks):
    ctx = callback_context
    if not ctx.triggered:
        return dash.no_update
    button_id = ctx.triggered[0]['prop_id'].split('.')[0]
    if button_id == 'region-orders-card':
        return measure_cols["Order Amount"]
    elif button_id == 'region-revenue-card':
        return measure_cols["Revenue Amount"]
    elif button_id == 'region-backlog-card':
        return measure_cols["Backlog Amount"]
    elif button_id == 'region-cash-card':
        return measure_cols["Cash Amount"]
    elif button_id == 'region-pending-card':
        return measure_cols["Pending Amount"]
    return dash.no_update

@app.callback(
    [Output("region-orders-card-value", "children"),
     Output("region-revenue-card-value", "children"),
     Output("region-backlog-card-value", "children"),
     Output("region-cash-card-value", "children"),
     Output("region-pending-card-value", "children"),
     Output("region-po-count-value", "children"),
     Output("region-orders-card", "style"),
     Output("region-revenue-card", "style"),
     Output("region-backlog-card", "style"),
     Output("region-cash-card", "style"),
     Output("region-pending-card", "style"),
     Output("region-po-count-card", "style"),
     Output("region-charts", "children"),
     Output("region-summary-table", "children"),
     Output("p2-chart1-store", "data"),
     Output("p2-chart2-store", "data"),
     Output("p2-filter-store", "data")],
    [Input('region-dropdown1', 'value'),
     Input('region-dropdown2', 'value'),
     Input('region-dropdown3', 'value'),
     Input('region-dropdown4', 'value'),
     Input('specific-region-filter', 'value'),
     Input('region-year-filter', 'value'),
     Input('p2-period-filter', 'value'),
     Input('region-measure-store', 'data'),
     Input('data-refresh-interval', 'n_intervals'),
     Input('fast-data-refresh-interval', 'n_intervals')]
)
def update_region_analysis(d1, d2, d3, d4, specific_region, year_filter, p2_period, selected_measure, n_intervals, fast_n_intervals):
    if is_data_updated():
        orders, revenues, cash, merged, measure_cols = load_data()
        try:
            os.remove('data_updated.txt')
        except Exception:
            pass
    else:
        orders, revenues, cash, merged, measure_cols = load_data()
    try:
        filtered_df = merged.copy()
        filters = [d1, d2, d3, d4]
        for i, col in enumerate(dropdown_cols):
            if filters[i]:
                filtered_df = filtered_df[filtered_df[col] == filters[i]]
        if specific_region and specific_region != "All":
            filtered_df = filtered_df[filtered_df['Region'] == specific_region]
        if year_filter:
            filtered_df = filtered_df[filtered_df['Year'] == year_filter]
        # Resolve actual_measure and perform validation based on actual column names
        actual_measure = get_actual_column_name(selected_measure, measure_cols)
        if filtered_df.empty or actual_measure not in filtered_df.columns:
            default = "N/A"
            default_style = {'padding': '10px', 'textAlign': 'center', 'cursor': 'pointer', 'border': '1px solid #ddd', 'borderRadius': '5px', 'margin': '0 5px'}
            empty_table = dash_table.DataTable(columns=[], data=[])
            return [
                default, default, default, default, default, default,
                default_style, default_style, default_style, default_style, default_style, default_style,
                [], [html.Div([empty_table], style={'marginLeft': '2cm', 'marginRight': '2cm'})], None, None, None
            ]
        total_orders = filtered_df[measure_cols["Order Amount"]].sum()
        total_revenue = filtered_df[measure_cols["Revenue Amount"]].sum()
        total_backlog = filtered_df[measure_cols["Backlog Amount"]].sum()
        total_cash = filtered_df[measure_cols["Cash Amount"]].sum()
        total_pending = filtered_df[measure_cols["Pending Amount"]].sum()
        po_count = filtered_df['PO REF'].nunique()
        formatted_orders = f"€{total_orders:,.0f}"
        formatted_revenue = f"€{total_revenue:,.0f}"
        formatted_backlog = f"€{total_backlog:,.0f}"
        formatted_cash = f"€{total_cash:,.0f}"
        formatted_pending = f"€{total_pending:,.0f}"
        formatted_po_count = f"{po_count:,}"
        default_style = {'padding': '10px', 'textAlign': 'center', 'cursor': 'pointer', 'border': '1px solid #ddd', 'borderRadius': '5px', 'margin': '0 5px'}
        active_style = default_style.copy()
        active_style['border'] = '2px solid #007BFF'
        active_style['boxShadow'] = '0 0 5px #007BFF'
        orders_style = active_style if selected_measure == measure_cols["Order Amount"] else default_style
        revenue_style = active_style if selected_measure == measure_cols["Revenue Amount"] else default_style
        backlog_style = active_style if selected_measure == "Backlog Amount" else default_style
        cash_style = active_style if selected_measure == measure_cols["Cash Amount"] else default_style
        pending_style = active_style if selected_measure == "Pending Amount" else default_style
        po_count_style = default_style
        # Get the actual column name for the selected measure
        actual_measure = get_actual_column_name(selected_measure, measure_cols)
        
        bar_title = f"{selected_measure.replace('_', ' ').title()} by Region"
        year_comparison_title = f"{selected_measure.replace('_', ' ').title()} Year Comparison by Region"
        pie_df = filtered_df.groupby('Region', as_index=False)[actual_measure].sum()
        # Pick a valid dimension (one of dropdown_cols) that exists in the dataframe
        dimension = next((col for col in dropdown_cols if col in filtered_df.columns), None)
        if not dimension:
            dimension = 'Region' if 'Region' in filtered_df.columns else filtered_df.columns[0]

        try:
            bar_df = filtered_df.groupby(dimension, as_index=False)[actual_measure].sum().nlargest(10, actual_measure)
        except Exception as e:
            import traceback
            print("Failed to compute bar_df:")
            print(traceback.format_exc())
            bar_df = pd.DataFrame()
        
        # Create comprehensive year comparison data with Revenue, Orders, and Cash
        year_comparison_df = filtered_df.groupby(['Year', 'Region'], as_index=False).agg({
            measure_cols["Revenue Amount"]: "sum",
            measure_cols["Order Amount"]: "sum", 
            measure_cols["Cash Amount"]: "sum"
        }).rename(columns={
            measure_cols["Revenue Amount"]: "Revenue Amount",
            measure_cols["Order Amount"]: "Order Amount",
            measure_cols["Cash Amount"]: "Cash Amount"
        })
        
        try:
            if not is_valid_for_plot(bar_df, actual_measure):
                bar_fig = px.bar(title="No data available")
            else:
                clean_bar_df = bar_df.copy()
                clean_bar_df[actual_measure] = pd.to_numeric(clean_bar_df[actual_measure], errors='coerce')
                clean_bar_df = clean_bar_df.dropna(subset=[actual_measure])
                
                if clean_bar_df.empty:
                    bar_fig = px.bar(title="No valid data available")
                else:
                    bar_fig = px.bar(
                        clean_bar_df, 
                        x=dimension, 
                        y=actual_measure, 
                        title=f"{selected_measure} by {dimension}"
                    )
        except Exception as e:
            print(f"Error creating region bar chart: {e}")
            bar_fig = px.bar(title="Error loading chart")
        
        bar_fig.update_layout(title_font_size=12, title_x=0.5, margin=dict(t=40, b=20, l=20, r=20))
        
        # If Period selected, override comparison to be by Period instead of Year
        if p2_period:
            temp_df = filtered_df.copy()
            if p2_period == 'Quarterly':
                temp_df['Period'] = temp_df['Month'].dt.to_period('Q').astype(str)
            else:
                temp_df['Period'] = temp_df['Month'].dt.to_period('M').astype(str)
            year_comparison_df = temp_df.groupby(['Period', 'Region'], as_index=False).agg({
                measure_cols["Revenue Amount"]: "sum",
                measure_cols["Order Amount"]: "sum", 
                measure_cols["Cash Amount"]: "sum"
            }).rename(columns={
                measure_cols["Revenue Amount"]: "Revenue Amount",
                measure_cols["Order Amount"]: "Order Amount",
                measure_cols["Cash Amount"]: "Cash Amount"
            })

        # Create comprehensive year/period comparison bar chart with Revenue, Orders, and Cash
        try:
            candidate_cols = ['Revenue Amount', 'Order Amount', 'Cash Amount']
            valid_cols = [c for c in candidate_cols if c in year_comparison_df.columns and is_valid_for_plot(year_comparison_df, c)]
            if not valid_cols:
                year_comparison_fig = px.bar(title="No data available")
            else:
                clean_year_comparison_df = year_comparison_df.copy()
                for col in candidate_cols:
                    if col in clean_year_comparison_df.columns:
                        clean_year_comparison_df[col] = pd.to_numeric(clean_year_comparison_df[col], errors='coerce')
                clean_year_comparison_df = clean_year_comparison_df.dropna(subset=valid_cols, how='all')
                if clean_year_comparison_df.empty:
                    year_comparison_fig = px.bar(title="No valid data available")
                else:
                    id_vars = ['Region']
                    if p2_period:
                        id_vars = ['Period'] + id_vars
                    else:
                        id_vars = ['Year'] + id_vars
                    melted_df = clean_year_comparison_df.melt(id_vars=id_vars, value_vars=valid_cols, var_name='Measure', value_name='Amount')
                    x_axis = 'Period' if p2_period else 'Year'
                    title_txt = "Revenue, Orders & Cash Comparison by " + ("Period" if p2_period else "Year") + " and Region"
                    year_comparison_fig = px.bar(melted_df, x=x_axis, y='Amount', color='Region', pattern_shape='Measure', title=title_txt, barmode='group', color_discrete_sequence=px.colors.qualitative.Set3)
                    year_comparison_fig.update_layout(xaxis_title=("Period" if p2_period else "Year"), yaxis_title="Amount (€)", legend_title="Region & Measure", barmode='group')
        except Exception as e:
            print(f"Error creating region year comparison chart: {e}")
            year_comparison_fig = px.bar(title="Error loading chart")
        
        year_comparison_fig.update_layout(title_font_size=12, title_x=0.5, margin=dict(t=40, b=20, l=20, r=20))
        charts = html.Div([
            create_chart_card(bar_fig, "p2-chart1"), 
            create_chart_card(year_comparison_fig, "p2-chart2")
        ], style={'display': 'flex', 'justifyContent': 'center', 'gap': '50px'})
        summary_df = filtered_df.groupby(["Customer", "Region", "Project"], as_index=False).agg({
            measure_cols["Order Amount"]: "sum",
            measure_cols["Revenue Amount"]: "sum",
            "Backlog Amount": "sum",
            "Pending Amount": "sum",
            measure_cols["Cash Amount"]: "sum"
        }).rename(columns={
            measure_cols["Order Amount"]: "Order Amount",
            measure_cols["Revenue Amount"]: "Revenue Amount",
            measure_cols["Cash Amount"]: "Cash Amount",
            "Backlog Amount": "Backlog Amount",
            "Pending Amount": "Pending Amount"
        }).sort_values(by=selected_measure if selected_measure in ["Order Amount", "Revenue Amount", "Backlog Amount"] else "Revenue Amount", ascending=False)
        table_data = summary_df.to_dict("records")
        table = dash_table.DataTable(
            columns=[
                {"name": "Customer", "id": "Customer"},
                {"name": "Region", "id": "Region"},
                {"name": "Project", "id": "Project"},
                {"name": "Order Amount", "id": "Order Amount", "type": "numeric", "format": {"specifier": ",.0f"}},
                {"name": "Revenue Amount", "id": "Revenue Amount", "type": "numeric", "format": {"specifier": ",.0f"}},
                {"name": "Cash Amount", "id": "Cash Amount", "type": "numeric", "format": {"specifier": ",.0f"}},
                {"name": "Backlog Amount", "id": "Backlog Amount", "type": "numeric", "format": {"specifier": ",.0f"}},
                {"name": "Pending Amount", "id": "Pending Amount", "type": "numeric", "format": {"specifier": ",.0f"}},
            ],
            data=table_data,
            style_table={'overflowX': 'auto', 'borderRadius': '8px', 'border': '1px solid #1976d2'},
            style_cell={'textAlign': 'center', 'padding': '8px'},
            style_header={'fontWeight': 'bold', 'backgroundColor': '#1976d2', 'color': 'white', 'border': 'none'},
            style_data_conditional=[{'if': {'row_index': 'even'}, 'backgroundColor': '#e3f2fd'}],
            export_format='csv',
        )
        chart1_data = {'df': bar_df.to_json(orient='split'), 'title': bar_fig.layout.title.text}
        chart2_data = {'df': year_comparison_df.to_json(orient='split'), 'title': year_comparison_fig.layout.title.text}
        active_filters = {dropdown_cols[i]: filters[i] for i, f in enumerate(filters) if f}
        if specific_region and specific_region != "All":
            active_filters['Region'] = specific_region
        if year_filter:
            active_filters['Year'] = year_filter
        if p2_period:
            active_filters['Period'] = p2_period
        return formatted_orders, formatted_revenue, formatted_backlog, formatted_cash, formatted_pending, formatted_po_count, orders_style, revenue_style, backlog_style, cash_style, pending_style, po_count_style, charts, [html.Div([table], style={'marginLeft': '2cm', 'marginRight': '2cm'})], json.dumps(chart1_data), json.dumps(chart2_data), json.dumps(active_filters)
    except Exception as e:
        print(f"Exception in update_region_analysis: {e}")
        traceback.print_exc()
        default = "N/A"
        default_style = {'padding': '10px', 'textAlign': 'center', 'cursor': 'pointer', 'border': '1px solid #ddd', 'borderRadius': '5px', 'margin': '0 5px'}
        empty_table = dash_table.DataTable(columns=[], data=[])
        return [
            default, default, default, default, default, default,
            default_style, default_style, default_style, default_style, default_style, default_style,
            [], [html.Div([empty_table], style={'marginLeft': '2cm', 'marginRight': '2cm'})], None, None, None
        ]

@app.callback(
    Output('sm-measure-store', 'data'),
    [Input('sm-orders-card', 'n_clicks'),
     Input('sm-revenue-card', 'n_clicks'),
     Input('sm-cash-card', 'n_clicks'),
     Input('sm-backlog-card', 'n_clicks'),
     Input('sm-pending-card', 'n_clicks')],
    prevent_initial_call=True
)
def update_sm_selected_measure(orders_clicks, revenue_clicks, cash_clicks, backlog_clicks, pending_clicks):
    ctx = callback_context
    if not ctx.triggered:
        return dash.no_update
    button_id = ctx.triggered[0]['prop_id'].split('.')[0]
    if button_id == 'sm-orders-card':
        return measure_cols["Order Amount"]
    elif button_id == 'sm-revenue-card':
        return measure_cols["Revenue Amount"]
    elif button_id == 'sm-cash-card':
        return measure_cols["Cash Amount"]
    elif button_id == 'sm-backlog-card':
        return measure_cols["Backlog Amount"]
    elif button_id == 'sm-pending-card':
        return measure_cols["Pending Amount"]
    return dash.no_update

@app.callback(
    [Output("sm-orders-card-value", "children"),
     Output("sm-revenue-card-value", "children"),
     Output("sm-cash-card-value", "children"),
     Output("sm-backlog-card-value", "children"),
     Output("sm-pending-card-value", "children"),
     Output("sm-po-count-value", "children"),
     Output("sm-orders-card", "style"),
     Output("sm-revenue-card", "style"),
     Output("sm-cash-card", "style"),
     Output("sm-backlog-card", "style"),
     Output("sm-pending-card", "style"),
     Output("sm-po-count-card", "style"),
     Output("sm-charts", "children"),
     Output("sm-summary-table-page3", "children"),
     Output("p3-chart1-store", "data"),
     Output("p3-chart2-store", "data"),
     Output("p3-filter-store", "data")],
    [Input('sm-dropdown1', 'value'),
     Input('sm-dropdown2', 'value'),
     Input('sm-dropdown3', 'value'),
     Input('sm-dropdown4', 'value'),
     Input('sm-region-filter', 'value'),
     Input('sm-year-filter', 'value'),
     Input('p3-period-filter', 'value'),
     Input('sm-measure-store', 'data'),
     Input('data-refresh-interval', 'n_intervals'),
     Input('fast-data-refresh-interval', 'n_intervals')]
)
def update_sm_analysis(d1, d2, d3, d4, region, year_filter, p3_period, selected_measure, n_intervals, fast_n_intervals):
    if is_data_updated():
        orders, revenues, cash, merged, measure_cols = load_data()
        try:
            os.remove('data_updated.txt')
        except Exception:
            pass
    else:
        orders, revenues, cash, merged, measure_cols = load_data()
    try:
        filtered_df = merged.copy()
        filters = [d1, d2, d3, d4]
        for i, col in enumerate(dropdown_cols):
            if filters[i]:
                filtered_df = filtered_df[filtered_df[col] == filters[i]]
        if year_filter:
            filtered_df = filtered_df[filtered_df['Year'] == year_filter]
        if region and region != "All":
            filtered_df = filtered_df[filtered_df['Region'] == region]
        if filtered_df.empty or selected_measure not in filtered_df.columns:
            default = "N/A"
            default_style = {'padding': '10px', 'textAlign': 'center', 'cursor': 'pointer', 'border': '1px solid #ddd', 'borderRadius': '5px', 'margin': '0 5px'}
            empty_table = dash_table.DataTable(columns=[], data=[])
            return [
                default, default, default, default, default, default,
                default_style, default_style, default_style, default_style, default_style, default_style,
                [], [html.Div([empty_table], style={'marginLeft': '2cm', 'marginRight': '2cm'})], None, None, None
            ]
        total_orders = filtered_df[measure_cols["Order Amount"]].sum()
        total_revenue = filtered_df[measure_cols["Revenue Amount"]].sum()
        total_cash = filtered_df[measure_cols["Cash Amount"]].sum()
        total_backlog = filtered_df[measure_cols["Backlog Amount"]].sum()
        total_pending = filtered_df[measure_cols["Pending Amount"]].sum()
        po_count = filtered_df['PO REF'].nunique()
        formatted_orders = f"€{total_orders:,.0f}"
        formatted_revenue = f"€{total_revenue:,.0f}"
        formatted_cash = f"€{total_cash:,.0f}"
        formatted_backlog = f"€{total_backlog:,.0f}"
        formatted_pending = f"€{total_pending:,.0f}"
        formatted_po_count = f"{po_count:,}"
        default_style = {'padding': '10px', 'textAlign': 'center', 'cursor': 'pointer', 'border': '1px solid #ddd', 'borderRadius': '5px', 'margin': '0 5px'}
        active_style = default_style.copy()
        active_style['border'] = '2px solid #007BFF'
        active_style['boxShadow'] = '0 0 5px #007BFF'
        orders_style = active_style if selected_measure == measure_cols["Order Amount"] else default_style
        revenue_style = active_style if selected_measure == measure_cols["Revenue Amount"] else default_style
        cash_style = active_style if selected_measure == measure_cols["Cash Amount"] else default_style
        backlog_style = active_style if selected_measure == "Backlog Amount" else default_style
        pending_style = active_style if selected_measure == "Pending Amount" else default_style
        po_count_style = default_style
        bar_title = f"{selected_measure.replace('_', ' ').title()} by SM"
        pie_title = f"{selected_measure.replace('_', ' ').title()} Share by SM"
        # If Period selected, build period dataset for second chart
        if p3_period:
            temp = filtered_df.copy()
            if p3_period == 'Quarterly':
                temp['Period'] = temp['Month'].dt.to_period('Q').astype(str)
            else:
                temp['Period'] = temp['Month'].dt.to_period('M').astype(str)
            pie_df = temp.groupby('Period', as_index=False)[selected_measure].sum()
        else:
            pie_df = filtered_df.groupby('SM', as_index=False)[selected_measure].sum()
        # Get the actual column name for the selected measure
        actual_measure = get_actual_column_name(selected_measure, measure_cols)
        
        bar_df = filtered_df.groupby('SM', as_index=False)[actual_measure].sum().nlargest(10, actual_measure)
        
        try:
            if not is_valid_for_plot(bar_df, actual_measure):
                bar_fig = px.bar(title="No data available")
            else:
                clean_bar_df = bar_df.copy()
                clean_bar_df[actual_measure] = pd.to_numeric(clean_bar_df[actual_measure], errors='coerce')
                clean_bar_df = clean_bar_df.dropna(subset=[actual_measure])
                
                if clean_bar_df.empty:
                    bar_fig = px.bar(title="No valid data available")
                else:
                    bar_fig = px.bar(
                        clean_bar_df, 
                        x='SM', 
                        y=actual_measure, 
                        title=f"{selected_measure} by SM"
                    )
        except Exception as e:
            print(f"Error creating bar chart: {e}")
            bar_fig = px.bar(title="Error loading chart")
        
        bar_fig.update_layout(title_font_size=12, title_x=0.5, margin=dict(t=40, b=20, l=20, r=20))
       
        try:
            if not is_valid_for_plot(pie_df, selected_measure):
                pie_fig = px.pie(title="No data available")
            else:
                clean_pie_df = pie_df.copy()
                clean_pie_df[selected_measure] = pd.to_numeric(clean_pie_df[selected_measure], errors='coerce')
                clean_pie_df = clean_pie_df.dropna(subset=[selected_measure])
                
                if clean_pie_df.empty:
                    pie_fig = px.pie(title="No valid data available")
                else:
                    if p3_period:
                        pie_fig = px.bar(clean_pie_df, x='Period', y=selected_measure, title=f"{selected_measure} by {p3_period}")
                    else:
                        pie_fig = px.pie(clean_pie_df, names='SM', values=selected_measure, title=f"{selected_measure} Distribution by SM", hole=0.3)
        except Exception as e:
            print(f"Error creating pie chart: {e}")
            pie_fig = px.pie(title="Error loading chart")
        
        pie_fig.update_layout(title_font_size=12, title_x=0.5, margin=dict(t=40, b=20, l=20, r=20))
        charts = html.Div([
            create_chart_card(bar_fig, "p3-chart1"), 
            create_chart_card(pie_fig, "p3-chart2")
        ], style={'display': 'flex', 'justifyContent': 'center', 'gap': '50px'})
        summary_df = filtered_df.groupby(["SM", "Customer", "Project"], as_index=False).agg({
            measure_cols["Order Amount"]: "sum",
            measure_cols["Revenue Amount"]: "sum",
            measure_cols["Cash Amount"]: "sum",
            "Backlog Amount": "sum",
            "Pending Amount": "sum"
        }).rename(columns={
            measure_cols["Order Amount"]: "Order Amount",
            measure_cols["Revenue Amount"]: "Revenue Amount",
            measure_cols["Cash Amount"]: "Cash Amount",
            "Backlog Amount": "Backlog Amount",
            "Pending Amount": "Pending Amount"
        }).sort_values(by=selected_measure if selected_measure in ["Order Amount", "Revenue Amount", "Cash Amount"] else "Order Amount", ascending=False)
        table_data = summary_df.to_dict("records")
        table = dash_table.DataTable(
            columns=[
                {"name": "SM", "id": "SM"},
                {"name": "Customer", "id": "Customer"},
                {"name": "Project", "id": "Project"},
                {"name": "Order Amount", "id": "Order Amount", "type": "numeric", "format": {"specifier": ",.0f"}},
                {"name": "Revenue Amount", "id": "Revenue Amount", "type": "numeric", "format": {"specifier": ",.0f"}},
                {"name": "Cash Amount", "id": "Cash Amount", "type": "numeric", "format": {"specifier": ",.0f"}},
                {"name": "Backlog Amount", "id": "Backlog Amount", "type": "numeric", "format": {"specifier": ",.0f"}},
                {"name": "Pending Amount", "id": "Pending Amount", "type": "numeric", "format": {"specifier": ",.0f"}},
            ],
            data=table_data,
            style_table={'overflowX': 'auto', 'borderRadius': '8px', 'border': '1px solid #1976d2'},
            style_cell={'textAlign': 'center', 'padding': '8px'},
            style_header={'fontWeight': 'bold', 'backgroundColor': '#1976d2', 'color': 'white', 'border': 'none'},
            style_data_conditional=[{'if': {'row_index': 'even'}, 'backgroundColor': '#e3f2fd'}],
            export_format='csv',
        )
        chart1_data = {'df': bar_df.to_json(orient='split'), 'title': bar_fig.layout.title.text}
        chart2_data = {'df': pie_df.to_json(orient='split'), 'title': pie_fig.layout.title.text}
        active_filters = {dropdown_cols[i]: filters[i] for i, f in enumerate(filters) if f}
        if region and region != "All":
            active_filters['Region'] = region
        if year_filter:
            active_filters['Year'] = year_filter
        if p3_period:
            active_filters['Period'] = p3_period
        return formatted_orders, formatted_revenue, formatted_cash, formatted_backlog, formatted_pending, formatted_po_count, orders_style, revenue_style, cash_style, backlog_style, pending_style, po_count_style, charts, [html.Div([table], style={'marginLeft': '2cm', 'marginRight': '2cm'})], json.dumps(chart1_data), json.dumps(chart2_data), json.dumps(active_filters)
    except Exception as e:
        print(f"Exception in update_sm_analysis: {e}")
        traceback.print_exc()
        default = "N/A"
        default_style = {'padding': '10px', 'textAlign': 'center', 'cursor': 'pointer', 'border': '1px solid #ddd', 'borderRadius': '5px', 'margin': '0 5px'}
        empty_table = dash_table.DataTable(columns=[], data=[])
        return [
            default, default, default, default, default, default,
            default_style, default_style, default_style, default_style, default_style, default_style,
            [], [html.Div([empty_table], style={'marginLeft': '2cm', 'marginRight': '2cm'})], None, None, None
        ]

@app.callback(
    Output('year-measure-store', 'data'),
    [Input('year-orders-card', 'n_clicks'),
     Input('year-revenue-card', 'n_clicks'),
     Input('year-cash-card', 'n_clicks'),
     Input('year-backlog-card', 'n_clicks'),
     Input('year-pending-card', 'n_clicks')],
    prevent_initial_call=True
)
def update_year_selected_measure(orders_clicks, revenue_clicks, cash_clicks, backlog_clicks, pending_clicks):
    ctx = callback_context
    if not ctx.triggered:
        return dash.no_update
    button_id = ctx.triggered[0]['prop_id'].split('.')[0]

    if button_id == 'year-orders-card':
        return measure_cols["Order Amount"]
    elif button_id == 'year-revenue-card':
        return measure_cols["Revenue Amount"]
    elif button_id == 'year-cash-card':
        return measure_cols["Cash Amount"]
    elif button_id == 'year-backlog-card':
        return measure_cols["Backlog Amount"]
    elif button_id == 'year-pending-card':
        return measure_cols["Pending Amount"]
    return dash.no_update

@app.callback(
    [Output("year-orders-card-value", "children"),
     Output("year-revenue-card-value", "children"),
     Output("year-cash-card-value", "children"),
     Output("year-backlog-card-value", "children"),
     Output("year-pending-card-value", "children"),
     Output("year-po-count-value", "children"),
     Output("year-orders-card", "style"),
     Output("year-revenue-card", "style"),
     Output("year-cash-card", "style"),
     Output("year-backlog-card", "style"),
     Output("year-pending-card", "style"),
     Output("year-po-count-card", "style"),
     Output("year-charts", "children"),
     Output("year-summary-table", "children"),
     Output("p4-chart1-store", "data"),
     Output("p4-chart2-store", "data"),
     Output("p4-filter-store", "data")],
    [Input('year-dropdown1', 'value'),
     Input('year-dropdown2', 'value'),
     Input('year-dropdown3', 'value'),
     Input('year-dropdown4', 'value'),
     Input('year-region-filter', 'value'),
     Input('p4-year-filter', 'value'),
     Input('p4-period-filter', 'value'),
     Input('year-measure-store', 'data'),
     Input('data-refresh-interval', 'n_intervals'),
     Input('fast-data-refresh-interval', 'n_intervals')]
)
def update_year_analysis(d1, d2, d3, d4, region_filter, year_filter, p4_period, selected_measure, n_intervals, fast_n_intervals):
    if is_data_updated():
        orders, revenues, cash, merged, measure_cols = load_data()
        try:
            os.remove('data_updated.txt')
        except Exception:
            pass
    else:
        orders, revenues, cash, merged, measure_cols = load_data()
    try:
        filtered_df = merged.copy()
        filters = [d1, d2, d3, d4]
        for i, col in enumerate(dropdown_cols):
            if filters[i]:
                filtered_df = filtered_df[filtered_df[col] == filters[i]]
        if region_filter and region_filter != "All":
            filtered_df = filtered_df[filtered_df['Region'] == region_filter]
        if year_filter:
            filtered_df = filtered_df[filtered_df['Year'] == year_filter]
        
        # Get the actual column name for the selected measure
        actual_measure = get_actual_column_name(selected_measure, measure_cols)
        
        if filtered_df.empty or actual_measure not in filtered_df.columns:
            default = "N/A"
            default_style = {'padding': '10px', 'textAlign': 'center', 'cursor': 'pointer', 'border': '1px solid #ddd', 'borderRadius': '5px', 'margin': '0 5px'}
            empty_table = dash_table.DataTable(columns=[], data=[])
            return [
                default, default, default, default, default, default,
                default_style, default_style, default_style, default_style, default_style, default_style,
                [], [html.Div([empty_table], style={'marginLeft': '2cm', 'marginRight': '2cm'})], None, None, None
            ]
        total_revenue = filtered_df[measure_cols["Revenue Amount"]].sum()
        total_orders = filtered_df[measure_cols["Order Amount"]].sum()
        total_cash = filtered_df[measure_cols["Cash Amount"]].sum()
        total_backlog = filtered_df[measure_cols["Backlog Amount"]].sum()
        total_pending = filtered_df[measure_cols["Pending Amount"]].sum()
        total_pos = filtered_df['PO REF'].nunique()
        formatted_revenue = f"€{total_revenue:,.0f}"
        formatted_orders = f"€{total_orders:,.0f}"
        formatted_cash = f"€{total_cash:,.0f}"
        formatted_backlog = f"€{total_backlog:,.0f}"
        formatted_pending = f"€{total_pending:,.0f}"
        formatted_pos = f"{total_pos:,}"
        default_style = {'padding': '10px', 'textAlign': 'center', 'cursor': 'pointer', 'border': '1px solid #ddd', 'borderRadius': '5px', 'margin': '0 5px'}
        active_style = default_style.copy()
        active_style['border'] = '2px solid #007BFF'
        active_style['boxShadow'] = '0 0 5px #007BFF'
        revenue_style = active_style if selected_measure == measure_cols["Revenue Amount"] else default_style
        orders_style = active_style if selected_measure == measure_cols["Order Amount"] else default_style
        cash_style = active_style if selected_measure == measure_cols["Cash Amount"] else default_style
        backlog_style = active_style if selected_measure == "Backlog Amount" else default_style
        pending_style = active_style if selected_measure == "Pending Amount" else default_style
        po_count_style = default_style
        if selected_measure in measure_cols.values():
            measure_name = [k for k, v in measure_cols.items() if v == selected_measure][0]
        # If Period selected, create period-based aggregations; otherwise keep Year-based
        if p4_period:
            temp = filtered_df.copy()
            if p4_period == 'Quarterly':
                temp['Period'] = temp['Month'].dt.to_period('Q').astype(str)
            else:
                temp['Period'] = temp['Month'].dt.to_period('M').astype(str)
            year_trend_df = temp.groupby(['Period', 'Region'], as_index=False)[actual_measure].sum()
            year_comparison_df = temp.groupby('Period', as_index=False).agg({
                measure_cols["Revenue Amount"]: "sum",
                measure_cols["Order Amount"]: "sum",
                measure_cols["Cash Amount"]: "sum"
            })
        else:
            year_trend_df = filtered_df.groupby(['Year', 'Region'], as_index=False)[actual_measure].sum()
            year_comparison_df = filtered_df.groupby('Year', as_index=False).agg({
                measure_cols["Revenue Amount"]: "sum",
                measure_cols["Order Amount"]: "sum",
                measure_cols["Cash Amount"]: "sum"
            })
        rename_dict = {v: k for k, v in measure_cols.items() if k in ["Revenue Amount", "Order Amount", "Cash Amount"]}
        year_comparison_df = year_comparison_df.rename(columns=rename_dict)
        try:
            if not is_valid_for_plot(year_trend_df, actual_measure):
                fig_trend = px.line(title="No data available")
            else:
                clean_trend_df = year_trend_df.copy()
                clean_trend_df[actual_measure] = pd.to_numeric(clean_trend_df[actual_measure], errors='coerce')
                clean_trend_df = clean_trend_df.dropna(subset=[actual_measure])
                
                if clean_trend_df.empty:
                    fig_trend = px.line(title="No valid data available")
                else:
                    x_col = 'Period' if p4_period else 'Year'
                    title_txt = ("Period" if p4_period else "Yearly") + f" Trend of {selected_measure}"
                    fig_trend = px.line(clean_trend_df, x=x_col, y=actual_measure, color='Region', title=title_txt, markers=True)
        except Exception as e:
            print(f"Error creating trend chart: {e}")
            fig_trend = px.line(title="Error loading chart")
        
        fig_trend.update_layout(title_font_size=12, title_x=0.5, margin=dict(t=40, b=20, l=20, r=20))
        
        try:
            valid_cols = [col for col in ["Revenue Amount", "Order Amount", "Cash Amount"] if is_valid_for_plot(year_comparison_df, col)]
            if year_comparison_df.empty or not valid_cols:
                fig_compare = px.bar(title="No data available")
            else:
                clean_compare_df = year_comparison_df.copy()
                for col in valid_cols:
                    clean_compare_df[col] = pd.to_numeric(clean_compare_df[col], errors='coerce')
                clean_compare_df = clean_compare_df.dropna(subset=valid_cols, how='all')
                
                if clean_compare_df.empty:
                    fig_compare = px.bar(title="No valid data available")
                else:
                    x_col = 'Period' if p4_period else 'Year'
                    title_txt = ("Period" if p4_period else "Year-wise") + " Comparison (Revenue, Order, Cash)"
                    fig_compare = px.bar(clean_compare_df, x=x_col, y=valid_cols, title=title_txt, barmode='group')
        except Exception as e:
            print(f"Error creating comparison chart: {e}")
            fig_compare = px.bar(title="Error loading chart")
        
        fig_compare.update_layout(title_font_size=12, title_x=0.5, margin=dict(t=40, b=20, l=20, r=20), legend_title_text='Measure')
        charts = html.Div([
            create_chart_card(fig_trend, "p4-chart1"), 
            create_chart_card(fig_compare, "p4-chart2")
        ], style={'display': 'flex', 'justifyContent': 'center', 'gap': '50px'})
        summary_df = filtered_df.groupby(['Year', 'Region', 'SM'], as_index=False).agg({
            measure_cols["Revenue Amount"]: "sum",
            measure_cols["Order Amount"]: "sum",
            measure_cols["Cash Amount"]: "sum",
            measure_cols["Backlog Amount"]: "sum",
            measure_cols["Pending Amount"]: "sum",
            'PO REF': 'nunique'
        }).rename(columns={
            measure_cols["Revenue Amount"]: "Revenue Amount",
            measure_cols["Order Amount"]: "Order Amount",
            measure_cols["Cash Amount"]: "Cash Amount",
            measure_cols["Backlog Amount"]: "Backlog Amount",
            measure_cols["Pending Amount"]: "Pending Amount",
            'PO REF': 'PO Count'
        }).sort_values(['Year', 'Revenue Amount'], ascending=[True, False])
        table_data = summary_df.to_dict("records")
        table = dash_table.DataTable(
            columns=[
                {"name": "Year", "id": "Year"},
                {"name": "Region", "id": "Region"},
                {"name": "SM", "id": "SM"},
                {"name": "Revenue Amount", "id": "Revenue Amount", "type": "numeric", "format": {"specifier": ",.0f"}},
                {"name": "Order Amount", "id": "Order Amount", "type": "numeric", "format": {"specifier": ",.0f"}},
                {"name": "Cash Amount", "id": "Cash Amount", "type": "numeric", "format": {"specifier": ",.0f"}},
                {"name": "Backlog Amount", "id": "Backlog Amount", "type": "numeric", "format": {"specifier": ",.0f"}},
                {"name": "Pending Amount", "id": "Pending Amount", "type": "numeric", "format": {"specifier": ",.0f"}},
                {"name": "PO Count", "id": "PO Count", "type": "numeric", "format": {"specifier": ",.0f"}},
            ],
            data=table_data,
            style_table={'overflowX': 'auto', 'borderRadius': '8px', 'border': '1px solid #1976d2'},
            style_cell={'textAlign': 'center', 'padding': '8px'},
            style_header={'fontWeight': 'bold', 'backgroundColor': '#1976d2', 'color': 'white', 'border': 'none'},
            style_data_conditional=[{'if': {'row_index': 'even'}, 'backgroundColor': '#e3f2fd'}],
            export_format='csv',
        )
        chart1_data = {'df': year_trend_df.to_json(orient='split'), 'title': fig_trend.layout.title.text}
        chart2_data = {'df': year_comparison_df.to_json(orient='split'), 'title': fig_compare.layout.title.text}
        active_filters = {dropdown_cols[i]: filters[i] for i, f in enumerate(filters) if f}
        if region_filter and region_filter != "All":
            active_filters['Region'] = region_filter
        if year_filter:
            active_filters['Year'] = year_filter
        if p4_period:
            active_filters['Period'] = p4_period
        return formatted_orders, formatted_revenue, formatted_cash, formatted_backlog, formatted_pending, formatted_pos, orders_style, revenue_style, cash_style, backlog_style, pending_style, po_count_style, charts, [html.Div([table], style={'marginLeft': '2cm', 'marginRight': '2cm'})], json.dumps(chart1_data), json.dumps(chart2_data), json.dumps(active_filters)
    except Exception as e:
        print(f"Exception in update_year_analysis: {e}")
        traceback.print_exc()
        default = "N/A"
        default_style = {'padding': '10px', 'textAlign': 'center', 'cursor': 'pointer', 'border': '1px solid #ddd', 'borderRadius': '5px', 'margin': '0 5px'}
        empty_table = dash_table.DataTable(columns=[], data=[])
        return [
            default, default, default, default, default, default,
            default_style, default_style, default_style, default_style, default_style, default_style,
            [], [html.Div([empty_table], style={'marginLeft': '2cm', 'marginRight': '2cm'})], None, None, None
        ]

# This implementation avoids circular dependencies by separating the "update" and "set" logic.
# 1. A master callback updates the store when any dropdown changes.
# 2. Page-specific callbacks use the store's state to set values only when navigating to that page.
dropdown_key_mapping = {
    # Page 1 (Region Analysis)
    'region-dropdown1': 'customer', 'region-dropdown2': 'project', 'region-dropdown3': 'sm', 'region-dropdown4': 'po_ref',
    'specific-region-filter': 'region', 'region-year-filter': 'year',
    # Page 2 (SM Analysis)
    'sm-dropdown1': 'customer', 'sm-dropdown2': 'project', 'sm-dropdown3': 'sm', 'sm-dropdown4': 'po_ref',
    'sm-region-filter': 'region', 'sm-year-filter': 'year',
    # Page 3 (Year-wise Analysis)
    'year-dropdown1': 'customer', 'year-dropdown2': 'project', 'year-dropdown3': 'sm', 'year-dropdown4': 'po_ref',
    'year-region-filter': 'region', 'p4-year-filter': 'year',
    # Page 4 (PO Analysis)
    'dropdown1': 'customer', 'dropdown2': 'project', 'dropdown3': 'sm', 'dropdown4': 'po_ref',
    'p1-region-filter': 'region', 'year-filter1': 'year',
    # Period filters (shared across pages)
    'p1-period-filter': 'period', 'p2-period-filter': 'period', 'p3-period-filter': 'period', 'p4-period-filter': 'period',
} 
all_dropdown_ids = list(dropdown_key_mapping.keys())

# 1. Master callback to update the store from ANY dropdown change
@app.callback(
    Output('shared-dropdowns', 'data', allow_duplicate=True),
    [Input(dropdown_id, 'value') for dropdown_id in all_dropdown_ids],
    State('shared-dropdowns', 'data'),
    prevent_initial_call=True
)
def update_shared_store(*args):
    ctx = callback_context
    if not ctx.triggered:
        return dash.no_update

    triggered_id = ctx.triggered[0]['prop_id'].split('.')[0]
    triggered_value = ctx.triggered[0]['value']
    
    store_data = args[-1] or {}

    filter_key = dropdown_key_mapping.get(triggered_id)
    if filter_key:
        store_data[filter_key] = triggered_value

    return store_data



@app.callback(
    [
        Output('region-dropdown1', 'value', allow_duplicate=True), 
        Output('region-dropdown2', 'value', allow_duplicate=True),
        Output('region-dropdown3', 'value', allow_duplicate=True), 
        Output('region-dropdown4', 'value', allow_duplicate=True),
        Output('specific-region-filter', 'value', allow_duplicate=True),
        Output('region-year-filter', 'value', allow_duplicate=True),
        Output('p2-period-filter', 'value', allow_duplicate=True)
    ],
    [Input('url', 'pathname'), Input('shared-dropdowns', 'data')],
    prevent_initial_call=True
)
def set_page1_dropdowns(pathname, store_data):
    # Apply stored values to Region Analysis dropdowns whenever the store updates
    if not store_data:
        return [dash.no_update] * 7

    # Load current data options and only set values if they are valid (avoid Invalid value errors)
    try:
        _, _, _, merged, _ = load_data()
    except Exception:
        merged = pd.DataFrame()

    def valid_value(col, val):
        # Allow explicit clears (None) to propagate to other pages
        if val is None:
            return None
        if col not in merged.columns:
            return dash.no_update
        opts = set(merged[col].dropna().unique())
        return val if val in opts else dash.no_update

    customer_val = valid_value('Customer', store_data.get('customer'))
    project_val = valid_value('Project', store_data.get('project'))
    sm_val = valid_value('SM', store_data.get('sm'))
    po_val = valid_value('PO REF', store_data.get('po_ref'))
    region_val = valid_value('Region', store_data.get('region'))

    # Year (integer)
    year_raw = store_data.get('year')
    try:
        if year_raw is None:
            year_val = None
        else:
            years = set(merged['Year'].dropna().unique()) if 'Year' in merged.columns else set()
            year_val = year_raw if (year_raw in years) else dash.no_update
    except Exception:
        year_val = None

    # Period
    period_raw = store_data.get('period')
    period_val = None if period_raw is None else (period_raw if period_raw in ("Monthly", "Quarterly") else dash.no_update)

    return [customer_val, project_val, sm_val, po_val, region_val, year_val, period_val]

@app.callback(
    [
        Output('sm-dropdown1', 'value', allow_duplicate=True), 
        Output('sm-dropdown2', 'value', allow_duplicate=True),
        Output('sm-dropdown3', 'value', allow_duplicate=True), 
        Output('sm-dropdown4', 'value', allow_duplicate=True),
        Output('sm-region-filter', 'value', allow_duplicate=True),
        Output('sm-year-filter', 'value', allow_duplicate=True),
        Output('p3-period-filter', 'value', allow_duplicate=True)
    ],
    [Input('url', 'pathname'), Input('shared-dropdowns', 'data')],
    prevent_initial_call=True
)
def set_page2_dropdowns(pathname, store_data):
    # Apply stored values to SM Analysis dropdowns whenever the store updates
    if not store_data:
        return [dash.no_update] * 7

    try:
        _, _, _, merged, _ = load_data()
    except Exception:
        merged = pd.DataFrame()

    def valid_value(col, val):
        # Allow explicit clears (None) to propagate to other pages
        if val is None:
            return None
        if col not in merged.columns:
            return dash.no_update
        opts = set(merged[col].dropna().unique())
        return val if val in opts else dash.no_update

    customer_val = valid_value('Customer', store_data.get('customer'))
    project_val = valid_value('Project', store_data.get('project'))
    sm_val = valid_value('SM', store_data.get('sm'))
    po_val = valid_value('PO REF', store_data.get('po_ref'))
    region_val = valid_value('Region', store_data.get('region'))

    year_raw = store_data.get('year')
    try:
        if year_raw is None:
            year_val = None
        else:
            years = set(merged['Year'].dropna().unique()) if 'Year' in merged.columns else set()
            year_val = year_raw if (year_raw in years) else dash.no_update
    except Exception:
        year_val = None

    period_raw = store_data.get('period')
    period_val = None if period_raw is None else (period_raw if period_raw in ("Monthly", "Quarterly") else dash.no_update)

    return [customer_val, project_val, sm_val, po_val, region_val, year_val, period_val]

@app.callback(
    [
        Output('year-dropdown1', 'value', allow_duplicate=True), 
        Output('year-dropdown2', 'value', allow_duplicate=True),
        Output('year-dropdown3', 'value', allow_duplicate=True), 
        Output('year-dropdown4', 'value', allow_duplicate=True),
        Output('year-region-filter', 'value', allow_duplicate=True),
        Output('p4-year-filter', 'value', allow_duplicate=True),
        Output('p4-period-filter', 'value', allow_duplicate=True)
    ],
    [Input('url', 'pathname'), Input('shared-dropdowns', 'data')],
    prevent_initial_call=True
)
def set_page3_dropdowns(pathname, store_data):
    # Apply stored values to Year-wise Analysis dropdowns whenever the store updates
    if not store_data:
        return [dash.no_update] * 7

    try:
        _, _, _, merged, _ = load_data()
    except Exception:
        merged = pd.DataFrame()

    def valid_value(col, val):
        if val is None:
            return dash.no_update
        if col not in merged.columns:
            return dash.no_update
        opts = set(merged[col].dropna().unique())
        return val if val in opts else dash.no_update

    customer_val = valid_value('Customer', store_data.get('customer'))
    project_val = valid_value('Project', store_data.get('project'))
    sm_val = valid_value('SM', store_data.get('sm'))
    po_val = valid_value('PO REF', store_data.get('po_ref'))
    region_val = valid_value('Region', store_data.get('region'))

    year_raw = store_data.get('year')
    try:
        years = set(merged['Year'].dropna().unique()) if 'Year' in merged.columns else set()
        year_val = year_raw if (year_raw in years) else dash.no_update
    except Exception:
        year_val = dash.no_update

    period_raw = store_data.get('period')
    period_val = period_raw if period_raw in ("Monthly", "Quarterly") else dash.no_update

    return [customer_val, project_val, sm_val, po_val, region_val, year_val, period_val]

@app.callback(
    [
        Output('dropdown1', 'value', allow_duplicate=True), 
        Output('dropdown2', 'value', allow_duplicate=True),
        Output('dropdown3', 'value', allow_duplicate=True), 
        Output('dropdown4', 'value', allow_duplicate=True),
        Output('p1-region-filter', 'value', allow_duplicate=True),
        Output('year-filter1', 'value', allow_duplicate=True),
        Output('p1-period-filter', 'value', allow_duplicate=True)
    ],
    [Input('url', 'pathname'), Input('shared-dropdowns', 'data')],
    prevent_initial_call=True
)
def set_page4_dropdowns(pathname, store_data):
    # Apply stored values to PO Analysis dropdowns whenever the store updates
    if not store_data:
        return [dash.no_update] * 7

    try:
        _, _, _, merged, _ = load_data()
    except Exception:
        merged = pd.DataFrame()

    def valid_value(col, val):
        if val is None:
            return dash.no_update
        if col not in merged.columns:
            return dash.no_update
        opts = set(merged[col].dropna().unique())
        return val if val in opts else dash.no_update

    customer_val = valid_value('Customer', store_data.get('customer'))
    project_val = valid_value('Project', store_data.get('project'))
    sm_val = valid_value('SM', store_data.get('sm'))
    po_val = valid_value('PO REF', store_data.get('po_ref'))
    region_val = valid_value('Region', store_data.get('region'))

    year_raw = store_data.get('year')
    try:
        years = set(merged['Year'].dropna().unique()) if 'Year' in merged.columns else set()
        year_val = year_raw if (year_raw in years) else dash.no_update
    except Exception:
        year_val = dash.no_update

    period_raw = store_data.get('period')
    period_val = period_raw if period_raw in ("Monthly", "Quarterly") else dash.no_update

    return [customer_val, project_val, sm_val, po_val, region_val, year_val, period_val]


# Keep all shared dropdown options in sync and ensure stored selections are included in options
@app.callback(
    [
        # customer/project/sm/po options for each page (repeated)
        Output('dropdown1', 'options'), Output('dropdown2', 'options'), Output('dropdown3', 'options'), Output('dropdown4', 'options'),
        Output('region-dropdown1', 'options'), Output('region-dropdown2', 'options'), Output('region-dropdown3', 'options'), Output('region-dropdown4', 'options'),
        Output('sm-dropdown1', 'options'), Output('sm-dropdown2', 'options'), Output('sm-dropdown3', 'options'), Output('sm-dropdown4', 'options'),
        Output('year-dropdown1', 'options'), Output('year-dropdown2', 'options'), Output('year-dropdown3', 'options'), Output('year-dropdown4', 'options'),
        # region filters
        Output('p1-region-filter', 'options'), Output('specific-region-filter', 'options'), Output('sm-region-filter', 'options'), Output('year-region-filter', 'options'),
        # year filters
        Output('year-filter1', 'options'), Output('region-year-filter', 'options'), Output('sm-year-filter', 'options'), Output('p4-year-filter', 'options')
    ],
    [Input('data-refresh-interval', 'n_intervals'), Input('fast-data-refresh-interval', 'n_intervals'), Input('shared-dropdowns', 'data')]
)
def update_all_shared_options(n_intervals, fast_n_intervals, store_data):
    try:
        _, _, _, merged, _ = load_data()
    except Exception:
        merged = pd.DataFrame()

    store = store_data or {}

    def make_opts(col):
        if col not in merged.columns:
            return []
        vals = sorted(merged[col].dropna().unique())
        return [{"label": str(v), "value": v} for v in vals]

    def ensure(opts, val):
        if val is None:
            return opts
        try:
            if any(o['value'] == val for o in opts):
                return opts
        except Exception:
            pass
        return [{"label": str(val), "value": val}] + opts

    customer_opts = make_opts('Customer')
    project_opts = make_opts('Project')
    sm_opts = make_opts('SM')
    po_opts = make_opts('PO REF')
    region_vals = make_opts('Region')
    region_opts = [{"label": "All Regions", "value": "All"}] + region_vals if region_vals else [{"label": "All Regions", "value": "All"}]
    year_vals = []
    if 'Year' in merged.columns:
        years = sorted([y for y in merged['Year'].dropna().unique() if y and y > 0])
        year_vals = [{"label": str(int(y)), "value": int(y)} for y in years]

    # Ensure any stored selections are present
    customer_opts = ensure(customer_opts, store.get('customer'))
    project_opts = ensure(project_opts, store.get('project'))
    sm_opts = ensure(sm_opts, store.get('sm'))
    po_opts = ensure(po_opts, store.get('po_ref'))
    region_opts = ensure(region_opts, store.get('region'))
    year_opts = ensure(year_vals, store.get('year'))

    # Return options in the order of Outputs
    return (
        customer_opts, project_opts, sm_opts, po_opts,
        customer_opts, project_opts, sm_opts, po_opts,
        customer_opts, project_opts, sm_opts, po_opts,
        customer_opts, project_opts, sm_opts, po_opts,
        region_opts, region_opts, region_opts, region_opts,
        year_opts, year_opts, year_opts, year_opts
    )


# Clean the shared store if any stored selections are no longer valid according to current options
@app.callback(
    Output('shared-dropdowns', 'data', allow_duplicate=True),
    [Input('dropdown1', 'options'), Input('dropdown2', 'options'), Input('dropdown3', 'options'), Input('dropdown4', 'options'),
     Input('p1-region-filter', 'options'), Input('year-filter1', 'options')],
    State('shared-dropdowns', 'data'),
    prevent_initial_call=True
)
def clean_shared_store(d1_opts, d2_opts, d3_opts, d4_opts, region_opts, year_opts, store_data):
    if not store_data:
        return dash.no_update

    def opt_values(opts):
        try:
            return set(o['value'] for o in (opts or []))
        except Exception:
            return set()

    cust_vals = opt_values(d1_opts)
    proj_vals = opt_values(d2_opts)
    sm_vals = opt_values(d3_opts)
    po_vals = opt_values(d4_opts)
    region_vals = opt_values(region_opts)
    year_vals = opt_values(year_opts)

    updated = False
    new_store = dict(store_data)

    # If stored value is not present in options, clear it (set None)
    if 'customer' in new_store and new_store.get('customer') not in cust_vals and new_store.get('customer') is not None:
        new_store['customer'] = None
        updated = True
    if 'project' in new_store and new_store.get('project') not in proj_vals and new_store.get('project') is not None:
        new_store['project'] = None
        updated = True
    if 'sm' in new_store and new_store.get('sm') not in sm_vals and new_store.get('sm') is not None:
        new_store['sm'] = None
        updated = True
    if 'po_ref' in new_store and new_store.get('po_ref') not in po_vals and new_store.get('po_ref') is not None:
        new_store['po_ref'] = None
        updated = True
    if 'region' in new_store and new_store.get('region') not in region_vals and new_store.get('region') is not None:
        # If region options include 'All' but stored value is None or not present, just clear
        new_store['region'] = None
        updated = True
    if 'year' in new_store and new_store.get('year') not in year_vals and new_store.get('year') is not None:
        new_store['year'] = None
        updated = True

    if updated:
        return new_store
    return dash.no_update


COLOR_MAP = {
    'Revenue Amount': '#1976d2',   
    'Order Amount': '#388e3c',     
    'Cash Amount': '#f9a825',     
    'Backlog Amount': '#d81b60',   
    'Pending Amount': '#7b1fa2',  
}
COLOR_SEQ_MULTI = [
    COLOR_MAP['Revenue Amount'],
    COLOR_MAP['Order Amount'],
    COLOR_MAP['Cash Amount'],
    COLOR_MAP['Backlog Amount'],
    COLOR_MAP['Pending Amount'],
]

def is_data_updated(flag_file="data_updated.txt"):
    return os.path.exists(flag_file)

def is_valid_for_plot(df, col):
    try:
        if df.empty or col not in df.columns:
            return False
       
        if not pd.api.types.is_numeric_dtype(df[col]):
            return False
        
        valid_data = df[col].dropna()
        if valid_data.size == 0:
            return False
        
        if not np.all(np.isfinite(valid_data)):
            return False
        
        return True
    except Exception:
        return False

def get_actual_column_name(selected_measure, measure_cols):
    """Get the actual column name from measure_cols dictionary"""
    if selected_measure in measure_cols.values():
        return selected_measure
    elif selected_measure in measure_cols:
        return measure_cols[selected_measure]
    else:
        # Fallback to the original value
        return selected_measure

# ---------- Main Dashboard (Page 5) Callbacks ----------

@app.callback(
    Output('main-year', 'options'),
    Input('data-refresh-interval', 'n_intervals'),
    Input('fast-data-refresh-interval', 'n_intervals')
)
def populate_main_year_options(n_intervals, fast_n_intervals):
    try:
        if is_data_updated():
            _o, _r, _c, m, _mc = load_data()
            try:
                os.remove('data_updated.txt')
            except Exception:
                pass
        else:
            _o, _r, _c, m, _mc = load_data()
        if 'Year' not in m.columns:
            return []
        years = sorted(m['Year'].dropna().unique())
        # Filter out 0 values (which represent invalid dates)
        years = [y for y in years if y > 0]
        return [{'label': str(int(y)), 'value': int(y)} for y in years]
    except Exception:
        return []

@app.callback(
    [Output('main-region', 'options'), Output('main-sm', 'options')],
    [Input('main-year', 'value'), Input('main-region', 'value')]
)
def populate_main_region_sm_options(selected_year, selected_region):
    try:
        _o, _r, _c, m, _mc = load_data()
        df = m.copy()
        if selected_year and 'Year' in df.columns:
            df = df[df['Year'] == selected_year]
        if selected_region and 'Region' in df.columns and selected_region != 'All':
            df = df[df['Region'] == selected_region]
        region_opts = [{'label': r, 'value': r} for r in sorted(df['Region'].dropna().unique())]
        if selected_region:
            df = df[df['Region'] == selected_region]
        sm_opts = [{'label': s, 'value': s} for s in sorted(df['SM'].dropna().unique())]
        return region_opts, sm_opts
    except Exception:
        return [], []

@app.callback(
    [Output('main-orders', 'children'),
     Output('main-revenue', 'children'),
     Output('main-cash', 'children'),
     Output('main-backlog', 'children'),
     Output('main-pending', 'children'),
     Output('main-po-count', 'children'),
     Output('main-charts', 'children'),
     Output('main-summary-table', 'children'),
     Output('main-chart1-store', 'data'),
     Output('main-chart2-store', 'data'),
     Output('main-filter-store', 'data')],
    [Input('main-year', 'value'),
     Input('main-region', 'value'),
     Input('main-sm', 'value'),
     Input('main-period', 'value'),
     Input('data-refresh-interval', 'n_intervals'),
     Input('fast-data-refresh-interval', 'n_intervals')]
)
def update_main_dashboard(year_value, region_value, sm_value, period_value, n_intervals, fast_n_intervals):
    try:
        _o, _r, _c, m, mc = load_data()
        df = m.copy()
        if year_value:
            df = df[df['Year'] == year_value]
        if region_value:
            df = df[df['Region'] == region_value]
        if sm_value:
            df = df[df['SM'] == sm_value]
        if df.empty:
            empty_table = dash_table.DataTable(columns=[], data=[])
            return 'N/A','N/A','N/A','N/A','N/A','0', [], [html.Div([empty_table])], None, None, None

        total_orders = df[mc["Order Amount"]].sum()
        total_revenue = df[mc["Revenue Amount"]].sum()
        total_cash = df[mc["Cash Amount"]].sum()
        total_backlog = df['Backlog Amount'].sum() if 'Backlog Amount' in df.columns else 0
        total_pending = df['Pending Amount'].sum() if 'Pending Amount' in df.columns else 0
        po_count = df['PO REF'].nunique() if 'PO REF' in df.columns else 0

        formatted = (
            f"€{total_orders:,.0f}",
            f"€{total_revenue:,.0f}",
            f"€{total_cash:,.0f}",
            f"€{total_backlog:,.0f}",
            f"€{total_pending:,.0f}",
            f"{po_count:,}"
        )

        work_df = df.copy()
        if period_value == 'Quarterly':
            work_df['Period'] = work_df['Month'].dt.to_period('Q').astype(str)
        else:
            work_df['Period'] = work_df['Month'].dt.to_period('M').astype(str)

        group_col = 'Region' if region_value or not sm_value else 'SM'
        agg = work_df.groupby(['Period', group_col], as_index=False).agg({
            mc['Revenue Amount']: 'sum',
            mc['Order Amount']: 'sum',
            mc['Cash Amount']: 'sum',
            'Backlog Amount': 'sum',
            'Pending Amount': 'sum'
        }).rename(columns={
            mc['Revenue Amount']: 'Revenue Amount',
            mc['Order Amount']: 'Order Amount',
            mc['Cash Amount']: 'Cash Amount',
            'Backlog Amount': 'Backlog Amount',
            'Pending Amount': 'Pending Amount'
        })

        # Chart 1: grouped bars for 3 measures by period and group
        melted = agg.melt(id_vars=['Period', group_col], value_vars=['Revenue Amount', 'Order Amount', 'Cash Amount'], var_name='Measure', value_name='Amount')
        fig1 = px.bar(melted, x='Period', y='Amount', color='Measure', pattern_shape=group_col, barmode='group', title=f"{period_value or 'Period'} Performance by {group_col}")
        fig1.update_layout(title_font_size=12, title_x=0.5, margin=dict(t=40, b=20, l=20, r=20))

        # Chart 2: line trend of measures by period
        trend = agg.groupby('Period', as_index=False).agg({'Revenue Amount':'sum','Order Amount':'sum','Cash Amount':'sum'})
        fig2 = px.line(trend, x='Period', y=['Revenue Amount','Order Amount','Cash Amount'], markers=True, title=f"{period_value or 'Period'} Trends")
        fig2.update_layout(title_font_size=12, title_x=0.5, margin=dict(t=40, b=20, l=20, r=20))

        charts = html.Div([
            create_chart_card(fig1, 'main-chart1'),
            create_chart_card(fig2, 'main-chart2')
        ], style={'display': 'flex', 'justifyContent': 'center', 'gap': '50px'})

        # Summary table
        if period_value == 'Quarterly':
            # Build a compact quarterly pivot table for the selected year
            sel_year = year_value if year_value else (df['Year'].max() if not df.empty else None)
            if sel_year is None:
                empty_table = dash_table.DataTable(columns=[], data=[])
                table = empty_table
            else:
                year_df = df.copy()
                year_df = year_df[year_df['Year'] == sel_year]
                # Ensure Month is datetime
                year_df['Quarter'] = year_df['Month'].dt.quarter

                measures_map = {
                    'Order Amount': mc['Order Amount'],
                    'Revenue Amount': mc['Revenue Amount'],
                    'Cash Amount': mc['Cash Amount'],
                    'Backlog Amount': 'Backlog Amount',
                    'Pending Amount': 'Pending Amount'
                }

                # Aggregate sums per quarter
                quarter_sums = {}
                for m_name, col in measures_map.items():
                    if col in year_df.columns:
                        s = year_df.groupby('Quarter', as_index=False)[col].sum()
                        s = s.set_index('Quarter')[col].to_dict()
                    else:
                        s = {}
                    quarter_sums[m_name] = s

                # Build rows: Measure + Q1..Q4 columns
                col_names = [f"{sel_year}Q1", f"{sel_year}Q2", f"{sel_year}Q3", f"{sel_year}Q4"]
                rows = []
                for m_name in ['Order Amount', 'Revenue Amount', 'Cash Amount', 'Backlog Amount', 'Pending Amount']:
                    qdict = quarter_sums.get(m_name, {})
                    row = {'Measure': m_name}
                    for q in range(1,5):
                        val = qdict.get(q, 0)
                        try:
                            val_num = float(val)
                        except Exception:
                            val_num = 0.0
                        row[f"{sel_year}Q{q}"] = f"€{val_num:,.0f}"
                    rows.append(row)

                # Build DataTable
                columns = [{'name': '', 'id': 'Measure'}] + [{'name': cn, 'id': cn, 'type': 'text'} for cn in col_names]
                table = dash_table.DataTable(
                    columns=columns,
                    data=rows,
                    style_table={'overflowX': 'auto', 'borderRadius': '8px', 'border': '1px solid #1976d2'},
                    style_cell={'textAlign': 'center', 'padding': '8px'},
                    style_header={'fontWeight': 'bold', 'backgroundColor': '#1976d2', 'color': 'white', 'border': 'none'},
                    style_data_conditional=[{'if': {'row_index': 'even'}, 'backgroundColor': '#e3f2fd'}],
                    export_format='csv',
                )
        else:
            table = dash_table.DataTable(
                columns=[
                    {"name": "Period", "id": "Period"},
                    {"name": group_col, "id": group_col},
                    {"name": "Order Amount", "id": "Order Amount", "type": "numeric", "format": {"specifier": ",.0f"}},
                    {"name": "Revenue Amount", "id": "Revenue Amount", "type": "numeric", "format": {"specifier": ",.0f"}},
                    {"name": "Cash Amount", "id": "Cash Amount", "type": "numeric", "format": {"specifier": ",.0f"}},
                    {"name": "Backlog Amount", "id": "Backlog Amount", "type": "numeric", "format": {"specifier": ",.0f"}},
                    {"name": "Pending Amount", "id": "Pending Amount", "type": "numeric", "format": {"specifier": ",.0f"}},
                ],
                data=agg.to_dict('records'),
                style_table={'overflowX': 'auto', 'borderRadius': '8px', 'border': '1px solid #1976d2'},
                style_cell={'textAlign': 'center', 'padding': '8px'},
                style_header={'fontWeight': 'bold', 'backgroundColor': '#1976d2', 'color': 'white', 'border': 'none'},
                style_data_conditional=[{'if': {'row_index': 'even'}, 'backgroundColor': '#e3f2fd'}],
                export_format='csv',
            )

        chart1_data = {'df': agg.to_json(orient='split'), 'title': fig1.layout.title.text}
        chart2_data = {'df': trend.to_json(orient='split'), 'title': fig2.layout.title.text}
        filters = {k:v for k,v in [('Year', year_value), ('Region', region_value), ('SM', sm_value), ('PeriodType', period_value)] if v}

        return *formatted, charts, [html.Div([table], style={'marginLeft': '2cm', 'marginRight': '2cm'})], json.dumps(chart1_data), json.dumps(chart2_data), json.dumps(filters)
    except Exception as e:
        print(f"Exception in update_main_dashboard: {e}")
        empty_table = dash_table.DataTable(columns=[], data=[])
        return 'N/A','N/A','N/A','N/A','N/A','0', [], [html.Div([empty_table])], None, None, None

# Toggle fast refresh interval based on data_updated.txt presence
@app.callback(
    Output('fast-data-refresh-interval', 'disabled'),
    Input('flag-check-interval', 'n_intervals'),
)
def toggle_fast_refresh(_):
    """Enable fast refresh if data_updated.txt exists, else disable. Triggered frequently by flag-check-interval."""
    return not is_data_updated()

# Export callbacks for main dashboard
@app.callback(Output("download-main-chart1", "data"), Input("export-main-chart1", "n_clicks"), State("main-chart1-store", "data"), State("main-filter-store", "data"), prevent_initial_call=True)
def export_main_chart1(n_clicks, chart_json, filter_json):
    return _create_export_data(n_clicks, chart_json, filter_json)

@app.callback(Output("download-main-chart2", "data"), Input("export-main-chart2", "n_clicks"), State("main-chart2-store", "data"), State("main-filter-store", "data"), prevent_initial_call=True)
def export_main_chart2(n_clicks, chart_json, filter_json):
    return _create_export_data(n_clicks, chart_json, filter_json)



if __name__ == "__main__":
    print("Dashboard will be available at: http://localhost:8053")

    # Run initial transformation from Google Sheets
    print("Fetching data from Google Sheets...")
    if get_google_sheets_data() is not None:
        transform_data()
        print("✅ Initial transformation completed")
    else:
        print("⚠️ Could not fetch from Google Sheets initially")

    # Reload data after transformation
    orders, revenues, cash, merged, measure_cols = load_data()

    # Note: Monitoring thread removed - data fetched from Google Sheets on demand
    print("✅ Dashboard initialized successfully")

    # Use PORT env var when provided by the host (e.g., Render.com)
    port = int(os.environ.get("PORT", 8053))
    debug_env = os.environ.get("DEBUG", "True").lower() in ("1", "true", "yes")
    app.run(debug=debug_env, host='0.0.0.0', port=port)