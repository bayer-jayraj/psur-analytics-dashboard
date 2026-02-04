import streamlit as st
import pandas as pd
import pyodbc
import datetime
import plotly.express as px
import plotly.graph_objects as go
from io import BytesIO
import numpy as np
import importlib
import time

# Check if pymssql is available as an alternative
try:
    pymssql_spec = importlib.util.find_spec("pymssql")
    pymssql_available = pymssql_spec is not None
except ImportError:
    pymssql_available = False

if pymssql_available:
    import pymssql

# ============================================================================
# CONNECTION RETRY CONFIGURATION
# ============================================================================
MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 2

# Set page config
st.set_page_config(
    page_title="PSUR Utilization Analytics",
    page_icon="📊",
    layout="wide"
)

# ============================================================================
# CONFIGURATION CONSTANTS - Easy to update for future years
# ============================================================================
LAST_FULL_YEAR = 2024  # Hardcoded as per requirement
CURRENT_YEAR = 2025    # Hardcoded as per requirement

# ============================================================================
# REGION MAPPING for Chart 2 - Uses Sales_Country_Region_lookup.csv
# Supports: EU, USA, ROW, China, UK regions
# ============================================================================
import os

# Global variable to cache the region lookup
_REGION_LOOKUP_CACHE = None

def load_region_lookup():
    """Load region lookup from CSV file and cache it"""
    global _REGION_LOOKUP_CACHE
    if _REGION_LOOKUP_CACHE is not None:
        return _REGION_LOOKUP_CACHE
    
    try:
        # Try to load from the csv file
        script_dir = os.path.dirname(os.path.abspath(__file__))
        lookup_path = os.path.join(script_dir, 'final csv files', 'Sales_Country_Region_lookup.csv')
        
        if os.path.exists(lookup_path):
            lookup_df = pd.read_csv(lookup_path)
            # Create dictionary mapping country to region
            _REGION_LOOKUP_CACHE = dict(zip(lookup_df['Country'].str.strip(), lookup_df['Region'].str.strip()))
        else:
            # Fallback to hardcoded mapping if file not found
            _REGION_LOOKUP_CACHE = {}
    except Exception as e:
        print(f"Warning: Could not load region lookup file: {e}")
        _REGION_LOOKUP_CACHE = {}
    
    return _REGION_LOOKUP_CACHE

def get_region(country):
    """Map a country to its region using Sales_Country_Region_lookup.csv
    Supports regions: EU, USA, ROW, China, UK
    """
    if not country or pd.isna(country):
        return 'Unknown'
    
    country_str = str(country).strip()
    
    # Load the lookup table
    region_lookup = load_region_lookup()
    
    # Direct lookup first
    if country_str in region_lookup:
        return region_lookup[country_str]
    
    # Try case-insensitive matching
    for lookup_country, region in region_lookup.items():
        if lookup_country.lower() == country_str.lower():
            return region
    
    # Fallback mappings for common variations not in lookup file
    fallback_mappings = {
        'United States': 'USA',
        'United States of America': 'USA',
        'US': 'USA',
        'United Kingdom': 'UK',
        'Great Britain': 'UK',
        'Britain': 'UK',
        'PR of China': 'China',
        'People\'s Republic of China': 'China',
    }
    
    if country_str in fallback_mappings:
        return fallback_mappings[country_str]
    
    # Default to ROW if not found
    return 'ROW'

# Initialize session state
if 'logged_in' not in st.session_state:
    st.session_state['logged_in'] = False
if 'conn' not in st.session_state:
    st.session_state['conn'] = None

# Country standardization function
def standardize_country_name(country):
    """Standardize country names and filter out unwanted entries"""
    if not country or pd.isna(country):
        return None
    
    country = str(country).strip()
    
    # Filter out 2-letter country codes and very short entries
    if len(country) <= 2:
        return None
        
    # Country mapping dictionary for common variations
    country_mapping = {
        'US': 'United States',
        'USA': 'United States', 
        'United States of America': 'United States',
        'UK': 'United Kingdom',
        'Britain': 'United Kingdom',
        'Great Britain': 'United Kingdom',
        'Deutschland': 'Germany',
        'DE': 'Germany',
        'FR': 'France',
        'IT': 'Italy',
        'ES': 'Spain',
        'NL': 'Netherlands',
        'Holland': 'Netherlands',
        'BE': 'Belgium',
        'CH': 'Switzerland',
        'AT': 'Austria',
        'SE': 'Sweden',
        'NO': 'Norway',
        'DK': 'Denmark',
        'FI': 'Finland',
        'PL': 'Poland',
        'CZ': 'Czech Republic',
        'HU': 'Hungary',
        'SK': 'Slovakia',
        'SI': 'Slovenia',
        'HR': 'Croatia',
        'RO': 'Romania',
        'BG': 'Bulgaria',
        'GR': 'Greece',
        'PT': 'Portugal',
        'IE': 'Ireland',
        'LU': 'Luxembourg',
        'MT': 'Malta',
        'CY': 'Cyprus',
        'EE': 'Estonia',
        'LV': 'Latvia',
        'LT': 'Lithuania',
        'CA': 'Canada',
        'MX': 'Mexico',
        'BR': 'Brazil',
        'AR': 'Argentina',
        'CL': 'Chile',
        'PE': 'Peru',
        'CO': 'Colombia',
        'VE': 'Venezuela',
        'AU': 'Australia',
        'NZ': 'New Zealand',
        'JP': 'Japan',
        'CN': 'China',
        'IN': 'India',
        'KR': 'South Korea',
        'TH': 'Thailand',
        'SG': 'Singapore',
        'MY': 'Malaysia',
        'ID': 'Indonesia',
        'PH': 'Philippines',
        'VN': 'Vietnam',
        'TW': 'Taiwan',
        'HK': 'Hong Kong',
        'ZA': 'South Africa',
        'EG': 'Egypt',
        'IL': 'Israel',
        'SA': 'Saudi Arabia',
        'AE': 'United Arab Emirates',
        'TR': 'Turkey',
        'RU': 'Russia'
    }
    
    # Check if country is in mapping
    if country in country_mapping:
        return country_mapping[country]
    
    # For other countries, capitalize properly
    return country.title()

# Function to detect available ODBC drivers
def get_available_odbc_driver():
    """Try to find an available SQL Server ODBC driver"""
    drivers_to_try = [
        'ODBC Driver 18 for SQL Server',
        'ODBC Driver 17 for SQL Server',
        'ODBC Driver 13 for SQL Server',
        'ODBC Driver 13.1 for SQL Server',
        'SQL Server Native Client 11.0',
        'SQL Server'
    ]
    
    try:
        available_drivers = pyodbc.drivers()
        for driver in drivers_to_try:
            if driver in available_drivers:
                return driver
    except:
        pass
    
    return None

# Function to connect to Azure SQL
def connect_to_azure_sql(username, password, server="ph-radc-server-eastus.database.windows.net", database="azure-db-radcommercial"):
    try:
        # Log connection attempt
        st.info(f"Attempting to connect to server: {server}")
        
        # Auto-detect available ODBC driver
        driver = get_available_odbc_driver()
        if driver:
            st.info(f"Using ODBC driver: {driver}")
        else:
            st.error("No SQL Server ODBC driver found. Please install ODBC Driver 17 or use pymssql connection method.")
            return None
        
        # Create connection string with detected driver
        # Increased Connection Timeout to 60 seconds and added Command Timeout
        conn_str = f"DRIVER={{{driver}}};SERVER=tcp:{server},1433;DATABASE={database};UID={username};PWD={password};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=60;"
        
        # Attempt connection
        conn = pyodbc.connect(conn_str, timeout=60)
        # Set command timeout to 120 seconds for long-running queries
        conn.timeout = 120
        
        # Store credentials for reconnection
        st.session_state['db_credentials'] = {
            'username': username,
            'password': password,
            'server': server,
            'database': database,
            'driver': driver,
            'conn_str': conn_str
        }
        
        return conn
    except pyodbc.Error as e:
        st.error(f"ODBC Connection Error: {str(e)}")
        # Add troubleshooting info
        st.info("""
        Troubleshooting Tips:
        1. Verify your server name is correct and includes '.database.windows.net'
        2. Make sure your username/password are correct
        3. Check if your IP address is allowed in Azure SQL firewall settings
        4. Verify the ODBC driver is installed correctly
        """)
        return None
    except Exception as e:
        st.error(f"General Error: {str(e)}")
        return None

# ============================================================================
# ROBUST QUERY EXECUTION WITH RETRY LOGIC
# ============================================================================
def reconnect_to_database():
    """Attempt to reconnect to the database using stored credentials"""
    if 'db_credentials' not in st.session_state:
        return None
    
    creds = st.session_state['db_credentials']
    try:
        conn = pyodbc.connect(creds['conn_str'], timeout=60)
        conn.timeout = 120
        st.session_state['conn'] = conn
        return conn
    except Exception as e:
        st.warning(f"Reconnection attempt failed: {str(e)}")
        return None

def is_connection_alive(conn):
    """Check if the database connection is still alive"""
    if conn is None:
        return False
    try:
        # Try a simple query to test connection
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.fetchone()
        cursor.close()
        return True
    except:
        return False

def execute_query_with_retry(query, conn=None, max_retries=MAX_RETRIES):
    """
    Execute a SQL query with automatic retry and reconnection logic.
    Handles 'Communication link failure' and other transient errors.
    
    Args:
        query: SQL query string to execute
        conn: Database connection (uses session state if None)
        max_retries: Maximum number of retry attempts
    
    Returns:
        pandas DataFrame with query results, or empty DataFrame on failure
    """
    if conn is None:
        conn = st.session_state.get('conn')
    
    last_error = None
    
    for attempt in range(max_retries):
        try:
            # Check if connection is alive, reconnect if needed
            if not is_connection_alive(conn):
                st.warning(f"Connection lost. Attempting to reconnect... (Attempt {attempt + 1}/{max_retries})")
                conn = reconnect_to_database()
                if conn is None:
                    time.sleep(RETRY_DELAY_SECONDS * (attempt + 1))  # Exponential backoff
                    continue
            
            # Execute the query
            df = pd.read_sql(query, conn)
            return df
            
        except (pyodbc.Error, Exception) as e:
            last_error = e
            error_str = str(e).lower()
            
            # Check for transient/recoverable errors
            transient_errors = [
                'communication link failure',
                'connection was closed',
                'connection is broken',
                'login timeout',
                'timeout expired',
                'server is not responding',
                '08s01',  # ODBC communication link failure code
                '08003',  # Connection not open
                '08007',  # Connection failure during transaction
            ]
            
            is_transient = any(err in error_str for err in transient_errors)
            
            if is_transient and attempt < max_retries - 1:
                wait_time = RETRY_DELAY_SECONDS * (attempt + 1)
                st.warning(f"Database connection issue. Retrying in {wait_time} seconds... (Attempt {attempt + 1}/{max_retries})")
                time.sleep(wait_time)
                
                # Try to reconnect
                conn = reconnect_to_database()
            else:
                # Non-transient error or last attempt
                break
    
    # All retries failed
    error_msg = f"Error executing query after {max_retries} attempts: {str(last_error)}"
    st.error(error_msg)
    
    # Show debug info in expander
    with st.expander("Debug Information"):
        st.write(f"Selected product line: {st.session_state.get('selected_product_line', 'N/A')}")
        if 'start_date' in st.session_state:
            st.write(f"Date range: {st.session_state.get('start_date')} to {st.session_state.get('end_date')}")
        st.write(f"Error details: {str(last_error)}")
        st.code(query[:500] + "..." if len(query) > 500 else query, language="sql")
    
    return pd.DataFrame()

# ============================================================================
# DATA VALIDATION HELPER FUNCTIONS
# ============================================================================
def check_data_availability(conn, table_name, date_column, product_line=None, brand_column=None):
    """
    Check data availability for a given table and return date range info.
    Helps identify data gaps gracefully.
    """
    try:
        where_clause = ""
        if product_line and brand_column:
            where_clause = f"WHERE {brand_column} = '{product_line}'"
        
        query = f"""
        SELECT 
            MIN(TRY_CONVERT(date, {date_column})) as min_date,
            MAX(TRY_CONVERT(date, {date_column})) as max_date,
            COUNT(*) as record_count,
            COUNT(DISTINCT YEAR(TRY_CONVERT(date, {date_column}))) as year_count
        FROM {table_name}
        {where_clause}
        """
        df = pd.read_sql(query, conn)
        return df.iloc[0].to_dict() if not df.empty else None
    except Exception as e:
        return None

def get_years_with_data(conn, table_name, date_column, product_line=None, brand_column=None):
    """Get list of years that have data in the table"""
    try:
        where_clause = ""
        if product_line and brand_column:
            where_clause = f"WHERE {brand_column} = '{product_line}'"
        
        query = f"""
        SELECT DISTINCT YEAR(TRY_CONVERT(date, {date_column})) as data_year
        FROM {table_name}
        {where_clause}
        AND {date_column} IS NOT NULL
        ORDER BY data_year
        """
        df = pd.read_sql(query, conn)
        return df['data_year'].tolist() if not df.empty else []
    except Exception as e:
        return []

# Login Page
if not st.session_state['logged_in']:
    st.title("PSUR Utilization Analytics Dashboard")
    st.subheader("Login to Azure SQL Database")
    
    col1, col2 = st.columns(2)
    
    with col1:
        username = st.text_input("Username")
        password = st.text_input("Password", type="password")
        server = st.text_input("Server (Optional)", value="ph-radc-server-eastus.database.windows.net")
        database = st.text_input("Database", value="azure-db-radcommercial")
        
        # Add connection method selection
        connection_method = st.radio(
            "Connection Method",
            options=["ODBC", "pymssql"] if pymssql_available else ["ODBC"]
        )
        
        if st.button("Connect"):
            if username and password:
                if connection_method == "ODBC":
                    conn = connect_to_azure_sql(username, password, server, database)
                else:  # using pymssql
                    try:
                        # If pymssql is selected, use it instead
                        st.info("Attempting connection using pymssql...")
                        conn = pymssql.connect(
                            server=server.replace('.database.windows.net', ''),  # pymssql adds this automatically
                            user=username,
                            password=password,
                            database=database
                        )
                        st.info("pymssql connection successful!")
                    except Exception as e:
                        st.error(f"pymssql connection error: {str(e)}")
                        conn = None
                        
                if conn:
                    st.session_state['conn'] = conn
                    st.session_state['logged_in'] = True
                    st.success("Connected to Azure SQL Database successfully!")
                    st.rerun()
            else:
                st.warning("Please enter both username and password")
    
    with col2:
        st.info("""
        ### Welcome to PSUR Analytics
        
        This dashboard allows you to:
        - Explore database tables
        - Download data
        - Analyze basic statistics  
        - Generate PSUR reports based on product and timeline
        
        Please login with your Azure SQL credentials to continue.
        """)

# Main Application
else:
    # Create tabs
    tab1, tab2, tab3 = st.tabs(["Data Explorer", "PSUR Report Generator", "Risk Calculation"])
    
    # Tab 1: Data Explorer  
    with tab1:
        st.header("Data Explorer")
        
        # Function to get table list
        @st.cache_data
        def get_tables():
            tables_query = """
            SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_TYPE = 'BASE TABLE'
            ORDER BY TABLE_NAME
            """
            return pd.read_sql(tables_query, st.session_state['conn'])
        
        # Get table list
        try:
            tables_df = get_tables()
            table_list = tables_df['TABLE_NAME'].tolist()
            
            # Table selection
            selected_table = st.selectbox("Select a table", table_list)
            
            # Function to get table data
            def get_table_data(table_name, limit=1000):
                query = f"SELECT TOP {limit} * FROM [{table_name}]"
                return pd.read_sql(query, st.session_state['conn'])
            
            # Get and display data
            if selected_table:
                with st.spinner(f"Loading data from {selected_table}..."):
                    data = get_table_data(selected_table)
                    
                st.write(f"Showing top 1000 rows from {selected_table} table:")
                st.dataframe(data)
                
                # Download data button
                def to_excel(df):
                    try:
                        output = BytesIO()
                        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                            df.to_excel(writer, sheet_name='Sheet1', index=False)
                        processed_data = output.getvalue()
                        return processed_data, "xlsx"
                    except ImportError:
                        # If xlsxwriter is not available, fall back to CSV
                        output = BytesIO()
                        df.to_csv(output, index=False, encoding='utf-8')
                        processed_data = output.getvalue()
                        return processed_data, "csv"
                
                excel_data, file_ext = to_excel(data)
                
                if file_ext == "xlsx":
                    st.download_button(
                        label="Download data as Excel",
                        data=excel_data,
                        file_name=f'{selected_table}.xlsx',
                        mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
                    )
                else:
                    st.download_button(
                        label="Download data as CSV", 
                        data=excel_data,
                        file_name=f'{selected_table}.csv',
                        mime='text/csv'
                    )
                    st.info("Note: Excel download is not available because the 'xlsxwriter' module is not installed. Using CSV format instead. To enable Excel downloads, install xlsxwriter with: pip install xlsxwriter")
                
                # Basic statistics
                st.subheader("Basic Statistics")
                numeric_columns = data.select_dtypes(include=['number']).columns.tolist()
                if numeric_columns:
                    selected_column = st.selectbox("Select column for statistics", numeric_columns)
                    stats = data[selected_column].describe()
                    st.write(stats)
                    
                    # Simple histogram
                    fig = px.histogram(data, x=selected_column, title=f"Distribution of {selected_column}")
                    st.plotly_chart(fig)
                    
                    # Box plot
                    fig2 = px.box(data, y=selected_column, title=f"Box Plot of {selected_column}")
                    st.plotly_chart(fig2)
                else:
                    st.info("No numeric columns available for statistics")
                    
        except Exception as e:
            st.error(f"Error retrieving data: {str(e)}")
    
    # Tab 2: PSUR Report Generator
    with tab2:
        st.header("PSUR Report Generator")
        
        # Functions to get unique values for dropdowns
        @st.cache_data
        def get_product_lines():
            try:
                query = """
                SELECT DISTINCT Brand FROM MaterialReference
                WHERE Brand IS NOT NULL
                """
                df = pd.read_sql(query, st.session_state['conn'])
                return df.iloc[:, 0].tolist()
            except Exception as e:
                st.error(f"Error retrieving product lines: {str(e)}")
                return []
        
        @st.cache_data
        def get_catalogs():
            try:
                query = """
                SELECT DISTINCT CATALOG FROM MaterialReference
                WHERE CATALOG IS NOT NULL
                """
                df = pd.read_sql(query, st.session_state['conn'])
                return df.iloc[:, 0].tolist()
            except Exception as e:
                st.error(f"Error retrieving catalogs: {str(e)}")
                return []
        
        @st.cache_data
        def get_countries():
            """Get standardized list of countries from all relevant tables"""
            try:
                query = """
                SELECT DISTINCT Country_final_dest as Country FROM Sales
                WHERE Country_final_dest IS NOT NULL
                UNION
                SELECT DISTINCT COUNTRY_of_ORIGIN as Country FROM AdverseEventsData  
                WHERE COUNTRY_of_ORIGIN IS NOT NULL
                UNION
                SELECT DISTINCT CD_Complaint_Country as Country FROM ComplaintMerged
                WHERE CD_Complaint_Country IS NOT NULL
                """
                df = pd.read_sql(query, st.session_state['conn'])
                
                # Apply country standardization
                countries = []
                for country in df['Country'].tolist():
                    standardized = standardize_country_name(country)
                    if standardized and standardized not in countries:
                        countries.append(standardized)
                
                # Sort alphabetically
                return sorted(countries)
                
            except Exception as e:
                st.error(f"Error retrieving countries: {str(e)}")
                return []
        
        # Get unique values for dropdowns
        product_lines = get_product_lines()
        catalogs = get_catalogs()
        countries = get_countries()
        
        # Form for PSUR report generation
        st.subheader("Select Parameters for PSUR Report")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Required fields
            # Set default to Arterion if available, otherwise first item
            default_product = "Arterion" if "Arterion" in product_lines else (product_lines[0] if product_lines else None)
            default_index = product_lines.index(default_product) if default_product in product_lines else 0
            selected_product_line = st.selectbox("Product Line (Required)", product_lines, index=default_index)
            
            # Date selectors
            today = datetime.datetime.now().date()
            start_date = st.date_input("Start Date (Required)", value=today - datetime.timedelta(days=730))
            end_date = st.date_input("End Date (Required)", value=today)
            
            # Optional fields
            selected_catalog = st.selectbox("Catalog (Optional)", [""] + catalogs)
            
            # Country multi-select with United States as default
            st.write("**Countries (Select one or more)**")
            default_countries = ['United States'] if 'United States' in countries else []
            selected_countries = st.multiselect(
                "Select Countries",
                options=countries,
                default=default_countries,
                help="Select one or more countries. Leave empty to include all countries."
            )
        
        with col2:
            st.info(f"""
            ### PSUR Report Information
            
            This report will include:
            - **Chart 1**: Sales by Country with Product Type & Years as Columns
            - **Chart 2**: Sales by Region (EEA_CH_TR/ROW/USA) with Product Type
            - Adverse Events Analysis
            - Field Notices / Recalls
            - Customer Complaint / User Feedback Review
            - Complaints per Final Object Code
            
            **Configuration:**
            - Last Full Year: **{LAST_FULL_YEAR}**
            - Current Year: **{CURRENT_YEAR}**
            
            **Note:** Charts display top 10 values only.
            Required fields must be filled. Optional fields can be left blank to include all values.
            """)
        
        # Generate report button
        if st.button("Generate PSUR Report"):
            if not selected_product_line or selected_product_line == "":
                st.error("Product Line is required. Please select a Product Line.")
            else:
                st.subheader("PSUR Report Results")
                
                # Display selected parameters
                st.write("#### Parameters")
                st.write(f"- Product Line: {selected_product_line}")
                st.write(f"- Date Range: {start_date} to {end_date}")
                if selected_catalog and selected_catalog != "":
                    st.write(f"- Catalog: {selected_catalog}")
                if selected_countries:
                    st.write(f"- Countries: {', '.join(selected_countries)}")
                else:
                    st.write(f"- Countries: All")
                
                # Calculate last year in user's selected range for complaint tables
                last_year_in_range = min(end_date.year, LAST_FULL_YEAR)
                
                try:
                    with st.spinner("Generating PSUR report..."):
                        # Convert dates to strings in SQL format
                        start_date_str = start_date.strftime('%Y-%m-%d')
                        end_date_str = end_date.strftime('%Y-%m-%d')
                        
                        # Helper function to create country filter for SQL queries
                        def get_country_filter(column_name, selected_countries_list):
                            if not selected_countries_list or len(selected_countries_list) == 0:
                                return ""
                            
                            # Reverse mappings for common country name variations
                            reverse_mapping = {
                                'United States': ['US', 'USA', 'United States of America', 'United States'],
                                'United Kingdom': ['UK', 'Britain', 'Great Britain', 'United Kingdom'],
                                'Germany': ['Deutschland', 'DE', 'Germany'],
                                'Netherlands': ['Holland', 'NL', 'Netherlands']
                            }
                            
                            # Build list of all country variations
                            all_variations = []
                            for country in selected_countries_list:
                                if country in reverse_mapping:
                                    all_variations.extend(reverse_mapping[country])
                                else:
                                    all_variations.append(country)
                            
                            # Remove duplicates
                            all_variations = list(set(all_variations))
                            
                            # Create OR condition for all variations
                            conditions = [f"{column_name} = '{var}'" for var in all_variations]
                            return f"AND ({' OR '.join(conditions)})"
                        
                        # ================================================================
                        # 1. SALES BY COUNTRY AND YEAR - CHART 1 (Individual Countries)
                        # ================================================================
                        st.subheader("1. Sales Analysis")
                        
                        # FIX TC.2.1.2/TC2.1.3: Calculate the last year in the user's selected range
                        # For Chart 1 (Sales by Country), show only the LAST year of the range
                        last_year_for_sales = end_date.year
                        last_year_sales_start = f"{last_year_for_sales}-01-01"
                        last_year_sales_end = f"{last_year_for_sales}-12-31"
                        
                        # FIX TC2.1.13: Use ROW_NUMBER() to get truly unique MATNo and avoid duplicate counting
                        # The previous DISTINCT on multiple columns still caused duplicates when MATNo had different ProductGroup values
                        # Query for Sales by Country - LAST YEAR ONLY (FIX for TC.2.1.2/TC2.1.3)
                        # Chart 1 should only show the last year in the date range the user inputs
                        sales_by_country_query = f"""
                        WITH RankedMatRef AS (
                            SELECT MATNo, Brand, CATALOG, COALESCE(ProductGroup, 'Other') as ProductGroup,
                                   ROW_NUMBER() OVER (PARTITION BY MATNo ORDER BY ProductGroup) as rn
                            FROM MaterialReference
                            WHERE Brand = '{selected_product_line}'
                            {f"AND CATALOG = '{selected_catalog}'" if selected_catalog else ""}
                        ),
                        UniqueMatRef AS (
                            SELECT MATNo, Brand, CATALOG, ProductGroup
                            FROM RankedMatRef
                            WHERE rn = 1
                        )
                        SELECT 
                            s.Country_final_dest,
                            YEAR(s.[Date]) as SaleYear,
                            m.ProductGroup as ProductType,
                            SUM(s.Quantity) as TotalQuantity
                        FROM Sales s
                        INNER JOIN UniqueMatRef m ON s.Material = m.MATNo
                        WHERE YEAR(s.[Date]) = {last_year_for_sales}
                        {get_country_filter('s.Country_final_dest', selected_countries)}
                        GROUP BY s.Country_final_dest, YEAR(s.[Date]), m.ProductGroup
                        """
                        
                        # FIX TC2.1.13: Query for comprehensive Sales data - ALL years in range (for Chart 2)
                        # Use ROW_NUMBER() to get truly unique MATNo and avoid duplicate counting
                        sales_query = f"""
                        WITH RankedMatRef AS (
                            SELECT MATNo, Brand, CATALOG, COALESCE(ProductGroup, 'Other') as ProductGroup,
                                   ROW_NUMBER() OVER (PARTITION BY MATNo ORDER BY ProductGroup) as rn
                            FROM MaterialReference
                            WHERE Brand = '{selected_product_line}'
                            {f"AND CATALOG = '{selected_catalog}'" if selected_catalog else ""}
                        ),
                        UniqueMatRef AS (
                            SELECT MATNo, Brand, CATALOG, ProductGroup
                            FROM RankedMatRef
                            WHERE rn = 1
                        )
                        SELECT 
                            s.Country_final_dest,
                            YEAR(s.[Date]) as SaleYear,
                            m.ProductGroup as ProductType,
                            SUM(s.Quantity) as TotalQuantity
                        FROM Sales s
                        INNER JOIN UniqueMatRef m ON s.Material = m.MATNo
                        WHERE s.[Date] >= '{start_date_str}'
                        AND s.[Date] <= '{end_date_str}'
                        {get_country_filter('s.Country_final_dest', selected_countries)}
                        GROUP BY s.Country_final_dest, YEAR(s.[Date]), m.ProductGroup
                        """
                        
                        # Use retry logic for Sales queries to handle connection issues
                        sales_by_country = execute_query_with_retry(sales_query)
                        
                        # Execute the last year only query for Chart 1
                        sales_last_year = execute_query_with_retry(sales_by_country_query)
                        
                        if not sales_by_country.empty:
                            # ============================================================
                            # CHART 1: Sales by Country - LAST YEAR ONLY (FIX TC.2.1.2/TC2.1.3)
                            # ============================================================
                            st.write("### Chart 1: Sales by Country and Product Group")
                            st.write(f"**Table 4a: {selected_product_line} Sales by Country ({last_year_for_sales} Only)**")
                            st.caption(f"*Showing sales data for the last year ({last_year_for_sales}) of the selected date range*")
                            
                            if not sales_last_year.empty:
                                # Rename ProductType to ProductGroup for display
                                sales_last_year_display = sales_last_year.rename(columns={'ProductType': 'ProductGroup'})
                                
                                # Create pivot table for last year only: ProductGroups as columns (Change 1)
                                sales_pivot_country = sales_last_year_display.pivot_table(
                                    index='Country_final_dest',
                                    columns='ProductGroup',
                                    values='TotalQuantity',
                                    fill_value=0,
                                    aggfunc='sum'
                                ).reset_index()
                                
                                # Rename columns for clarity
                                sales_pivot_country.columns.name = None
                                sales_pivot_country = sales_pivot_country.rename(columns={'Country_final_dest': 'Country'})
                                
                                # Sort by country
                                sales_pivot_country = sales_pivot_country.sort_values('Country')
                                
                                # Display the table - format all numeric columns with thousand separators
                                numeric_cols = [col for col in sales_pivot_country.columns if col != 'Country']
                                st.dataframe(sales_pivot_country.style.format(
                                    {col: '{:,.0f}' for col in numeric_cols}
                                ), use_container_width=True)
                                
                                # Create grouped bar chart for Chart 1 (Top 10 countries) - LAST YEAR ONLY
                                # Aggregate by country first, sorted by total quantity descending
                                country_totals = sales_last_year_display.groupby('Country_final_dest')['TotalQuantity'].sum().nlargest(10)
                                # Get countries in descending order by total
                                sorted_countries = country_totals.index.tolist()
                                chart1_data = sales_last_year_display[sales_last_year_display['Country_final_dest'].isin(sorted_countries)]
                                
                                # Create categorical order for x-axis (descending by total)
                                fig1 = px.bar(
                                    chart1_data,
                                    x='Country_final_dest',
                                    y='TotalQuantity',
                                    color='ProductGroup',
                                    barmode='group',
                                    title=f"{selected_product_line} Sales by Country and Product Group ({last_year_for_sales}) - Top 10",
                                    labels={'TotalQuantity': 'Total Quantity', 'Country_final_dest': 'Country', 'ProductGroup': 'Product Group'},
                                    category_orders={'Country_final_dest': sorted_countries}  # Sort descending by total
                                )
                                fig1.update_layout(xaxis_tickangle=-45)
                                st.plotly_chart(fig1, use_container_width=True)
                            else:
                                st.info(f"No sales data found for {last_year_for_sales}.")
                            
                            # ============================================================
                            # CHART 2: Sales by Region (EU, USA, ROW, China, UK) with Product Group
                            # Shows comprehensive sales across the full date range
                            # ============================================================
                            st.write("### Chart 2: Sales by Region and Product Group")
                            st.write(f"**Table 4b: {selected_product_line} Sales by Region ({start_date.year} - {end_date.year})**")
                            
                            # Add region column using lookup file
                            sales_by_country['Region'] = sales_by_country['Country_final_dest'].apply(get_region)
                            
                            # Rename ProductType to ProductGroup for display
                            sales_by_country_display = sales_by_country.rename(columns={'ProductType': 'ProductGroup'})
                            
                            # Aggregate by Region, ProductGroup, and Year
                            sales_by_region = sales_by_country_display.groupby(['Region', 'ProductGroup', 'SaleYear'])['TotalQuantity'].sum().reset_index()
                            
                            # Create pivot table: Region as rows, Years as columns, with ProductGroup subtotals (Change 1)
                            # First create a multi-index pivot with Region and Year, then ProductGroup as columns
                            sales_pivot_region = sales_by_region.pivot_table(
                                index='Region',
                                columns=['ProductGroup', 'SaleYear'],
                                values='TotalQuantity',
                                fill_value=0,
                                aggfunc='sum'
                            ).reset_index()
                            
                            # Flatten column multi-index for display
                            sales_pivot_region.columns = [col[0] if col[1] == '' else f"{col[0]}_{col[1]}" for col in sales_pivot_region.columns]
                            
                            # Sort by region order (China, EU, ROW, UK, USA)
                            region_order = {'China': 0, 'EU': 1, 'ROW': 2, 'UK': 3, 'USA': 4}
                            sales_pivot_region['sort_key'] = sales_pivot_region['Region'].map(lambda x: region_order.get(x, 99))
                            sales_pivot_region = sales_pivot_region.sort_values('sort_key').drop('sort_key', axis=1)
                            
                            # Display the table - format all numeric columns with thousand separators
                            numeric_cols_region = [col for col in sales_pivot_region.columns if col != 'Region']
                            st.dataframe(sales_pivot_region.style.format(
                                {col: '{:,.0f}' for col in numeric_cols_region}
                            ), use_container_width=True)
                            
                            # Create grouped bar chart for Chart 2
                            fig2 = px.bar(
                                sales_by_region,
                                x='Region',
                                y='TotalQuantity',
                                color='ProductGroup',
                                barmode='group',
                                title=f"{selected_product_line} Sales by Region and Product Group ({start_date.year} - {end_date.year})",
                                labels={'TotalQuantity': 'Total Quantity', 'Region': 'Region', 'ProductGroup': 'Product Group'},
                                category_orders={'Region': ['China', 'EU', 'ROW', 'UK', 'USA']}
                            )
                            st.plotly_chart(fig2, use_container_width=True)
                            
                            # Add footnote explaining product categories
                            st.caption("*Product Group indicates the type of product: Syringe Kits, Disposables, or other categories as defined in Material Reference.*")
                            
                        else:
                            st.info("No sales data found for the selected criteria.")
                        
                        # ================================================================
                        # 2. ADVERSE EVENTS
                        # ================================================================
                        st.subheader("2. Adverse Events")
                        
                        # FIX TC2.1.4-TC2.1.7: Use YEAR column directly instead of converting dates
                        # Also exclude "Non-incident, requested by NHA" per test case notes
                        adverse_events_query = f"""
                        SELECT 
                            Type_of_Incident,
                            YEAR,
                            COUNT(*) as EventCount
                        FROM AdverseEventsData
                        WHERE Product_Line = '{selected_product_line}'
                        AND YEAR >= {start_date.year}
                        AND YEAR <= {end_date.year}
                        {f"AND Catalog = '{selected_catalog}'" if selected_catalog else ""}
                        {get_country_filter('COUNTRY_of_ORIGIN', selected_countries)}
                        AND Type_of_Incident IS NOT NULL
                        AND Type_of_Incident NOT LIKE '%Non-incident%'
                        GROUP BY Type_of_Incident, YEAR
                        ORDER BY YEAR, Type_of_Incident
                        """
                        
                        # Use retry logic for Adverse Events query
                        adverse_events = execute_query_with_retry(adverse_events_query)
                        
                        if not adverse_events.empty:
                            # Create pivot table for adverse events
                            ae_pivot = adverse_events.pivot_table(
                                index='YEAR',
                                columns='Type_of_Incident',
                                values='EventCount',
                                fill_value=0
                            )
                            
                            # Create stacked bar chart
                            fig = go.Figure()
                            colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', '#8c564b']
                            
                            for i, column in enumerate(ae_pivot.columns):
                                fig.add_trace(go.Bar(
                                    name=column,
                                    x=ae_pivot.index.astype(int).astype(str),  # Convert to string to avoid decimals
                                    y=ae_pivot[column],
                                    marker_color=colors[i % len(colors)]
                                ))
                            
                            fig.update_layout(
                                title=f"{selected_product_line} MDR Breakdown ({start_date.year} - {end_date.year})",
                                xaxis_title="Year",
                                yaxis_title="# MDRs",
                                barmode='stack',
                                height=400,
                                xaxis={
                                    'type': 'category'  # Treat years as categories to avoid duplicates/decimals
                                }
                            )
                            st.plotly_chart(fig, use_container_width=True)
                            
                            # Create table view
                            ae_table = ae_pivot.reset_index()
                            ae_table.columns.name = None
                            # Format YEAR column as integer without commas
                            ae_table['YEAR'] = ae_table['YEAR'].astype(int)
                            
                            st.write(f"**Table 5: {selected_product_line} MDR Category Totals ({start_date.year} – {end_date.year})**")
                            st.dataframe(ae_table.style.format({'YEAR': '{:.0f}'}))
                        else:
                            st.info("No adverse events data found for the selected criteria.")
                        
                        # 2b. ADVERSE EVENTS BY COUNTRY  
                        st.subheader("2b. Adverse Events by Country")
                        
                        # FIX TC2.1.4-TC2.1.7: Use YEAR column directly, exclude Non-incidents
                        ae_by_country_query = f"""
                        SELECT 
                            COUNTRY_of_ORIGIN,
                            Type_of_Incident,
                            COUNT(*) as EventCount
                        FROM AdverseEventsData
                        WHERE Product_Line = '{selected_product_line}'
                        AND YEAR >= {start_date.year}
                        AND YEAR <= {end_date.year}
                        {f"AND Catalog = '{selected_catalog}'" if selected_catalog else ""}
                        {get_country_filter('COUNTRY_of_ORIGIN', selected_countries)}
                        AND COUNTRY_of_ORIGIN IS NOT NULL
                        AND Type_of_Incident IS NOT NULL
                        AND Type_of_Incident NOT LIKE '%Non-incident%'
                        GROUP BY COUNTRY_of_ORIGIN, Type_of_Incident
                        ORDER BY COUNTRY_of_ORIGIN, Type_of_Incident
                        """
                        
                        # Use retry logic for AE by country query
                        ae_by_country = execute_query_with_retry(ae_by_country_query)
                        
                        if not ae_by_country.empty:
                            # Create pivot for country view
                            ae_country_pivot = ae_by_country.pivot_table(
                                index='COUNTRY_of_ORIGIN',
                                columns='Type_of_Incident',
                                values='EventCount',
                                fill_value=0
                            )
                            
                            # Get top 10 countries by total events
                            top_countries = ae_country_pivot.sum(axis=1).nlargest(10).index
                            ae_country_pivot_top10 = ae_country_pivot.loc[top_countries]
                            
                            # Create stacked bar chart
                            fig = go.Figure()
                            colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', '#8c564b']
                            
                            for i, column in enumerate(ae_country_pivot_top10.columns):
                                fig.add_trace(go.Bar(
                                    name=column,
                                    x=ae_country_pivot_top10.index,
                                    y=ae_country_pivot_top10[column],
                                    marker_color=colors[i % len(colors)]
                                ))
                            
                            # FIX TC2.1.6: Title must match actual query date range, not LAST_FULL_YEAR
                            year_range_display = f"{start_date.year}-{end_date.year}" if start_date.year != end_date.year else str(start_date.year)
                            fig.update_layout(
                                title=f"{selected_product_line} Adverse Events by Country - {year_range_display} (Top 10)",
                                xaxis_title="Country",
                                yaxis_title="Events",
                                barmode='stack',
                                height=400,
                                xaxis_tickangle=-45
                            )
                            st.plotly_chart(fig, use_container_width=True)
                            
                            st.write(f"**Figure 2: {selected_product_line} Adverse Event (MDR) Breakdown by Country ({year_range_display})**")
                            
                            # Display table
                            ae_country_table = ae_country_pivot.reset_index()
                            ae_country_table.columns.name = None
                            st.dataframe(ae_country_table)
                        else:
                            st.info("No adverse events by country data found for the selected criteria.")
                        
                        # ================================================================
                        # 3. FIELD NOTICES / RECALLS
                        # ================================================================
                        st.subheader("3. Field Notices / Recalls")
                        
                        recalls_query = f"""
                        SELECT 
                            Name_of_Issue,
                            Brief_Description,
                            Agency_Notifications,
                            Products,
                            YEAR(Date_Initiated) AS Year_Initiated
                        FROM Recalls
                        WHERE (Products = '{selected_product_line}' OR Products LIKE '%{selected_product_line}%')
                        AND Date_Initiated >= '{start_date_str}'
                        AND Date_Initiated <= '{end_date_str}'
                        ORDER BY Date_Initiated DESC
                        """
                        
                        try:
                            # Use retry logic for recalls query
                            recalls_data = execute_query_with_retry(recalls_query)
                            
                            if not recalls_data.empty:
                                st.write(f"**Table 7: {selected_product_line} Product Recalls ({start_date.year} - {end_date.year})**")
                                st.dataframe(recalls_data)
                                
                                # CHANGE 3: Fix decimal years in recalls graph
                                recalls_summary = recalls_data.groupby('Year_Initiated').size().reset_index(name='Count')
                                # Ensure Year is integer
                                recalls_summary['Year_Initiated'] = recalls_summary['Year_Initiated'].astype(int)
                                
                                fig = px.bar(
                                    recalls_summary,
                                    x='Year_Initiated',
                                    y='Count',
                                    title=f"{selected_product_line} Recalls by Year ({start_date.year} - {end_date.year})",
                                    labels={'Count': 'Number of Recalls', 'Year_Initiated': 'Year'}
                                )
                                # Force integer display on x-axis
                                fig.update_layout(
                                    xaxis={
                                        'type': 'category',  # Treat as category to show each year once
                                        'categoryorder': 'category ascending'
                                    }
                                )
                                fig.update_traces(
                                    text=recalls_summary['Count'],
                                    textposition='outside'
                                )
                                st.plotly_chart(fig, use_container_width=True)
                            else:
                                st.info(f"No recalls found for {selected_product_line} in the selected time period.")
                        except Exception as e:
                            st.warning(f"Recalls table may not exist in the database: {str(e)}")
                        
                        # ================================================================
                        # 4. CUSTOMER COMPLAINT / USER FEEDBACK REVIEW
                        # ================================================================
                        st.subheader("4. Customer Complaint / User Feedback Review")
                        
                        # FIX TC3.1.3: Add toggle to show complaint totals for full date range or last year only
                        st.write("**Complaint Data View Options:**")
                        complaint_date_option = st.radio(
                            "Select date range for complaint totals:",
                            ["Full Date Range", "Last Year Only"],
                            horizontal=True,
                            key="complaint_date_option",
                            help="Full Date Range uses the entire selected period. Last Year Only uses only the final year of the range."
                        )
                        
                        if complaint_date_option == "Full Date Range":
                            # Use full date range
                            complaint_start = start_date_str
                            complaint_end = end_date_str
                            complaint_period_label = f"{start_date.year} - {end_date.year}"
                        else:
                            # Use last year in range (original behavior)
                            last_year_in_range = end_date.year
                            complaint_start = f"{last_year_in_range}-01-01"
                            complaint_end = f"{last_year_in_range}-12-31"
                            complaint_period_label = str(last_year_in_range)
                        
                        # For procedure calculation, always use full date range for accurate rates
                        last_year_in_range = end_date.year
                        last_year_start = f"{last_year_in_range}-01-01"
                        last_year_end = f"{last_year_in_range}-12-31"
                        
                        # Complaint Totals and Rates by Country
                        # FIX TC2.1.10 & TC3.1.3: Use ROW_NUMBER() to get truly unique MATNo and use selected date range
                        complaint_rates_query = f"""
                        WITH RankedMatRef AS (
                            SELECT MATNo, Brand, CATALOG, SingleUse,
                                   ROW_NUMBER() OVER (PARTITION BY MATNo ORDER BY CATALOG) as rn
                            FROM MaterialReference
                            WHERE Brand = '{selected_product_line}'
                            AND SingleUse = 'Y'
                            {f"AND CATALOG = '{selected_catalog}'" if selected_catalog else ""}
                        ),
                        UniqueMatRef AS (
                            SELECT MATNo, Brand, CATALOG, SingleUse
                            FROM RankedMatRef
                            WHERE rn = 1
                        ),
                        ComplaintData AS (
                            SELECT 
                                c.CD_Complaint_Country as Country,
                                COUNT(*) as Complaint_Total
                            FROM ComplaintMerged c
                            WHERE c.Brand = '{selected_product_line}'
                            AND c.CD_Date_Complaint_Entry >= '{complaint_start}'
                            AND c.CD_Date_Complaint_Entry <= '{complaint_end}'
                            {f"AND c.Catalog_No = '{selected_catalog}'" if selected_catalog else ""}
                            {get_country_filter('c.CD_Complaint_Country', selected_countries)}
                            AND c.CD_Complaint_Country IS NOT NULL
                            GROUP BY c.CD_Complaint_Country
                        ),
                        ProcedureData AS (
                            SELECT 
                                s.Country_final_dest as Country,
                                SUM(CAST(s.Quantity AS BIGINT)) as Estimated_Procedures
                            FROM Sales s
                            INNER JOIN UniqueMatRef m ON s.Material = m.MATNo
                            WHERE s.[Date] >= '{complaint_start}'
                            AND s.[Date] <= '{complaint_end}'
                            {get_country_filter('s.Country_final_dest', selected_countries)}
                            GROUP BY s.Country_final_dest
                        )
                        SELECT 
                            COALESCE(c.Country, p.Country) as Country,
                            COALESCE(c.Complaint_Total, 0) as Complaint_Total,
                            COALESCE(p.Estimated_Procedures, 0) as Estimated_Procedures,
                            CASE 
                                WHEN COALESCE(p.Estimated_Procedures, 0) = 0 THEN '0.00%'
                                ELSE FORMAT((COALESCE(c.Complaint_Total, 0) * 100.0 / COALESCE(p.Estimated_Procedures, 1)), 'N5') + '%'
                            END as Complaint_Rate
                        FROM ComplaintData c
                        FULL OUTER JOIN ProcedureData p ON c.Country = p.Country
                        WHERE COALESCE(c.Country, p.Country) IS NOT NULL
                        ORDER BY COALESCE(c.Complaint_Total, 0) DESC
                        """
                        
                        # Use retry logic for complaint rates query
                        complaint_rates = execute_query_with_retry(complaint_rates_query)
                        
                        if not complaint_rates.empty:
                            # Title shows the correct date period
                            st.write(f"**Table 8: Complaint Totals and Complaint Rates by Country ({complaint_period_label})**")
                            st.dataframe(complaint_rates)
                            
                            # Chart with date indication
                            complaint_rates_top10 = complaint_rates.nlargest(10, 'Complaint_Total')
                            fig = px.bar(
                                complaint_rates_top10,
                                x='Country',
                                y='Complaint_Total',
                                title=f"{selected_product_line} Complaints by Country - {complaint_period_label} (Top 10)",
                                labels={'Complaint_Total': 'Number of Complaints'}
                            )
                            fig.update_layout(xaxis_tickangle=-45)
                            st.plotly_chart(fig, use_container_width=True)
                        else:
                            st.info("No complaint data found for the selected criteria.")
                        
                        # Complaint Rates by Year
                        # FIX TC2.1.10: Use ROW_NUMBER() to get truly unique MATNo and avoid duplicate counting
                        complaint_rates_by_year_query = f"""
                        WITH RankedMatRef AS (
                            SELECT MATNo, Brand, CATALOG, SingleUse,
                                   ROW_NUMBER() OVER (PARTITION BY MATNo ORDER BY CATALOG) as rn
                            FROM MaterialReference
                            WHERE Brand = '{selected_product_line}'
                            AND SingleUse = 'Y'
                            {f"AND CATALOG = '{selected_catalog}'" if selected_catalog else ""}
                        ),
                        UniqueMatRef AS (
                            SELECT MATNo, Brand, CATALOG, SingleUse
                            FROM RankedMatRef
                            WHERE rn = 1
                        ),
                        ComplaintsByYear AS (
                            SELECT 
                                YEAR(c.CD_Date_Complaint_Entry) as Year_Occurrence,
                                COUNT(*) as Complaint_Total
                            FROM ComplaintMerged c
                            WHERE c.Brand = '{selected_product_line}'
                            AND c.CD_Date_Complaint_Entry >= '{start_date_str}'
                            AND c.CD_Date_Complaint_Entry <= '{end_date_str}'
                            {f"AND c.Catalog_No = '{selected_catalog}'" if selected_catalog else ""}
                            GROUP BY YEAR(c.CD_Date_Complaint_Entry)
                        ),
                        ProceduresByYear AS (
                            SELECT 
                                YEAR(s.[Date]) as Year_Occurrence,
                                SUM(CAST(s.Quantity AS BIGINT)) as Estimated_Procedures
                            FROM Sales s
                            INNER JOIN UniqueMatRef m ON s.Material = m.MATNo
                            WHERE s.[Date] >= '{start_date_str}'
                            AND s.[Date] <= '{end_date_str}'
                            GROUP BY YEAR(s.[Date])
                        )
                        SELECT 
                            COALESCE(c.Year_Occurrence, p.Year_Occurrence) as Year_Occurrence,
                            COALESCE(c.Complaint_Total, 0) as Complaint_Total,
                            COALESCE(p.Estimated_Procedures, 0) as Estimated_Procedures,
                            CASE 
                                WHEN COALESCE(p.Estimated_Procedures, 0) = 0 THEN '0.00%'
                                ELSE FORMAT((COALESCE(c.Complaint_Total, 0) * 100.0 / COALESCE(p.Estimated_Procedures, 1)), 'N4') + '%'
                            END as Complaint_Rate
                        FROM ComplaintsByYear c
                        FULL OUTER JOIN ProceduresByYear p ON c.Year_Occurrence = p.Year_Occurrence
                        WHERE COALESCE(c.Year_Occurrence, p.Year_Occurrence) IS NOT NULL
                        ORDER BY Year_Occurrence
                        """
                        
                        # Use retry logic for complaint rates by year query
                        complaint_rates_by_year = execute_query_with_retry(complaint_rates_by_year_query)
                        
                        if not complaint_rates_by_year.empty:
                            # Format Year_Occurrence as integer without commas
                            complaint_rates_by_year['Year_Occurrence'] = complaint_rates_by_year['Year_Occurrence'].astype(int)
                            
                            st.write(f"**Table 9: Complaint Rates ({start_date.year} – {end_date.year})**")
                            # Change 3: Add thousand separator to Complaint_Total and Estimated_Procedures
                            st.dataframe(complaint_rates_by_year.style.format({
                                'Year_Occurrence': '{:.0f}',
                                'Complaint_Total': '{:,.0f}',
                                'Estimated_Procedures': '{:,.0f}'
                            }))
                            
                            # Create trend chart - use string for x-axis to avoid decimal years
                            complaint_rates_by_year['Year_Occurrence_Str'] = complaint_rates_by_year['Year_Occurrence'].astype(str)
                            
                            fig = px.line(
                                complaint_rates_by_year,
                                x='Year_Occurrence_Str',
                                y='Complaint_Total',
                                title=f"{selected_product_line} Complaint Trends ({start_date.year} - {end_date.year})",
                                labels={'Complaint_Total': 'Number of Complaints', 'Year_Occurrence_Str': 'Year'},
                                markers=True
                            )
                            fig.update_layout(
                                xaxis={'type': 'category'}
                            )
                            st.plotly_chart(fig, use_container_width=True)
                        else:
                            st.info("No complaint rates by year data found for the selected criteria.")
                        
                        # ================================================================
                        # 5. COMPLAINTS PER FINAL OBJECT CODE
                        # ================================================================
                        st.subheader("5. Complaints per Final Object Code")
                        
                        complaints_by_object_code_query = f"""
                        SELECT 
                            TA_Final_object_code_QualityCode as Object_Code,
                            YEAR(CD_Date_Complaint_Entry) as Year,
                            COUNT(*) as Complaint_Count
                        FROM ComplaintMerged
                        WHERE Brand = '{selected_product_line}'
                        AND CD_Date_Complaint_Entry >= '{start_date_str}'
                        AND CD_Date_Complaint_Entry <= '{end_date_str}'
                        {f"AND Catalog_No = '{selected_catalog}'" if selected_catalog else ""}
                        {get_country_filter('CD_Complaint_Country', selected_countries)}
                        AND TA_Final_object_code_QualityCode IS NOT NULL
                        GROUP BY TA_Final_object_code_QualityCode, YEAR(CD_Date_Complaint_Entry)
                        ORDER BY Object_Code, Year
                        """
                        
                        # Use retry logic for complaints by object code query
                        complaints_by_object_code = execute_query_with_retry(complaints_by_object_code_query)
                        
                        if not complaints_by_object_code.empty:
                            # Ensure Year is integer
                            complaints_by_object_code['Year'] = complaints_by_object_code['Year'].astype(int)
                            
                            # Create pivot table
                            object_code_pivot = complaints_by_object_code.pivot_table(
                                index='Object_Code',
                                columns='Year',
                                values='Complaint_Count',
                                fill_value=0
                            )
                            
                            # Get top 10 object codes by total complaints
                            top_object_codes = object_code_pivot.sum(axis=1).nlargest(10).index
                            object_code_pivot_top10 = object_code_pivot.loc[top_object_codes]
                            
                            # Create grouped bar chart
                            fig = go.Figure()
                            colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', '#8c564b', '#e377c2']
                            
                            for i, year in enumerate(sorted(object_code_pivot_top10.columns)):
                                fig.add_trace(go.Bar(
                                    name=str(int(year)),  # Ensure year is displayed as integer
                                    x=object_code_pivot_top10.index,
                                    y=object_code_pivot_top10[year],
                                    marker_color=colors[i % len(colors)]
                                ))
                            
                            fig.update_layout(
                                title=f"{selected_product_line} Complaints per Final Object Code ({start_date.year} - {end_date.year}) - Top 10",
                                xaxis_title="Final Object Code",
                                yaxis_title="Complaint Count",
                                barmode='group',
                                height=500,
                                xaxis={'categoryorder': 'total descending'},
                                xaxis_tickangle=-45
                            )
                            st.plotly_chart(fig, use_container_width=True)
                            
                            st.write(f"**Figure 3: {selected_product_line} Complaints per Final Object Code ({start_date.year} - {end_date.year})**")
                            
                            # Display table
                            object_code_table = object_code_pivot.reset_index()
                            object_code_table.columns.name = None
                            st.dataframe(object_code_table)
                        else:
                            st.info("No complaints by object code data found for the selected criteria.")
                        
                        # ================================================================
                        # 5. QIA ANALYSIS SECTION
                        # ================================================================
                        st.subheader("5. Quality Investigation Analysis (QIA)")
                        
                        # Query QIA data for the selected product line
                        try:
                            # Get QIA data grouped by type and year
                            qia_query = f"""
                            SELECT 
                                QIA_CAPA as Type,
                                Year,
                                COUNT(*) as Count
                            FROM QIAData
                            WHERE ProductLine LIKE '%{selected_product_line}%'
                            AND Year >= 2013 AND Year <= 2023
                            GROUP BY QIA_CAPA, Year
                            ORDER BY QIA_CAPA, Year
                            """
                            qia_data = execute_query_with_retry(qia_query)
                            
                            # Get RMDocType data for HRA, SEA, RM Memo, PSRA Reference
                            rm_doc_query = f"""
                            SELECT 
                                RMDocType as Type,
                                Year,
                                COUNT(*) as Count
                            FROM QIAData
                            WHERE ProductLine LIKE '%{selected_product_line}%'
                            AND Year >= 2013 AND Year <= 2023
                            AND RMDocType IS NOT NULL
                            AND RMDocType != 'N/A'
                            GROUP BY RMDocType, Year
                            ORDER BY RMDocType, Year
                            """
                            rm_doc_data = execute_query_with_retry(rm_doc_query)
                            
                            if not qia_data.empty or not rm_doc_data.empty:
                                # Create Table 15: QI/CAPA & HRA/RM Memo Totals
                                st.write(f"**Table 15: QI/CAPA & HRA/RM Memo Totals 2013 - 2023**")
                                
                                # Pivot QIA data for table display
                                years = list(range(2013, 2024))
                                qia_types = ['CAPA', 'QI', 'PA']
                                rm_types = ['HRA', 'SEA', 'RM Memo', 'PSRA Reference']
                                
                                # Create the QIA pivot table
                                if not qia_data.empty:
                                    qia_pivot = qia_data.pivot_table(
                                        index='Type',
                                        columns='Year',
                                        values='Count',
                                        fill_value=0,
                                        aggfunc='sum'
                                    ).reset_index()
                                    
                                    # Ensure all years are present
                                    for year in years:
                                        if year not in qia_pivot.columns:
                                            qia_pivot[year] = 0
                                    
                                    # Reorder columns
                                    qia_pivot = qia_pivot[['Type'] + years]
                                else:
                                    qia_pivot = pd.DataFrame(columns=['Type'] + years)
                                
                                # Create the RM Doc pivot table
                                if not rm_doc_data.empty:
                                    rm_pivot = rm_doc_data.pivot_table(
                                        index='Type',
                                        columns='Year',
                                        values='Count',
                                        fill_value=0,
                                        aggfunc='sum'
                                    ).reset_index()
                                    
                                    # Ensure all years are present
                                    for year in years:
                                        if year not in rm_pivot.columns:
                                            rm_pivot[year] = 0
                                    
                                    # Reorder columns
                                    rm_pivot = rm_pivot[['Type'] + years]
                                else:
                                    rm_pivot = pd.DataFrame(columns=['Type'] + years)
                                
                                # Combine both tables
                                combined_table = pd.concat([qia_pivot, rm_pivot], ignore_index=True)
                                
                                # Rename columns to display format
                                year_columns = {year: str(year) for year in years}
                                combined_table = combined_table.rename(columns=year_columns)
                                combined_table = combined_table.rename(columns={'Type': selected_product_line})
                                
                                st.dataframe(combined_table, use_container_width=True)
                                
                                # Chart 1: QIA Breakdown (QI, CAPA, PA) - Stacked Bar Chart
                                st.write(f"### {selected_product_line} QIA Breakdown 2013 - 2023")
                                
                                if not qia_data.empty:
                                    # Filter for QI, CAPA, PA only
                                    qia_chart_data = qia_data[qia_data['Type'].isin(['QI', 'CAPA', 'PA'])].copy()
                                    
                                    if not qia_chart_data.empty:
                                        fig_qia = px.bar(
                                            qia_chart_data,
                                            x='Year',
                                            y='Count',
                                            color='Type',
                                            barmode='stack',
                                            title=f"{selected_product_line} QIA Breakdown 2013 - 2023",
                                            labels={'Count': 'Count', 'Year': 'Year', 'Type': 'Type'},
                                            color_discrete_map={'QI': '#4472C4', 'CAPA': '#C65911', 'PA': '#A9D18E'}
                                        )
                                        fig_qia.update_layout(
                                            xaxis_tickangle=0,
                                            xaxis=dict(tickmode='linear', dtick=1),
                                            legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='center', x=0.5)
                                        )
                                        st.plotly_chart(fig_qia, use_container_width=True)
                                    else:
                                        st.info("No QI/CAPA/PA data available for the selected product line.")
                                else:
                                    st.info("No QIA data available for the selected product line.")
                                
                                # Chart 2: HRA/Memo/PSRA Breakdown - Stacked Bar Chart
                                st.write(f"### {selected_product_line} HRA / Memo / PSRA Breakdown 2013 - 2023")
                                
                                if not rm_doc_data.empty:
                                    fig_rm = px.bar(
                                        rm_doc_data,
                                        x='Year',
                                        y='Count',
                                        color='Type',
                                        barmode='stack',
                                        title=f"{selected_product_line} HRA / Memo / PSRA Breakdown 2013 - 2023",
                                        labels={'Count': 'Count', 'Year': 'Year', 'Type': 'Type'},
                                        color_discrete_map={
                                            'HRA': '#4472C4', 
                                            'SEA': '#C65911', 
                                            'RM Memo': '#A9D18E',
                                            'PSRA Reference': '#7030A0',
                                            'PM Benefit-Risk Assessment': '#00B0F0'
                                        }
                                    )
                                    fig_rm.update_layout(
                                        xaxis_tickangle=0,
                                        xaxis=dict(tickmode='linear', dtick=1),
                                        legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='center', x=0.5)
                                    )
                                    st.plotly_chart(fig_rm, use_container_width=True)
                                else:
                                    st.info("No HRA/Memo/PSRA data available for the selected product line.")
                                    
                            else:
                                st.info(f"No QIA data found for {selected_product_line}.")
                                
                        except Exception as qia_error:
                            st.warning(f"Unable to load QIA data: {str(qia_error)}")
                        
                        # ================================================================
                        # DOWNLOAD SECTION
                        # ================================================================
                        st.subheader("📋 Download Complete Report")
                        
                        # Prepare all data for Excel export
                        report_data = {}
                        
                        if not sales_by_country.empty:
                            report_data['Sales by Country'] = sales_pivot_country if 'sales_pivot_country' in locals() else sales_by_country
                            if 'sales_pivot_region' in locals():
                                report_data['Sales by Region'] = sales_pivot_region
                        if not adverse_events.empty:
                            report_data['Adverse Events by Year'] = ae_table if 'ae_table' in locals() else adverse_events
                        if not ae_by_country.empty:
                            report_data['Adverse Events by Country'] = ae_country_table if 'ae_country_table' in locals() else ae_by_country
                        try:
                            if 'recalls_data' in locals() and not recalls_data.empty:
                                report_data['Recalls'] = recalls_data
                        except:
                            pass
                        if not complaint_rates.empty:
                            report_data['Complaints by Country'] = complaint_rates
                        if not complaint_rates_by_year.empty:
                            report_data['Complaints by Year'] = complaint_rates_by_year
                        if not complaints_by_object_code.empty:
                            report_data['Complaints by Object Code'] = object_code_table if 'object_code_table' in locals() else complaints_by_object_code
                        
                        if report_data:
                            # Create Excel file with multiple sheets
                            output = BytesIO()
                            try:
                                with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                                    # Write summary sheet
                                    summary_data = {
                                        'Parameter': ['Product Line', 'Start Date', 'End Date', 'Catalog', 'Countries', 'Last Full Year', 'Current Year'],
                                        'Value': [
                                            selected_product_line,
                                            start_date_str,
                                            end_date_str, 
                                            selected_catalog if selected_catalog else 'All',
                                            ', '.join(selected_countries) if selected_countries else 'All',
                                            str(LAST_FULL_YEAR),
                                            str(CURRENT_YEAR)
                                        ]
                                    }
                                    pd.DataFrame(summary_data).to_excel(writer, sheet_name='Report Parameters', index=False)
                                    
                                    # Write all data sheets
                                    for sheet_name, data in report_data.items():
                                        # Ensure sheet name is valid (max 31 characters)
                                        clean_sheet_name = sheet_name[:31]
                                        data.to_excel(writer, sheet_name=clean_sheet_name, index=False)
                                
                                file_data = output.getvalue()
                                
                                st.download_button(
                                    label="📥 Download Complete PSUR Report (Excel)",
                                    data=file_data,
                                    file_name=f'PSUR_Report_{selected_product_line}_{start_date.strftime("%Y%m%d")}_to_{end_date.strftime("%Y%m%d")}.xlsx',
                                    mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
                                )
                            except ImportError:
                                st.error("xlsxwriter not available. Cannot create Excel file.")
                        else:
                            st.info("No data available to download.")
                        
                        # Summary statistics
                        st.subheader("📊 Report Summary")
                        
                        col1, col2, col3, col4 = st.columns(4)
                        
                        with col1:
                            total_sales = sales_by_country['TotalQuantity'].sum() if not sales_by_country.empty else 0
                            st.metric("Total Sales", f"{total_sales:,}")
                        
                        with col2:
                            total_adverse_events = adverse_events['EventCount'].sum() if not adverse_events.empty else 0
                            st.metric("Total Adverse Events", total_adverse_events)
                        
                        with col3:
                            total_complaints = complaint_rates['Complaint_Total'].sum() if not complaint_rates.empty else 0
                            st.metric(f"Total Complaints ({complaint_period_label})", total_complaints)
                        
                        with col4:
                            total_recalls = len(recalls_data) if 'recalls_data' in locals() and not recalls_data.empty else 0
                            st.metric("Total Recalls", total_recalls)
                        
                        st.success("✅ PSUR report generated successfully!")
                        
                except Exception as e:
                    st.error(f"❌ Error generating report: {str(e)}")
                    st.write("**Debug Information:**")
                    st.write(f"Selected product line: {selected_product_line}")
                    st.write(f"Date range: {start_date_str} to {end_date_str}")
                    st.write(f"Error details: {str(e)}")
                    
                    # Show the problematic query if available
                    if 'e' in locals():
                        st.write("**Error occurred during report generation. Please check:**")
                        st.write("1. Database connection is still active")
                        st.write("2. Selected product line exists in the database")
                        st.write("3. Date range contains valid data")  
                        st.write("4. Database tables have the expected structure")
    
    # Tab 3: Risk Calculation
    with tab3:
        st.header("Risk Calculation")
        
        # Functions to get data for risk calculation
        @st.cache_data
        def get_risk_product_lines():
            try:
                query = """
                SELECT DISTINCT Brand FROM MaterialReference
                WHERE Brand IS NOT NULL
                ORDER BY Brand
                """
                df = pd.read_sql(query, st.session_state['conn'])
                return df['Brand'].tolist()
            except Exception as e:
                st.error(f"Error retrieving product lines: {str(e)}")
                return []
        
        @st.cache_data
        def get_hhi_value(product_line):
            try:
                # Change 4: Fixed column name from HHI_Reference to [HHI Reference] (with space)
                query = f"""
                SELECT HHI FROM HHI_Lookup
                WHERE [HHI Reference] = '{product_line}'
                """
                df = pd.read_sql(query, st.session_state['conn'])
                if not df.empty:
                    return df['HHI'].iloc[0]
                return None
            except Exception as e:
                st.error(f"Error retrieving HHI value: {str(e)}")
                return None
        
        def get_p0_value(product_line, numeric_value):
            """
            Calculate P0 based on product line and numeric value
            Based on the frequency definitions table
            """
            # P0 mapping based on Brand and Numeric Value
            p0_mapping = {
                'Arterion, Avanta, MRXP, ProVis, Salient, Vistron Plus': {
                    '10^-3': 'Improbable',
                    '10^-4': 'Remote',
                    '10^-5': 'Occasional',
                    '10^-6': 'Probable',
                    '10^-7': 'Frequent'
                },
                'Centargo, Envision/Vistron, Intego, SSEP, Stellant, Stellant Flex, Stellant MP, Universal Disposables': {
                    '10^-4': 'Frequent',
                    '10^-5': 'Probable',
                    '10^-6': 'Occasional',
                    '10^-7': 'Remote',
                    '10^-8': 'Improbable'
                }
            }
            
            # Check if product line matches any of the categories
            for brand_group, value_map in p0_mapping.items():
                if product_line in brand_group or product_line in brand_group.split(', '):
                    return value_map.get(numeric_value, 'Unknown')
            
            # Default for other products
            return 'Unknown'
        
        def get_p1_classification(p1_numeric, product_line):
            """
            Classify P1 based on P1 numeric value and product line
            Based on the frequency definitions mapping table from Excel
            
            Arterion group (Value of Frequent: 1x10^-3):
              - Frequent: > 10^-3
              - Probable: > 10^-4 and <= 10^-3
              - Occasional: > 10^-5 and <= 10^-4
              - Remote: > 10^-6 and <= 10^-5
              - Improbable: <= 10^-6
            
            Centargo group (Value of Frequent: 1x10^-4):
              - Frequent: > 10^-4
              - Probable: > 10^-5 and <= 10^-4
              - Occasional: > 10^-6 and <= 10^-5
              - Remote: > 10^-7 and <= 10^-6
              - Improbable: <= 10^-7
            """
            # Product groupings and their P1 thresholds
            arterion_products = ['Arterion', 'Avanta', 'MRXP', 'ProVis', 'Salient', 'Vistron Plus']
            centargo_products = ['Centargo', 'Envision', 'Vistron', 'Intego', 'SSEP', 'Stellant', 
                               'Stellant Flex', 'Stellant MP', 'Universal Disposables']
            
            # Check if product is in Arterion group
            is_arterion = any(prod in product_line for prod in arterion_products)
            
            # Check if product is in Centargo group
            is_centargo = any(prod in product_line for prod in centargo_products)
            
            if is_arterion:
                # Arterion group thresholds (Value of Frequent: 1x10^-3)
                if p1_numeric > 1e-3:
                    return 'Frequent'
                elif p1_numeric > 1e-4:
                    return 'Probable'
                elif p1_numeric > 1e-5:
                    return 'Occasional'
                elif p1_numeric > 1e-6:
                    return 'Remote'
                else:  # <= 10^-6
                    return 'Improbable'
            elif is_centargo:
                # Centargo group thresholds (Value of Frequent: 1x10^-4)
                if p1_numeric > 1e-4:
                    return 'Frequent'
                elif p1_numeric > 1e-5:
                    return 'Probable'
                elif p1_numeric > 1e-6:
                    return 'Occasional'
                elif p1_numeric > 1e-7:
                    return 'Remote'
                else:  # <= 10^-7
                    return 'Improbable'
            else:
                # Default classification for other products (use Centargo thresholds)
                if p1_numeric > 1e-4:
                    return 'Frequent'
                elif p1_numeric > 1e-5:
                    return 'Probable'
                elif p1_numeric > 1e-6:
                    return 'Occasional'
                elif p1_numeric > 1e-7:
                    return 'Remote'
                else:
                    return 'Improbable'
        
        @st.cache_data
        def get_p2_lookup_values(product_line, hazard_severity_pairs):
            """
            Get P2 values from HHISummary table for given hazard-severity combinations
            Uses HHI_Reference = product_line OR HHI code to match
            
            Product to HHI Reference mapping:
            - Centargo uses 'Centargo' or 'CT'
            - Arterion uses 'CV'
            - etc.
            
            Args:
                product_line: The product line (e.g., 'Centargo', 'Arterion')
                hazard_severity_pairs: List of (hazard, severity) tuples
            
            Returns:
                Dictionary mapping (hazard, severity) to P2 value
            """
            try:
                if not hazard_severity_pairs or len(hazard_severity_pairs) == 0:
                    return {}
                
                # Get HHI code for product
                hhi_code = get_hhi_value(product_line)
                
                # Determine which HHI_Reference to query
                # Some products use product name, others use HHI code
                query_refs = []
                if product_line:
                    query_refs.append(product_line)
                if hhi_code:
                    query_refs.append(hhi_code)
                
                if not query_refs:
                    return {}
                
                # Build query with all possible HHI_References
                refs_str = "', '".join(query_refs)
                query = f"""
                SELECT 
                    [Devacom_Hazard] as Hazard,
                    [Severity],
                    [P2]
                FROM [dbo].[HHISummary]
                WHERE [HHI_Reference] IN ('{refs_str}')
                """
                df = pd.read_sql(query, st.session_state['conn'])
                
                # Create dictionary mapping (hazard, severity) to P2
                p2_dict = {}
                for _, row in df.iterrows():
                    hazard = row['Hazard']
                    severity = row['Severity']
                    p2 = row['P2']
                    if pd.notna(severity):
                        # Store with actual hazard value
                        if pd.notna(hazard):
                            p2_dict[(hazard, severity)] = p2
                        # Also store with None hazard for fallback matching
                        if pd.isna(hazard) or hazard == '':
                            p2_dict[(None, severity)] = p2
                
                return p2_dict
            except Exception as e:
                st.warning(f"Error retrieving P2 values: {str(e)}")
                return {}
        
        def get_probability_of_occurrence_of_harm(p1_prob, p2):
            """
            Calculate Probability of Occurrence of harm based on P1 and P2
            """
            if pd.isna(p2) or p2 == "N/A" or p2 == "":
                return "N/A"
            if pd.isna(p1_prob) or p1_prob == "" or p1_prob == "N/A":
                return "N/A"
            if p1_prob == "Improbable":
                return "Improbable"
            
            # Remote cases
            if p1_prob == "Remote":
                if p2 in ["Certain", "Likely"]:
                    return "Remote"
                elif p2 in ["Possible", "Unlikely", "Will Not Occur"]:
                    return "Improbable"
            
            # Occasional cases
            elif p1_prob == "Occasional":
                if p2 == "Certain":
                    return "Occasional"
                elif p2 in ["Likely", "Possible"]:
                    return "Remote"
                elif p2 in ["Unlikely", "Will Not Occur"]:
                    return "Improbable"
            
            # Probable cases
            elif p1_prob == "Probable":
                if p2 == "Certain":
                    return "Probable"
                elif p2 in ["Likely", "Possible"]:
                    return "Occasional"
                elif p2 == "Unlikely":
                    return "Remote"
                elif p2 == "Will Not Occur":
                    return "Improbable"
            
            # Frequent cases
            elif p1_prob == "Frequent":
                if p2 == "Certain":
                    return "Frequent"
                elif p2 == "Likely":
                    return "Probable"
                elif p2 == "Possible":
                    return "Occasional"
                elif p2 == "Unlikely":
                    return "Remote"
                elif p2 == "Will Not Occur":
                    return "Improbable"
            
            return "Error"
        
        def get_risk_level(p1_prob, severity, prob_occurrence_harm):
            """
            Calculate Risk Level based on P1, Severity, and Probability of Occurrence of harm
            Change 5: If P2 and/or Probability of Occurrence of harm are N/A, make Risk Level N/A
            """
            if p1_prob == "Error":
                return "Error"
            if pd.isna(severity) or severity == "":
                return ""
            if severity == "NAHC":
                return "N/A"
            if severity == "No Safety Impact":
                return "N/A"
            # Change 5: If Probability of Occurrence of harm is N/A, return N/A for Risk Level
            if prob_occurrence_harm == "N/A" or pd.isna(prob_occurrence_harm) or prob_occurrence_harm == "":
                return "N/A"
            
            # Negligible cases
            if severity == "Negligible":
                if prob_occurrence_harm == "Frequent":
                    return "Medium"
                else:
                    return "Low"
            
            # Minor cases
            elif severity == "Minor":
                if prob_occurrence_harm == "Frequent":
                    return "High"
                elif prob_occurrence_harm in ["Probable", "Occasional"]:
                    return "Medium"
                else:
                    return "Low"
            
            # Moderate cases
            elif severity == "Moderate":
                if prob_occurrence_harm in ["Occasional", "Remote"]:
                    return "Medium"
                elif prob_occurrence_harm == "Improbable":
                    return "Low"
                else:
                    return "High"
            
            # Critical cases
            elif severity == "Critical":
                if prob_occurrence_harm == "Remote":
                    return "Medium"
                elif prob_occurrence_harm == "Improbable":
                    return "Low"
                else:
                    return "High"
            
            # Catastrophic cases
            elif severity == "Catastrophic":
                if prob_occurrence_harm == "Improbable":
                    return "Medium"
                else:
                    return "High"
            
            return "High"
        
        def get_total_procedures(product_line, start_date_str=None, end_date_str=None):
            """
            Calculate total procedures for the product line within date range
            If no dates provided, calculates across all time
            """
            try:
                date_filter = ""
                if start_date_str and end_date_str:
                    date_filter = f"AND s.[Date] >= '{start_date_str}' AND s.[Date] <= '{end_date_str}'"
                
                query = f"""
                SELECT 
                    SUM(CAST(s.Quantity AS BIGINT)) as Total_Procedures
                FROM Sales s
                INNER JOIN MaterialReference m ON s.Material = m.MATNo
                WHERE m.Brand = '{product_line}'
                {date_filter}
                """
                df = pd.read_sql(query, st.session_state['conn'])
                if not df.empty and df['Total_Procedures'].iloc[0] is not None:
                    return int(df['Total_Procedures'].iloc[0])
                return 0
            except Exception as e:
                st.error(f"Error calculating total procedures: {str(e)}")
                return 0
        
        @st.cache_data
        def get_risk_calculation_data(product_line, start_date_str, end_date_str):
            """
            Get risk calculation data from ComplaintMerged table
            Maps TA_Final_Risk_Classification to Severity:
              - Risk category I -> Catastrophic
              - Risk category II -> Critical
              - Risk category III -> Moderate
              - Risk category IV -> Minor
              - Risk category V -> Negligible
            Uses TA_Final_error_code_RiskCodes as Hazard
            """
            try:
                query = f"""
                SELECT 
                    TA_Final_object_code_QualityCode as Object_Code,
                    TA_Final_error_code_QualityCode as Error_code,
                    TA_Final_error_subcode_QualityCode as Error_Subcode,
                    TA_Final_error_code_RiskCodes as Hazard,
                    CASE 
                        WHEN TA_Final_Risk_Classification = 'Risk category I' THEN 'Catastrophic'
                        WHEN TA_Final_Risk_Classification = 'Risk category II' THEN 'Critical'
                        WHEN TA_Final_Risk_Classification = 'Risk category III' THEN 'Moderate'
                        WHEN TA_Final_Risk_Classification = 'Risk category IV' THEN 'Minor'
                        WHEN TA_Final_Risk_Classification = 'Risk category V' THEN 'Negligible'
                        ELSE TA_Final_Risk_Classification
                    END as Severity,
                    COUNT(*) as Total_Complaints
                FROM [dbo].[ComplaintMerged]
                WHERE [Brand] = '{product_line}'
                AND [CD_Date_Complaint_Entry] >= '{start_date_str}'
                AND [CD_Date_Complaint_Entry] <= '{end_date_str}'
                GROUP BY 
                    TA_Final_object_code_QualityCode,
                    TA_Final_error_code_QualityCode,
                    TA_Final_error_subcode_QualityCode,
                    TA_Final_error_code_RiskCodes,
                    CASE 
                        WHEN TA_Final_Risk_Classification = 'Risk category I' THEN 'Catastrophic'
                        WHEN TA_Final_Risk_Classification = 'Risk category II' THEN 'Critical'
                        WHEN TA_Final_Risk_Classification = 'Risk category III' THEN 'Moderate'
                        WHEN TA_Final_Risk_Classification = 'Risk category IV' THEN 'Minor'
                        WHEN TA_Final_Risk_Classification = 'Risk category V' THEN 'Negligible'
                        ELSE TA_Final_Risk_Classification
                    END
                ORDER BY TA_Final_object_code_QualityCode, TA_Final_error_code_QualityCode, 
                         TA_Final_error_subcode_QualityCode, TA_Final_error_code_RiskCodes
                """
                df = pd.read_sql(query, st.session_state['conn'])
                return df
            except Exception as e:
                st.error(f"Error retrieving risk calculation data: {str(e)}")
                st.write(f"Debug info: {str(e)}")
                return pd.DataFrame()
        
        # UI for Risk Calculation
        st.subheader("Risk Assessment Parameters")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Get product lines
            risk_product_lines = get_risk_product_lines()
            
            # Set Centargo as default
            default_risk_product = "Centargo" if "Centargo" in risk_product_lines else (risk_product_lines[0] if risk_product_lines else None)
            default_risk_index = risk_product_lines.index(default_risk_product) if default_risk_product in risk_product_lines else 0
            
            selected_risk_product = st.selectbox(
                "Product Line (Required)", 
                risk_product_lines, 
                index=default_risk_index,
                key="risk_product_line"
            )
            
            # Date selectors - Default to 2022-01-01 to 2024-12-31 to match Excel validation
            today = datetime.datetime.now().date()
            risk_start_date = st.date_input(
                "Start Date (Required)", 
                value=datetime.date(2022, 1, 1),
                key="risk_start_date"
            )
            risk_end_date = st.date_input(
                "End Date (Required)", 
                value=datetime.date(2024, 12, 31),
                key="risk_end_date"
            )
        
        with col2:
            st.info("""
            ### Risk Calculation Information
            
            This tool calculates risk assessment for selected product lines based on:
            - Complaint frequency data within date range
            - Total procedures performed
            - Hazard severity classifications
            - Probability of occurrence (P1 classification)
            
            Select a product line and date range to view risk calculations.
            """)
        
        # Add Calculate Risk button for explicit trigger
        calculate_button = st.button("🔄 Calculate Risk Assessment", type="primary", key="calc_risk_btn")
        
        # Calculate and display results
        if selected_risk_product and calculate_button:
            st.subheader("Risk Assessment Results")
            
            # Convert dates to strings
            risk_start_date_str = risk_start_date.strftime('%Y-%m-%d')
            risk_end_date_str = risk_end_date.strftime('%Y-%m-%d')
            
            # Get HHI value
            hhi_value = get_hhi_value(selected_risk_product)
            
            # Get Total Procedures - now filtered by date range
            total_procedures = get_total_procedures(selected_risk_product, risk_start_date_str, risk_end_date_str)
            
            # Display summary metrics
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric("Product", selected_risk_product)
            
            with col2:
                st.metric("HHI", hhi_value if hhi_value else "N/A")
            
            with col3:
                st.metric("Total Procedures", f"{total_procedures:,}")
            
            # Get risk calculation data
            with st.spinner("Calculating risk assessment..."):
                risk_data = get_risk_calculation_data(selected_risk_product, risk_start_date_str, risk_end_date_str)
                
                if not risk_data.empty:
                    # Calculate P1 numeric and P1 classification for each row
                    if total_procedures > 0:
                        risk_data['P1'] = risk_data['Total_Complaints'] / total_procedures
                        risk_data['P1_Probability_of_Occurrence'] = risk_data['P1'].apply(
                            lambda x: get_p1_classification(x, selected_risk_product)
                        )
                    else:
                        risk_data['P1'] = 0
                        risk_data['P1_Probability_of_Occurrence'] = 'N/A'
                    
                    # Format P1 as scientific notation
                    risk_data['P1_Formatted'] = risk_data['P1'].apply(lambda x: f"{x:.2e}" if x > 0 else "0.00e+00")
                    
                    # Handle potential None/NaN values in Hazard and Severity columns
                    risk_data['Hazard_Clean'] = risk_data['Hazard'].fillna('').astype(str)
                    risk_data['Severity_Clean'] = risk_data['Severity'].fillna('').astype(str)
                    
                    # Create HHI-Hazard-Severity column for display
                    # Handle case where hhi_value is None (for products not in HHI_Lookup table)
                    hhi_str = str(hhi_value) if hhi_value is not None else ""
                    
                    # Build HHI-Hazard-Severity key safely
                    risk_data['HHI-Hazard-Severity'] = risk_data.apply(
                        lambda row: f"{hhi_str}{row['Hazard_Clean']}{row['Severity_Clean']}", axis=1
                    )
                    
                    # Get unique hazard-severity pairs for P2 lookup
                    hazard_severity_pairs = list(set(
                        zip(risk_data['Hazard_Clean'].tolist(), risk_data['Severity_Clean'].tolist())
                    ))
                    
                    # Get P2 values from HHISummary table using new function
                    p2_lookup = get_p2_lookup_values(selected_risk_product, hazard_severity_pairs)
                    
                    # Map P2 values using (hazard, severity) tuple lookup
                    def get_p2_for_row(row):
                        hazard = row['Hazard_Clean']
                        severity = row['Severity_Clean']
                        
                        # Try exact match first
                        if (hazard, severity) in p2_lookup:
                            return p2_lookup[(hazard, severity)]
                        
                        # Try with None hazard (for rows where hazard is empty/null)
                        if (None, severity) in p2_lookup:
                            return p2_lookup[(None, severity)]
                        
                        # Default values based on severity
                        if severity == 'Negligible':
                            return 'Likely'
                        elif severity in ['Minor', 'Moderate', 'Critical', 'Catastrophic']:
                            return 'N/A'
                        
                        return 'N/A'
                    
                    risk_data['P2'] = risk_data.apply(get_p2_for_row, axis=1)
                    
                    # Restore original Hazard and Severity columns (with NaN displayed properly)
                    risk_data['Hazard'] = risk_data['Hazard'].replace('', pd.NA)
                    risk_data['Severity'] = risk_data['Severity'].replace('', pd.NA)
                    
                    # Calculate Probability of Occurrence of harm
                    risk_data['Probability_of_Occurrence_of_harm'] = risk_data.apply(
                        lambda row: get_probability_of_occurrence_of_harm(
                            row['P1_Probability_of_Occurrence'], 
                            row['P2']
                        ), axis=1
                    )
                    
                    # Calculate Risk Level
                    risk_data['Risk_Level'] = risk_data.apply(
                        lambda row: get_risk_level(
                            row['P1_Probability_of_Occurrence'],
                            row['Severity'],
                            row['Probability_of_Occurrence_of_harm']
                        ), axis=1
                    )
                    
                    # Add Product Line column
                    risk_data['Product_Line'] = selected_risk_product
                    
                    # Display the results table
                    st.write(f"**Risk Assessment Table for {selected_risk_product}**")
                    
                    # Reorder columns for better display
                    display_columns = [
                        'Object_Code',
                        'Error_code',
                        'Error_Subcode',
                        'Hazard',
                        'Severity',
                        'Total_Complaints',
                        'P1_Formatted',
                        'P1_Probability_of_Occurrence',
                        'HHI-Hazard-Severity',
                        'P2',
                        'Probability_of_Occurrence_of_harm',
                        'Risk_Level',
                        'Product_Line'
                    ]
                    
                    # Filter to only existing columns
                    available_columns = [col for col in display_columns if col in risk_data.columns]
                    display_df = risk_data[available_columns].copy()
                    
                    # Rename columns for better presentation
                    display_df = display_df.rename(columns={
                        'P1_Formatted': 'P1',
                        'P1_Probability_of_Occurrence': 'P1 - Probability of Occurrence',
                        'Probability_of_Occurrence_of_harm': 'Probability of Occurrence of harm'
                    })
                    
                    st.dataframe(display_df, use_container_width=True)
                    
                    # Download button for risk assessment
                    try:
                        output = BytesIO()
                        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                            # Write summary sheet
                            summary_data = {
                                'Parameter': ['Product Line', 'HHI', 'Total Procedures', 'Start Date', 'End Date'],
                                'Value': [
                                    selected_risk_product,
                                    hhi_value if hhi_value else 'N/A',
                                    total_procedures,
                                    risk_start_date_str,
                                    risk_end_date_str
                                ]
                            }
                            pd.DataFrame(summary_data).to_excel(writer, sheet_name='Summary', index=False)
                            
                            # Write risk calculation data
                            display_df.to_excel(writer, sheet_name='Risk Assessment', index=False)
                        
                        excel_data = output.getvalue()
                        
                        st.download_button(
                            label="📥 Download Risk Assessment (Excel)",
                            data=excel_data,
                            file_name=f'Risk_Assessment_{selected_risk_product}_{datetime.datetime.now().strftime("%Y%m%d")}.xlsx',
                            mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
                        )
                    except Exception as e:
                        st.warning(f"Excel export not available: {str(e)}")
                    
                else:
                    st.info(f"No risk calculation data found for {selected_risk_product}. This could mean:")
                    st.write("- No complaints have been recorded for this product in the selected date range")
                    st.write("- The product is not in the ComplaintMerged table")
                    st.write("- There is a mismatch in product naming between tables (Brand column)")
                    st.write("")
                    st.write("**Debug: Check if data exists with this query:**")
                    st.code(f"SELECT COUNT(*) FROM ComplaintMerged WHERE Brand = '{selected_risk_product}' AND CD_Date_Complaint_Entry >= '{risk_start_date_str}' AND CD_Date_Complaint_Entry <= '{risk_end_date_str}'")

    # Sidebar with logout and connection info
    with st.sidebar:
        st.write("### Connection Status")
        st.success("✅ Connected to Azure SQL Database")
        st.write(f"**Server:** ph-radc-server-eastus.database.windows.net")
        st.write(f"**Database:** azure-db-radcommercial")
        
        st.write("### Configuration")
        st.write(f"📅 Last Full Year: **{LAST_FULL_YEAR}**")
        st.write(f"📅 Current Year: **{CURRENT_YEAR}**")
        
        st.write("### Available Tables")
        try:
            tables_df = get_tables()
            st.write(f"📊 {len(tables_df)} tables available")
            with st.expander("View Tables"):
                for table in tables_df['TABLE_NAME'].tolist():
                    st.write(f"• {table}")
        except:
            st.write("Unable to retrieve table list")
        
        st.write("### Report Features")
        st.write("✅ Sales by Country (Chart 1)")
        st.write("✅ Sales by Region (Chart 2)")
        st.write("✅ Adverse Events")
        st.write("✅ Field Notices/Recalls")
        st.write("✅ Complaint Analysis")
        st.write("✅ QIA Analysis (Table 15)")
        st.write("✅ Interactive Visualizations")
        st.write("✅ Excel Export")
        
        if st.button("🔓 Logout", type="secondary"):
            st.session_state['logged_in'] = False
            st.session_state['conn'] = None
            st.rerun()