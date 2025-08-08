import streamlit as st
import pandas as pd
import pyodbc
import datetime
import plotly.express as px
import plotly.graph_objects as go
from io import BytesIO
import numpy as np
import importlib

# Check if pymssql is available as an alternative
try:
    pymssql_spec = importlib.util.find_spec("pymssql")
    pymssql_available = pymssql_spec is not None
except ImportError:
    pymssql_available = False

if pymssql_available:
    import pymssql

# Set page config
st.set_page_config(
    page_title="PSUR Utilization Analytics",
    page_icon="📊",
    layout="wide"
)

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

# Function to connect to Azure SQL
def connect_to_azure_sql(username, password, server="ph-radc-server-eastus.database.windows.net", database="azure-db-radcommercial"):
    try:
        # Log connection attempt
        st.info(f"Attempting to connect to server: {server}")
        
        # Create connection string with ODBC Driver 17 (updated from 13)
        conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER=tcp:{server},1433;DATABASE={database};UID={username};PWD={password};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
        
        # Attempt connection
        conn = pyodbc.connect(conn_str)
        return conn
    except pyodbc.Error as e:
        st.error(f"ODBC Connection Error: {str(e)}")
        
        # Try pymssql as fallback
        try:
            st.info("Trying pymssql as fallback...")
            if pymssql_available:
                import pymssql
                conn = pymssql.connect(
                    server=server.replace('.database.windows.net', ''),
                    user=username,
                    password=password,
                    database=database
                )
                st.success("Connected using pymssql!")
                return conn
            else:
                st.error("pymssql not available as fallback")
        except Exception as pymssql_error:
            st.error(f"pymssql connection also failed: {str(pymssql_error)}")
        
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
                    st.experimental_rerun()
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
    tab1, tab2 = st.tabs(["Data Explorer", "PSUR Report Generator"])
    
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
            selected_product_line = st.selectbox("Product Line (Required)", [""] + product_lines)
            
            # Date selectors
            today = datetime.datetime.now().date()
            start_date = st.date_input("Start Date (Required)", value=today - datetime.timedelta(days=730))
            end_date = st.date_input("End Date (Required)", value=today)
            
            # Optional fields
            selected_catalog = st.selectbox("Catalog (Optional)", [""] + catalogs)
            selected_country = st.selectbox("Country (Optional)", [""] + countries)
        
        with col2:
            st.info("""
            ### PSUR Report Information
            
            This report will include:
            - Sales by Country
            - Adverse Events Analysis
            - Field Notices / Recalls
            - Customer Complaint / User Feedback Review
            - Complaints per Final Object Code
            
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
                if selected_country and selected_country != "":
                    st.write(f"- Country: {selected_country}")
                
                try:
                    with st.spinner("Generating PSUR report..."):
                        # Convert dates to strings in SQL format
                        start_date_str = start_date.strftime('%Y-%m-%d')
                        end_date_str = end_date.strftime('%Y-%m-%d')
                        
                        # Helper function to create country filter for SQL queries
                        def get_country_filter(column_name, selected_country):
                            if not selected_country:
                                return ""
                            
                            # Create a list of possible country variations to match
                            country_variations = [selected_country]
                            
                            # Add reverse mappings for common cases
                            reverse_mapping = {
                                'United States': ['US', 'USA', 'United States of America'],
                                'United Kingdom': ['UK', 'Britain', 'Great Britain'],
                                'Germany': ['Deutschland', 'DE'],
                                'Netherlands': ['Holland', 'NL']
                            }
                            
                            if selected_country in reverse_mapping:
                                country_variations.extend(reverse_mapping[selected_country])
                            
                            # Create OR condition for all variations
                            conditions = [f"{column_name} = '{var}'" for var in country_variations]
                            return f"AND ({' OR '.join(conditions)})"
                        
                        # 1. SALES BY COUNTRY
                        st.subheader("1. Sales by Country")
                        
                        # Query for Sales by Country with product breakdown
                        sales_query = f"""
                        WITH SalesByCatalog AS (
                            SELECT 
                                s.Country_final_dest,
                                m.ProductGroup,
                                SUM(s.Quantity) as TotalQuantity
                            FROM Sales s
                            LEFT JOIN MaterialReference m ON s.Material = m.MATNo
                            WHERE (m.Brand = '{selected_product_line}' OR m.ProductGroup = '{selected_product_line}')
                            AND s.[Date] >= '{start_date_str}'
                            AND s.[Date] <= '{end_date_str}'
                            {f"AND m.CATALOG = '{selected_catalog}'" if selected_catalog else ""}
                            {get_country_filter('s.Country_final_dest', selected_country)}
                            GROUP BY s.Country_final_dest, m.ProductGroup
                        )
                        SELECT * FROM SalesByCatalog
                        WHERE Country_final_dest IS NOT NULL
                        AND ProductGroup IS NOT NULL
                        ORDER BY Country_final_dest, ProductGroup
                        """
                        
                        sales_by_country = pd.read_sql(sales_query, st.session_state['conn'])
                        
                        if not sales_by_country.empty:
                            # Create pivot table for better display
                            sales_pivot = sales_by_country.pivot_table(
                                index='Country_final_dest',
                                columns='ProductGroup', 
                                values='TotalQuantity',
                                fill_value=0
                            ).reset_index()
                            
                            st.write("**Table 4: Sales by Country**")
                            st.dataframe(sales_pivot)
                            
                            # Create sales visualization
                            fig = px.bar(
                                sales_by_country,
                                x='Country_final_dest',
                                y='TotalQuantity',
                                color='ProductGroup',
                                title=f"{selected_product_line} Sales by Country",
                                labels={'TotalQuantity': 'Total Quantity', 'Country_final_dest': 'Country'}
                            )
                            st.plotly_chart(fig, use_container_width=True)
                        else:
                            st.info("No sales data found for the selected criteria.")
                        
                        # 2. ADVERSE EVENTS
                        st.subheader("2. Adverse Events")
                        
                        # Adverse Events by Type and Year
                        adverse_events_query = f"""
                        SELECT 
                            Type_of_Incident,
                            YEAR,
                            COUNT(*) as EventCount
                        FROM AdverseEventsData
                        WHERE Product_Line = '{selected_product_line}'
                        AND TRY_CONVERT(datetime, Issue_Aware_Date) >= '{start_date_str}'
                        AND TRY_CONVERT(datetime, Issue_Aware_Date) <= '{end_date_str}'
                        {f"AND Catalog = '{selected_catalog}'" if selected_catalog else ""}
                        {get_country_filter('COUNTRY_of_ORIGIN', selected_country)}
                        AND Type_of_Incident IS NOT NULL
                        GROUP BY Type_of_Incident, YEAR
                        ORDER BY YEAR, Type_of_Incident
                        """
                        
                        adverse_events = pd.read_sql(adverse_events_query, st.session_state['conn'])
                        
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
                                    x=ae_pivot.index,
                                    y=ae_pivot[column],
                                    marker_color=colors[i % len(colors)]
                                ))
                            
                            fig.update_layout(
                                title=f"{selected_product_line} MDR Breakdown {start_date.year} - {end_date.year}",
                                xaxis_title="Year",
                                yaxis_title="# MDRs",
                                barmode='stack',
                                height=400
                            )
                            st.plotly_chart(fig, use_container_width=True)
                            
                            # Create table view
                            ae_table = ae_pivot.reset_index()
                            ae_table.columns.name = None
                            st.write(f"**Table 5: {selected_product_line} MDR Category Totals {start_date.year} – {end_date.year}**")
                            st.dataframe(ae_table)
                        else:
                            st.info("No adverse events data found for the selected criteria.")
                        
                        # 2b. ADVERSE EVENTS BY COUNTRY  
                        st.subheader("2b. Adverse Events by Country")
                        
                        ae_by_country_query = f"""
                        SELECT 
                            COUNTRY_of_ORIGIN,
                            Type_of_Incident,
                            COUNT(*) as EventCount
                        FROM AdverseEventsData
                        WHERE Product_Line = '{selected_product_line}'
                        AND TRY_CONVERT(datetime, Issue_Aware_Date) >= '{start_date_str}'
                        AND TRY_CONVERT(datetime, Issue_Aware_Date) <= '{end_date_str}'
                        {f"AND Catalog = '{selected_catalog}'" if selected_catalog else ""}
                        {get_country_filter('COUNTRY_of_ORIGIN', selected_country)}
                        AND COUNTRY_of_ORIGIN IS NOT NULL
                        AND Type_of_Incident IS NOT NULL
                        GROUP BY COUNTRY_of_ORIGIN, Type_of_Incident
                        ORDER BY COUNTRY_of_ORIGIN, Type_of_Incident
                        """
                        
                        ae_by_country = pd.read_sql(ae_by_country_query, st.session_state['conn'])
                        
                        if not ae_by_country.empty:
                            # Create pivot for country view
                            ae_country_pivot = ae_by_country.pivot_table(
                                index='COUNTRY_of_ORIGIN',
                                columns='Type_of_Incident',
                                values='EventCount',
                                fill_value=0
                            )
                            
                            # Create stacked bar chart
                            fig = go.Figure()
                            colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', '#8c564b']
                            
                            for i, column in enumerate(ae_country_pivot.columns):
                                fig.add_trace(go.Bar(
                                    name=column,
                                    x=ae_country_pivot.index,
                                    y=ae_country_pivot[column],
                                    marker_color=colors[i % len(colors)]
                                ))
                            
                            fig.update_layout(
                                title=f"{selected_product_line} Adverse Events by Country - {end_date.year}",
                                xaxis_title="Country",
                                yaxis_title="Events",
                                barmode='stack',
                                height=400
                            )
                            st.plotly_chart(fig, use_container_width=True)
                            
                            st.write(f"**Figure 2: {selected_product_line} Adverse Event (MDR) Breakdown by Country ({end_date.year})**")
                            
                            # Display table
                            ae_country_table = ae_country_pivot.reset_index()
                            ae_country_table.columns.name = None
                            st.dataframe(ae_country_table)
                        else:
                            st.info("No adverse events by country data found for the selected criteria.")
                        
                        # 3. FIELD NOTICES / RECALLS
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
                            recalls_data = pd.read_sql(recalls_query, st.session_state['conn'])
                            
                            if not recalls_data.empty:
                                st.write(f"**Table 7: {selected_product_line} Product Recalls {start_date.year} - {end_date.year}**")
                                st.dataframe(recalls_data)
                                
                                # Create summary chart
                                recalls_summary = recalls_data.groupby('Year_Initiated').size().reset_index(name='Count')
                                fig = px.bar(
                                    recalls_summary,
                                    x='Year_Initiated',
                                    y='Count',
                                    title=f"{selected_product_line} Recalls by Year",
                                    labels={'Count': 'Number of Recalls', 'Year_Initiated': 'Year'}
                                )
                                st.plotly_chart(fig, use_container_width=True)
                            else:
                                st.info(f"No recalls found for {selected_product_line} in the selected time period.")
                        except Exception as e:
                            st.warning(f"Recalls table may not exist in the database: {str(e)}")
                        
                        # 4. CUSTOMER COMPLAINT / USER FEEDBACK REVIEW
                        st.subheader("4. Customer Complaint / User Feedback Review")
                        
                        # Complaint Totals and Rates by Country
                        complaint_rates_query = f"""
                        WITH ComplaintData AS (
                            SELECT 
                                c.CD_Complaint_Country as Country,
                                COUNT(*) as Complaint_Total
                            FROM ComplaintMerged c
                            WHERE c.Brand = '{selected_product_line}'
                            AND c.CD_Date_Complaint_Entry >= '{start_date_str}'
                            AND c.CD_Date_Complaint_Entry <= '{end_date_str}'
                            {f"AND c.Catalog_No = '{selected_catalog}'" if selected_catalog else ""}
                            {get_country_filter('c.CD_Complaint_Country', selected_country)}
                            AND c.CD_Complaint_Country IS NOT NULL
                            GROUP BY c.CD_Complaint_Country
                        ),
                        ProcedureData AS (
                            SELECT 
                                s.Country_final_dest as Country,
                                SUM(CAST(s.Quantity AS BIGINT)) as Estimated_Procedures
                            FROM Sales s
                            INNER JOIN MaterialReference m ON s.Material = m.MATNo
                            WHERE m.Brand = '{selected_product_line}'
                            AND s.[Date] >= '{start_date_str}'
                            AND s.[Date] <= '{end_date_str}'
                            {f"AND m.CATALOG = '{selected_catalog}'" if selected_catalog else ""}
                            {get_country_filter('s.Country_final_dest', selected_country)}
                            AND m.SingleUse = 'Y'
                            GROUP BY s.Country_final_dest
                        )
                        SELECT 
                            COALESCE(c.Country, p.Country) as Country,
                            COALESCE(c.Complaint_Total, 0) as Complaint_Total,
                            COALESCE(p.Estimated_Procedures, 0) as Estimated_Procedures,
                            CASE 
                                WHEN COALESCE(p.Estimated_Procedures, 0) = 0 THEN '0.00%'
                                ELSE FORMAT((COALESCE(c.Complaint_Total, 0) * 100.0 / COALESCE(p.Estimated_Procedures, 1)), 'N4') + '%'
                            END as Complaint_Rate
                        FROM ComplaintData c
                        FULL OUTER JOIN ProcedureData p ON c.Country = p.Country
                        WHERE COALESCE(c.Country, p.Country) IS NOT NULL
                        ORDER BY COALESCE(c.Complaint_Total, 0) DESC
                        """
                        
                        complaint_rates = pd.read_sql(complaint_rates_query, st.session_state['conn'])
                        
                        if not complaint_rates.empty:
                            st.write(f"**Table 8: Complaint Totals and Complaint Rates by Country ({end_date.year})**")
                            st.dataframe(complaint_rates)
                            
                            # Create visualization
                            fig = px.bar(
                                complaint_rates,
                                x='Country',
                                y='Complaint_Total',
                                title=f"{selected_product_line} Complaints by Country",
                                labels={'Complaint_Total': 'Number of Complaints'}
                            )
                            st.plotly_chart(fig, use_container_width=True)
                        else:
                            st.info("No complaint data found for the selected criteria.")
                        
                        # Complaint Rates by Year
                        complaint_rates_by_year_query = f"""
                        WITH ComplaintsByYear AS (
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
                            INNER JOIN MaterialReference m ON s.Material = m.MATNo
                            WHERE m.Brand = '{selected_product_line}'
                            AND s.[Date] >= '{start_date_str}'
                            AND s.[Date] <= '{end_date_str}'
                            {f"AND m.CATALOG = '{selected_catalog}'" if selected_catalog else ""}
                            AND m.SingleUse = 'Y'
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
                        
                        complaint_rates_by_year = pd.read_sql(complaint_rates_by_year_query, st.session_state['conn'])
                        
                        if not complaint_rates_by_year.empty:
                            st.write(f"**Table 9: Complaint Rates {start_date.year} – {end_date.year}**")
                            st.dataframe(complaint_rates_by_year)
                            
                            # Create trend chart
                            fig = px.line(
                                complaint_rates_by_year,
                                x='Year_Occurrence',
                                y='Complaint_Total',
                                title=f"{selected_product_line} Complaint Trends Over Time",
                                labels={'Complaint_Total': 'Number of Complaints', 'Year_Occurrence': 'Year'}
                            )
                            st.plotly_chart(fig, use_container_width=True)
                        else:
                            st.info("No complaint rates by year data found for the selected criteria.")
                        
                        # 5. COMPLAINTS PER FINAL OBJECT CODE
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
                        {get_country_filter('CD_Complaint_Country', selected_country)}
                        AND TA_Final_object_code_QualityCode IS NOT NULL
                        GROUP BY TA_Final_object_code_QualityCode, YEAR(CD_Date_Complaint_Entry)
                        ORDER BY Object_Code, Year
                        """
                        
                        complaints_by_object_code = pd.read_sql(complaints_by_object_code_query, st.session_state['conn'])
                        
                        if not complaints_by_object_code.empty:
                            # Create pivot table
                            object_code_pivot = complaints_by_object_code.pivot_table(
                                index='Object_Code',
                                columns='Year',
                                values='Complaint_Count',
                                fill_value=0
                            )
                            
                            # Create grouped bar chart
                            fig = go.Figure()
                            colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', '#8c564b', '#e377c2']
                            
                            for i, year in enumerate(sorted(object_code_pivot.columns)):
                                fig.add_trace(go.Bar(
                                    name=str(year),
                                    x=object_code_pivot.index,
                                    y=object_code_pivot[year],
                                    marker_color=colors[i % len(colors)]
                                ))
                            
                            fig.update_layout(
                                title=f"{selected_product_line} Complaints per Final Object Code",
                                xaxis_title="Final Object Code",
                                yaxis_title="Complaint Count",
                                barmode='group',
                                height=500,
                                xaxis={'categoryorder': 'total descending'}
                            )
                            st.plotly_chart(fig, use_container_width=True)
                            
                            st.write(f"**Figure 3: {selected_product_line} Complaints per Final Object Code {start_date.year} - {end_date.year}**")
                            
                            # Display table
                            object_code_table = object_code_pivot.reset_index()
                            object_code_table.columns.name = None
                            st.dataframe(object_code_table)
                        else:
                            st.info("No complaints by object code data found for the selected criteria.")
                        
                        # Create download section for complete report
                        st.subheader("📋 Download Complete Report")
                        
                        # Prepare all data for Excel export
                        report_data = {}
                        
                        if not sales_by_country.empty:
                            report_data['Sales by Country'] = sales_pivot if 'sales_pivot' in locals() else sales_by_country
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
                                        'Parameter': ['Product Line', 'Start Date', 'End Date', 'Catalog', 'Country'],
                                        'Value': [
                                            selected_product_line,
                                            start_date_str,
                                            end_date_str, 
                                            selected_catalog if selected_catalog else 'All',
                                            selected_country if selected_country else 'All'
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
                            st.metric("Total Complaints", total_complaints)
                        
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

    # Sidebar with logout and connection info
    with st.sidebar:
        st.write("### Connection Status")
        st.success("✅ Connected to Azure SQL Database")
        st.write(f"**Server:** ph-radc-server-eastus.database.windows.net")
        st.write(f"**Database:** azure-db-radcommercial")
        
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
        st.write("✅ Sales Analysis")
        st.write("✅ Adverse Events")
        st.write("✅ Field Notices/Recalls")
        st.write("✅ Complaint Analysis")
        st.write("✅ Interactive Visualizations")
        st.write("✅ Excel Export")
        
        if st.button("🔓 Logout", type="secondary"):
            st.session_state['logged_in'] = False
            st.session_state['conn'] = None
            st.experimental_rerun()
