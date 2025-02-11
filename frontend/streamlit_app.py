import streamlit as st
import pandas as pd
import snowflake.connector
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Snowflake connection parameters from environment variables
SNOWFLAKE_USER = os.getenv('SNOWFLAKE_USER')
SNOWFLAKE_PASSWORD = os.getenv('SNOWFLAKE_PASSWORD')
SNOWFLAKE_ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')
SNOWFLAKE_WAREHOUSE = os.getenv('SNOWFLAKE_WAREHOUSE')
SNOWFLAKE_DATABASE = os.getenv('SNOWFLAKE_DATABASE')
SNOWFLAKE_SCHEMA = os.getenv('SNOWFLAKE_SCHEMA')

# Page config
st.set_page_config(
    page_title="SEC Financial Data Explorer",
    page_icon="📊",
    layout="wide"
)

# Title and description
st.title("📊 SEC Financial Data Explorer")
st.markdown("""
This dashboard allows you to explore SEC financial data using SQL queries.
You can query the following tables:
- `num` (Numeric data)
- `pre` (Presentation data)
- `sub` (Submission data)
- `tag` (Tag data)
""")

# Create Snowflake connection
@st.cache_resource
def create_snowflake_connection():
    try:
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA
        )
        return conn
    except Exception as e:
        st.error(f"Error connecting to Snowflake: {str(e)}")
        return None

# Execute query function
def execute_query(query):
    conn = create_snowflake_connection()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute(query)
            results = cur.fetchall()
            columns = [desc[0] for desc in cur.description]
            df = pd.DataFrame(results, columns=columns)
            return df
        except Exception as e:
            st.error(f"Error executing query: {str(e)}")
            return None
        finally:
            conn.close()
    return None

# Sidebar with example queries
st.sidebar.header("Example Queries")
example_queries = {
    "Top 10 Companies by Assets": """
    SELECT sub.name, num.value as total_assets
    FROM num
    JOIN sub ON num.adsh = sub.adsh
    WHERE num.tag = 'Assets'
    ORDER BY num.value DESC
    LIMIT 10
    """,
    
    "Recent Submissions": """
    SELECT name, form, date
    FROM sub
    ORDER BY date DESC
    LIMIT 10
    """,
    
    "Revenue Analysis": """
    SELECT sub.name, num.value as revenue
    FROM num
    JOIN sub ON num.adsh = sub.adsh
    WHERE num.tag = 'Revenues'
    ORDER BY num.value DESC
    LIMIT 10
    """
}

selected_example = st.sidebar.selectbox(
    "Select an example query",
    ["Custom Query"] + list(example_queries.keys())
)

# Main query input area
if selected_example == "Custom Query":
    query = st.text_area(
        "Enter your SQL query",
        height=150,
        placeholder="SELECT * FROM num LIMIT 10"
    )
else:
    query = st.text_area(
        "SQL query",
        value=example_queries[selected_example],
        height=150
    )

# Execute button
if st.button("Execute Query"):
    if query:
        with st.spinner('Executing query...'):
            df = execute_query(query)
            if df is not None:
                # Display results
                st.subheader("Query Results")
                
                # Show row count
                st.markdown(f"*Found {len(df)} rows*")
                
                # Display the dataframe
                st.dataframe(df, use_container_width=True)
                
                # Download button
                csv = df.to_csv(index=False)
                st.download_button(
                    label="Download results as CSV",
                    data=csv,
                    file_name="query_results.csv",
                    mime="text/csv"
                )
                
                # Basic statistics
                if len(df.select_dtypes(include=['float64', 'int64']).columns) > 0:
                    st.subheader("Numerical Columns Statistics")
                    st.dataframe(df.describe(), use_container_width=True)
    else:
        st.warning("Please enter a query")

# Footer
st.markdown("---")
st.markdown("""
<div style='text-align: center'>
    <p>Created by Your Name | SEC Financial Data Explorer</p>
</div>
""", unsafe_allow_html=True)