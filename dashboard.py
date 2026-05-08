import streamlit as st
import pandas as pd
import psycopg2
import plotly.express as px
import plotly.graph_objects as go
from collections import Counter
import re
import os

# Page Config
st.set_page_config(page_title="Global News Intelligence", page_icon="G", layout="wide")

# Custom CSS for Premium Look
st.markdown("""
    <style>
    [data-testid="stMetricValue"] {
        font-size: 2.2rem !important;
        color: #00d4ff !important;
    }
    .main {
        background-color: #0e1117;
    }
    div[data-testid="stExpander"] {
        border: 1px solid #3e4259;
        border-radius: 10px;
    }
    </style>
    """, unsafe_allow_html=True)

def get_connection():
    return psycopg2.connect(
        host="localhost",
        port=5432,
        dbname="data_warehouse",
        user="airflow",
        password="airflow"
    )

def reset_system():
    """Wipes all data from MinIO and Postgres"""
    try:
        # 1. Clear Postgres tables (one by one to avoid total failure)
        conn = get_connection()
        with conn.cursor() as cur:
            try:
                cur.execute("TRUNCATE TABLE gold_articles CASCADE;")
            except: pass
            try:
                cur.execute("TRUNCATE TABLE quality_logs CASCADE;")
            except: pass
        conn.commit()
        conn.close()
        
        # 2. Clear MinIO
        from minio import Minio
        client = Minio("localhost:9000", access_key="minioadmin", secret_key="minioadmin", secure=False)
        for bucket in ["raw-data", "silver-data"]:
            if client.bucket_exists(bucket):
                objects = client.list_objects(bucket, recursive=True)
                for obj in objects:
                    client.remove_object(bucket, obj.object_name)
        
        return True
    except Exception as e:
        st.sidebar.error(f"Reset failed: {e}")
        return False

def get_top_keywords(df, top_n=15):
    """Simple stop-word filtered keyword counter"""
    stop_words = {'the', 'and', 'for', 'that', 'with', 'this', 'from', 'was', 'were', 'their', 'they', 'will', 'says', 'said', 'has', 'have', 'more', 'about', 'after'}
    all_text = " ".join(df['title'].fillna('') + " " + df['content'].fillna(''))
    words = re.findall(r'\w+', all_text.lower())
    filtered_words = [w for w in words if len(w) > 3 and w not in stop_words]
    return pd.DataFrame(Counter(filtered_words).most_common(top_n), columns=['Word', 'Count'])

# --- Sidebar ---
st.sidebar.title("Control Panel")
st.sidebar.info("Medallion Architecture News Pipeline")
auto_refresh = st.sidebar.checkbox("Auto-refresh Data", value=True)
search_query = st.sidebar.text_input("Search Articles", "")

st.sidebar.divider()
st.sidebar.markdown("### Danger Zone")
if st.sidebar.button("Reset All Data", help="Wipes MinIO and Postgres completely"):
    if reset_system():
        st.sidebar.success("System wiped! You can now run the DAGs.")
        st.rerun()

# --- Data Loading ---
try:
    conn = get_connection()
    
    # Load Main Data
    query = "SELECT * FROM gold_articles"
    if search_query:
        query += f" WHERE title ILIKE '%%{search_query}%%' OR content ILIKE '%%{search_query}%%'"
    
    df = pd.read_sql(query, conn)
    
    # Header Section
    st.title("News Intelligence Dashboard")
    st.markdown(f"**Last updated:** {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M')}")
    
    # 1. KPI Row
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Total Articles", len(df))
    m2.metric("Active Sources", df['source'].nunique() if not df.empty else 0)
    m3.metric("Avg Length", f"{int(df['word_count'].mean()) if not df.empty else 0} w")
    m4.metric("Categories", df['category'].nunique() if not df.empty else 0)

    st.divider()

    # 2. Main Analytics Row
    col_left, col_mid, col_right = st.columns([2, 1, 1])

    with col_left:
        # TENDANCES D'ACTUALITÉ (Requirement 1)
        st.subheader("Tendances d'Actualité")
        df['day'] = pd.to_datetime(df['loaded_at']).dt.date
        trend_df = df.groupby(['day', 'source']).size().reset_index(name='count')
        fig_trend = px.line(trend_df, x='day', y='count', color='source', markers=True,
                           line_shape='spline', template="plotly_dark")
        st.plotly_chart(fig_trend, use_container_width=True)

    with col_mid:
        # NOMBRE ARTICLES PAR SOURCE (Requirement 2)
        st.subheader("Part des Sources")
        source_counts = df['source'].value_counts().reset_index()
        fig_pie = px.pie(source_counts, values='count', names='source', hole=0.5,
                        color_discrete_sequence=px.colors.qualitative.Set3)
        st.plotly_chart(fig_pie, use_container_width=True)

    with col_right:
        # ARTICLES PAR PAYS (Requirement 3)
        st.subheader("Articles par Pays")
        country_counts = df['country'].value_counts().reset_index()
        fig_country = px.bar(country_counts, x='country', y='count', color='country',
                            template="plotly_dark", color_discrete_sequence=px.colors.qualitative.Pastel)
        st.plotly_chart(fig_country, use_container_width=True)

    st.divider()

    # 3. Keyword and Category Row
    col_k, col_c = st.columns(2)

    with col_k:
        # MOTS CLÉS LES PLUS FRÉQUENTS (Requirement 3)
        st.subheader("Mots-Clés les plus Fréquents")
        if not df.empty:
            kw_df = get_top_keywords(df)
            fig_kw = px.bar(kw_df, x='Count', y='Word', orientation='h', 
                           color='Count', color_continuous_scale='Viridis')
            fig_kw.update_layout(yaxis={'categoryorder':'total ascending'})
            st.plotly_chart(fig_kw, use_container_width=True)
        else:
            st.write("No keywords found.")

    with col_c:
        # CATEGORY DISTRIBUTION
        st.subheader("Distribution par Thème")
        cat_counts = df['category'].value_counts().reset_index()
        fig_cat = px.bar(cat_counts, x='category', y='count', color='category', text_auto=True)
        st.plotly_chart(fig_cat, use_container_width=True)

    st.divider()

    # 4. Global Analysis & Quality Audit
    st.subheader("Global Insights & Quality Monitoring")
    
    tab1, tab2, tab3 = st.tabs(["News Explorer", "Data Quality Audit", "Live Stream (Kafka)"])
    
    with tab1:
        c1, c2 = st.columns([1, 3])
        with c1:
            lang_counts = df['language'].value_counts().reset_index()
            fig_lang = px.pie(lang_counts, values='count', names='language', title="Articles par Langue")
            st.plotly_chart(fig_lang, use_container_width=True)
        with c2:
            st.dataframe(df[['source', 'title', 'author', 'language', 'word_count', 'loaded_at']].sort_values('loaded_at', ascending=False), 
                        hide_index=True, use_container_width=True)
            
    with tab2:
        st.markdown("### Data Quality Dimensions (Spec Section 9)")
        # ... (rest of DQ code remains same)
        try:
            dq_df = pd.read_sql("SELECT * FROM quality_logs", conn)
            if not dq_df.empty:
                q_col1, q_col2 = st.columns(2)
                
                with q_col1:
                    # Success rate by dimension
                    dim_stats = dq_df.groupby(['dimension', 'status']).size().reset_index(name='count')
                    fig_dq = px.bar(dim_stats, x='dimension', y='count', color='status', 
                                   title="Audit Results by Dimension", barmode='group',
                                   color_discrete_map={'PASS': '#00cc96', 'FAIL': '#ef553b'})
                    st.plotly_chart(fig_dq, use_container_width=True)
                
                with q_col2:
                    # Failure breakdown by test
                    fails_df = dq_df[dq_df['status'] == 'FAIL']
                    if not fails_df.empty:
                        fail_stats = fails_df.groupby('test_name').size().reset_index(name='count')
                        fig_fails = px.pie(fail_stats, values='count', names='test_name', 
                                         title="Failure Breakdown (Quarantine Reasons)",
                                         color_discrete_sequence=px.colors.sequential.Reds_r)
                        st.plotly_chart(fig_fails, use_container_width=True)
                    else:
                        st.success("No quality failures detected in the current dataset!")
                
                st.markdown("#### Detailed Audit Logs")
                st.dataframe(dq_df.sort_values('check_date', ascending=False).head(20), hide_index=True, use_container_width=True)
            else:
                st.info("No quality logs found yet. Run the Silver DAG to populate this audit.")
        except:
            st.warning("Quality log table not found. It will be created on the next DAG run.")

    with tab3:
        st.markdown("### Real-time Event Stream (Kafka)")
        st.info("Showing the last 10 events from the 'news_events' topic")
        
        try:
            from kafka import KafkaConsumer
            import json
            
            # Simple consumer to peek at the topic
            consumer = KafkaConsumer(
                'news_events',
                bootstrap_servers=['localhost:9094'], # 9094 is our local port to the broker
                auto_offset_reset='earliest',
                enable_auto_commit=False,
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                consumer_timeout_ms=1000 # Don't hang the UI
            )
            
            events = []
            for msg in consumer:
                events.append(msg.value)
                if len(events) >= 10: break
            consumer.close()
            
            if events:
                for ev in reversed(events):
                    st.write(f"[{ev['timestamp']}] **{ev['source']}**: {ev['title']}")
                    st.caption(f"URL: {ev['url']}")
                    st.divider()
            else:
                st.write("No real-time events found. Run the Bronze DAG to generate events!")
        except Exception as e:
            st.warning(f"Could not connect to Kafka Stream: {e}")
            st.info("Make sure Kafka is running and kafka-python is installed locally.")

    conn.close()

except Exception as e:
    st.error(f"Connection Error: {e}")
    st.info("Check if your Docker containers are running and Postgres is accessible on localhost:5432.")
