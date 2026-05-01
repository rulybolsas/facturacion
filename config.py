import streamlit as st

import streamlit as st

# ── Configuración de página ──────────────────────────────────────────────────
st.set_page_config(
    page_title="Sistema de Facturación",
    page_icon="🧾",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── CSS personalizado ────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

html, body, [class*="css"] {
    font-family: 'Inter', sans-serif;
}

section[data-testid="stSidebar"] {
    background: linear-gradient(180deg, #1a1f2e 0%, #0f1420 100%);
}
section[data-testid="stSidebar"] .stRadio label {
    color: #e2e8f0 !important;
    font-weight: 500;
}

[data-testid="stMetric"] {
    background: #f8fafc;
    border: 1px solid #e2e8f0;
    border-radius: 12px;
    padding: 16px;
}
[data-testid="stMetricValue"] { color: #1e293b; font-weight: 700; }

.dataframe { border-radius: 8px; overflow: hidden; }

.stButton > button[kind="primary"] {
    background: linear-gradient(135deg, #3b82f6, #1d4ed8);
    border: none;
    border-radius: 8px;
    font-weight: 600;
    color: white;
}
.stButton > button[kind="primary"]:hover {
    background: linear-gradient(135deg, #2563eb, #1e40af);
    transform: translateY(-1px);
    box-shadow: 0 4px 12px rgba(59,130,246,0.4);
}

.alerta-verde   { background:#dcfce7; border-left:4px solid #16a34a; padding:10px 16px; border-radius:6px; margin:4px 0; }
.alerta-amarilla{ background:#fef9c3; border-left:4px solid #ca8a04; padding:10px 16px; border-radius:6px; margin:4px 0; }
.alerta-roja    { background:#fee2e2; border-left:4px solid #dc2626; padding:10px 16px; border-radius:6px; margin:4px 0; }

.badge-normal   { background:#dcfce7; color:#15803d; padding:3px 10px; border-radius:20px; font-size:12px; font-weight:600; }
.badge-bajo     { background:#fef9c3; color:#a16207; padding:3px 10px; border-radius:20px; font-size:12px; font-weight:600; }
.badge-critico  { background:#ffedd5; color:#c2410c; padding:3px 10px; border-radius:20px; font-size:12px; font-weight:600; }
.badge-sinstock { background:#fee2e2; color:#b91c1c; padding:3px 10px; border-radius:20px; font-size:12px; font-weight:600; }

.app-header {
    background: linear-gradient(135deg, #1e293b 0%, #334155 100%);
    color: white;
    padding: 20px 28px;
    border-radius: 14px;
    margin-bottom: 24px;
}
.app-header h1 { margin:0; font-size:24px; font-weight:700; }
.app-header p  { margin:0; opacity:0.7; font-size:13px; }

.summary-card {
    background: white;
    border: 1px solid #e2e8f0;
    border-radius: 12px;
    padding: 20px;
    box-shadow: 0 1px 4px rgba(0,0,0,0.06);
}
</style>
""", unsafe_allow_html=True)