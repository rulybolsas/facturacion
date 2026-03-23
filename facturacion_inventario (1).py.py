import streamlit as st
import psycopg2
from psycopg2.extras import RealDictCursor
import pandas as pd
from datetime import datetime, date, timedelta

# ── CONFIGURACIÓN DE PÁGINA ──────────────────────────────────────────────────
st.set_page_config(
    page_title="Sistema de Facturación - RulyBolsas",
    page_icon="🧾",
    layout="wide",
)

# ── CONEXIÓN A SUPABASE (NUBE) ───────────────────────────────────────────────
def get_conn():
    # Conexión usando tu contraseña: Falucho881.
    DB_URI = "postgresql://postgres:Falucho881.@db.ftdyrelaeyreifvywhpb.supabase.co:5432/postgres"
    conn = psycopg2.connect(DB_URI)
    return conn

# ── INICIALIZACIÓN DE TABLAS ────────────────────────────────────────────────
def init_db():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS productos (
        id SERIAL PRIMARY KEY,
        nombre TEXT NOT NULL,
        precio REAL NOT NULL,
        stock INTEGER NOT NULL DEFAULT 0,
        stock_minimo INTEGER NOT NULL DEFAULT 5,
        creado_en TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    CREATE TABLE IF NOT EXISTS clientes (
        id SERIAL PRIMARY KEY,
        nombre TEXT NOT NULL,
        telefono TEXT,
        email TEXT,
        creado_en TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    CREATE TABLE IF NOT EXISTS ventas (
        id SERIAL PRIMARY KEY,
        cliente_id INTEGER,
        fecha TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        subtotal REAL NOT NULL,
        descuento REAL DEFAULT 0,
        total REAL NOT NULL,
        metodo_pago TEXT NOT NULL,
        es_cuenta_corriente INTEGER DEFAULT 0,
        estado TEXT DEFAULT 'pagada',
        notas TEXT,
        FOREIGN KEY (cliente_id) REFERENCES clientes(id)
    );
    CREATE TABLE IF NOT EXISTS venta_items (
        id SERIAL PRIMARY KEY,
        venta_id INTEGER NOT NULL,
        producto_id INTEGER NOT NULL,
        nombre_producto TEXT NOT NULL,
        cantidad INTEGER NOT NULL,
        precio_unitario REAL NOT NULL,
        subtotal REAL NOT NULL,
        FOREIGN KEY (venta_id) REFERENCES ventas(id),
        FOREIGN KEY (producto_id) REFERENCES productos(id)
    );
    CREATE TABLE IF NOT EXISTS pagos_cuenta (
        id SERIAL PRIMARY KEY,
        venta_id INTEGER NOT NULL,
        monto REAL NOT NULL,
        fecha TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        metodo_pago TEXT NOT NULL,
        notas TEXT,
        FOREIGN KEY (venta_id) REFERENCES ventas(id)
    );
    """)
    conn.commit()
    cur.close()
    conn.close()

# ── HELPERS ──────────────────────────────────────────────────────────────────
def fmt_money(v):
    return f"$ {v:,.2f}"

# ── MÓDULOS DEL SISTEMA ──────────────────────────────────────────────────────

def modulo_dashboard():
    st.title("📊 Panel Principal")
    conn = get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    # Ventas de hoy
    cur.execute("SELECT COALESCE(SUM(total),0) as total FROM ventas WHERE fecha::date = CURRENT_DATE AND estado != 'anulada'")
    ventas_hoy = cur.fetchone()['total']
    
    # Deuda total pendiente
    cur.execute("""
        SELECT COALESCE(SUM(v.total - COALESCE((SELECT SUM(monto) FROM pagos_cuenta p WHERE p.venta_id=v.id),0)),0) as deuda
        FROM ventas v WHERE es_cuenta_corriente=1 AND estado='pendiente'
    """)
    deuda = cur.fetchone()['deuda']

    col1, col2 = st.columns(2)
    col1.metric("💰 Ventas Hoy", fmt_money(ventas_hoy))
    col2.metric("📋 Deuda Clientes", fmt_money(deuda))
    
    st.markdown("---")
    st.subheader("⚠️ Alertas de Stock")
    cur.execute("SELECT nombre, stock FROM productos WHERE stock <= stock_minimo")
    alertas = cur.fetchall()
    if alertas:
        st.warning(f"Hay {len(alertas)} productos con stock bajo o nulo.")
        st.table(alertas)
    else:
        st.success("Inventario al día.")
        
    cur.close()
    conn.close()

def modulo_inventario():
    st.title("📦 Gestión de Inventario")
    conn = get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    with st.expander("➕ Agregar Nuevo Producto"):
        with st.form("form_prod", clear_on_submit=True):
            nom = st.text_input("Nombre del producto")
            pre = st.number_input("Precio de venta", min_value=0.0)
            stk = st.number_input("Stock inicial", min_value=0)
            if st.form_submit_button("Guardar en Nube"):
                cur.execute("INSERT INTO productos (nombre, precio, stock) VALUES (%s, %s, %s)", (nom, pre, stk))
                conn.commit()
                st.success("Producto registrado exitosamente.")
                st.rerun()

    cur.execute("SELECT id, nombre, precio, stock FROM productos ORDER BY nombre ASC")
    df = pd.DataFrame(cur.fetchall())
    if not df.empty:
        st.dataframe(df, use_container_width=True)

    cur.close()
    conn.close()

# ── NAVEGACIÓN ───────────────────────────────────────────────────────────────
def main():
    init_db()
    
    st.sidebar.title("RulyBolsas App")
    menu = st.sidebar.radio("Navegación", ["📊 Dashboard", "📦 Inventario", "👥 Clientes", "🛒 Nueva Venta"])
    
    if menu == "📊 Dashboard":
        modulo_dashboard()
    elif menu == "📦 Inventario":
        modulo_inventario()
    else:
        st.info("Módulo en desarrollo para la nube. ¡Pronto disponible!")

if __name__ == "__main__":
    main()