import streamlit as st
import pandas as pd
from datetime import date
from database import query
from helpers import fmt_money, estado_stock

@st.cache_data(ttl=10)
def cargar_dashboard_metrics():
    ventas_hoy = query(
        "SELECT COALESCE(SUM(total),0) v FROM ventas WHERE DATE(fecha)=%s AND estado != 'anulada'",
        (date.today().isoformat(),), fetch="one"
    )["v"]
    ventas_mes = query(
        "SELECT COALESCE(SUM(total),0) v FROM ventas WHERE DATE(fecha)>=%s AND estado != 'anulada'",
        (date.today().replace(day=1).isoformat(),), fetch="one"
    )["v"]
    num_ventas_hoy = query(
        "SELECT COUNT(*) v FROM ventas WHERE DATE(fecha)=%s AND estado != 'anulada'",
        (date.today().isoformat(),), fetch="one"
    )["v"]
    deuda_total = query(
        """SELECT COALESCE(SUM(v.total - COALESCE(p.pagado,0)),0) v
           FROM ventas v
           LEFT JOIN (SELECT venta_id, SUM(monto) pagado FROM pagos_cuenta GROUP BY venta_id) p
             ON p.venta_id = v.id
           WHERE v.es_cuenta_corriente=TRUE AND v.estado='pendiente'""",
        fetch="one"
    )["v"]
    sin_stock = query("SELECT COUNT(*) v FROM productos WHERE stock=0", fetch="one")["v"]
    bajo_stock = query("SELECT COUNT(*) v FROM productos WHERE stock>0 AND stock<=stock_minimo", fetch="one")["v"]
    return ventas_hoy, ventas_mes, num_ventas_hoy, deuda_total, sin_stock, bajo_stock

@st.cache_data(ttl=10)
def cargar_ultimas_ventas():
    return query("""
            SELECT v.id, COALESCE(c.nombre,'Consumidor Final') cliente,
                   v.fecha, v.total, v.metodo_pago, v.estado
            FROM ventas v LEFT JOIN clientes c ON v.cliente_id=c.id
            WHERE v.estado != 'anulada'
            ORDER BY v.fecha DESC LIMIT 8
        """)

@st.cache_data(ttl=10)
def cargar_alertas_inventario():
    return query("""
            SELECT nombre, stock, stock_minimo FROM productos
            WHERE stock <= stock_minimo ORDER BY stock ASC LIMIT 10
        """)

# ── DASHBOARD ─────────────────────────────────────────────────────────────────
def modulo_dashboard():
    st.markdown("""
    <div class="app-header">
        <div>
            <h1>📊 Panel Principal</h1>
            <p>Resumen del negocio en tiempo real</p>
        </div>
    </div>
    """, unsafe_allow_html=True)

    ventas_hoy, ventas_mes, num_ventas_hoy, deuda_total, sin_stock, bajo_stock = cargar_dashboard_metrics()

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("💰 Ventas Hoy", fmt_money(ventas_hoy), f"{num_ventas_hoy} transacciones")
    with col2:
        st.metric("📅 Ventas del Mes", fmt_money(ventas_mes))
    with col3:
        st.metric("📋 Deuda Pendiente", fmt_money(deuda_total))
    with col4:
        st.metric("⚠️ Alertas de Stock", f"{sin_stock} sin stock",
                  f"{bajo_stock} bajo mínimo", delta_color="inverse")

    st.markdown("---")
    col_a, col_b = st.columns(2)

    with col_a:
        st.subheader("📈 Últimas Ventas")
        ultimas = cargar_ultimas_ventas()
        if ultimas:
            df = pd.DataFrame(ultimas)
            df["total"] = df["total"].apply(fmt_money)
            df["fecha"] = pd.to_datetime(df["fecha"]).dt.strftime("%d/%m %H:%M")
            df.columns = ["#","Cliente","Fecha","Total","Pago","Estado"]
            st.dataframe(df, use_container_width=True, hide_index=True)
        else:
            st.info("No hay ventas registradas aún.")

    with col_b:
        st.subheader("📦 Alertas de Inventario")
        alertas = cargar_alertas_inventario()
        if alertas:
            for row in alertas:
                estado, cls = estado_stock(row["stock"], row["stock_minimo"])
                alerta_cls = "alerta-roja" if estado in ("Sin stock","Crítico") else "alerta-amarilla"
                st.markdown(f"""
                <div class="{alerta_cls}">
                    <strong>{row['nombre']}</strong> — Stock: {row['stock']} unidades
                    <span class="{cls}" style="float:right">{estado}</span>
                </div>""", unsafe_allow_html=True)
        else:
            st.success("✅ Todo el inventario en niveles normales.")