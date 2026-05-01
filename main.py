import streamlit as st
from config import *
from database import init_db, query
from helpers import fmt_money
from modules.dashboard import modulo_dashboard
from modules.inventory import modulo_inventario
from modules.clients import modulo_clientes
from modules.new_sale import modulo_nueva_venta
from modules.current_account import modulo_cuenta_corriente
from modules.history import modulo_historial
from modules.login import login, logout, check_login

@st.cache_data(ttl=10)
def cargar_resumen_sidebar():
    total_prods = query("SELECT COUNT(*) v FROM productos", fetch="one")["v"]
    total_clientes = query("SELECT COUNT(*) v FROM clientes", fetch="one")["v"]
    deuda = query("""
        SELECT COALESCE(SUM(v.total - COALESCE(p.pagado,0)),0) v
        FROM ventas v
        LEFT JOIN (SELECT venta_id, SUM(monto) pagado FROM pagos_cuenta GROUP BY venta_id) p
          ON p.venta_id=v.id
        WHERE v.es_cuenta_corriente=TRUE AND v.estado='pendiente'
    """, fetch="one")["v"]
    return total_prods, total_clientes, deuda

# ══════════════════════════════════════════════════════════════════════════════
# NAVEGACIÓN PRINCIPAL
# ══════════════════════════════════════════════════════════════════════════════
def main():
    # Verificar login
    if not check_login():
        login()
        return

    init_db()

    with st.sidebar:
        st.markdown("""
        <div style="text-align:center;padding:20px 0">
            <div style="font-size:40px">🧾</div>
            <div style="color:white;font-size:18px;font-weight:700;margin-top:8px">
                Sistema de<br>Facturación
            </div>
            <div style="color:#94a3b8;font-size:12px;margin-top:4px">
                Gestión completa de ventas
            </div>
        </div>
        """, unsafe_allow_html=True)

        st.markdown("---")

        menu = st.radio(
            "Navegación",
            [
                "📊 Dashboard",
                "📦 Inventario",
                "👥 Clientes",
                "🛒 Nueva Venta",
                "📋 Cuenta Corriente",
                "📜 Historial de Ventas",
            ],
            label_visibility="collapsed"
        )

        st.markdown("---")
        try:
            total_prods, total_clientes, deuda = cargar_resumen_sidebar()
            st.markdown(f"""
            <div style="color:#94a3b8;font-size:12px;padding:8px">
                📦 {total_prods} productos<br>
                👥 {total_clientes} clientes<br>
                💳 Deuda: {fmt_money(deuda)}
            </div>
            """, unsafe_allow_html=True)
        except Exception:
            pass

        # Botón de logout
        logout()

    if menu == "📊 Dashboard":
        modulo_dashboard()
    elif menu == "📦 Inventario":
        modulo_inventario()
    elif menu == "👥 Clientes":
        modulo_clientes()
    elif menu == "🛒 Nueva Venta":
        modulo_nueva_venta()
    elif menu == "📋 Cuenta Corriente":
        modulo_cuenta_corriente()
    elif menu == "📜 Historial de Ventas":
        modulo_historial()


if __name__ == "__main__":
    main()