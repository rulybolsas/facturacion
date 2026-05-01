import streamlit as st
import pandas as pd
from datetime import date, timedelta
from database import query
from helpers import fmt_money

@st.cache_data(ttl=30)
def cargar_clientes_historial():
    return query("SELECT id, nombre FROM clientes ORDER BY nombre")

# ── HISTORIAL DE VENTAS ───────────────────────────────────────────────────────
def modulo_historial():
    st.title("📜 Historial de Ventas")

    col1, col2, col3 = st.columns(3)
    with col1:
        fecha_desde = st.date_input("Desde", value=date.today() - timedelta(days=30))
    with col2:
        fecha_hasta = st.date_input("Hasta", value=date.today())
    with col3:
        clientes_lista = cargar_clientes_historial()
        opts_cli = {"Todos": None}
        opts_cli.update({c["nombre"]: c["id"] for c in clientes_lista})
        cli_sel = st.selectbox("Cliente", list(opts_cli.keys()))

    filtro_cliente = opts_cli[cli_sel]
    base_params = [fecha_desde.isoformat(), fecha_hasta.isoformat()]

    extra_where = "AND v.cliente_id=%s" if filtro_cliente else ""
    extra_params = [filtro_cliente] if filtro_cliente else []

    ventas = query(f"""
        SELECT v.id, COALESCE(c.nombre,'Consumidor Final') cliente,
               v.fecha, v.subtotal, v.descuento, v.total,
               v.metodo_pago, v.estado
        FROM ventas v LEFT JOIN clientes c ON v.cliente_id=c.id
        WHERE DATE(v.fecha) BETWEEN %s AND %s AND v.estado != 'anulada'
        {extra_where}
        ORDER BY v.fecha DESC
    """, base_params + extra_params)

    if ventas:
        tot = sum(float(v["total"]) for v in ventas)
        col_a, col_b, col_c = st.columns(3)
        col_a.metric("Total Período", fmt_money(tot))
        col_b.metric("Cantidad de Ventas", len(ventas))
        col_c.metric("Ticket Promedio", fmt_money(tot / len(ventas)))

        st.markdown("---")
        tab1, tab2 = st.tabs(["📋 Por Venta", "📦 Por Producto"])

        with tab1:
            filas = []
            for v in ventas:
                filas.append({
                    "#": v["id"],
                    "Cliente": v["cliente"],
                    "Fecha": pd.to_datetime(v["fecha"]).strftime("%d/%m/%Y %H:%M"),
                    "Subtotal": fmt_money(v["subtotal"]),
                    "Descuento": fmt_money(v["descuento"]),
                    "Total": fmt_money(v["total"]),
                    "Pago": v["metodo_pago"],
                    "Estado": v["estado"].capitalize(),
                })
            st.dataframe(pd.DataFrame(filas), use_container_width=True, hide_index=True)

            st.markdown("---")
            st.subheader("🔍 Detalle de Venta")
            ids = [v["id"] for v in ventas]
            vid_sel = st.selectbox("Seleccionar venta #", ids)
            if vid_sel:
                items = query(
                    "SELECT nombre_producto, cantidad, precio_unitario, subtotal FROM venta_items WHERE venta_id=%s",
                    (vid_sel,)
                )
                if items:
                    df_det = pd.DataFrame(items)
                    df_det["precio_unitario"] = df_det["precio_unitario"].apply(fmt_money)
                    df_det["subtotal"] = df_det["subtotal"].apply(fmt_money)
                    df_det.columns = ["Producto","Cant.","P. Unit.","Subtotal"]
                    st.dataframe(df_det, use_container_width=True, hide_index=True)

        with tab2:
            resumen_prod = query(f"""
                SELECT vi.nombre_producto, SUM(vi.cantidad) cant, SUM(vi.subtotal) total
                FROM venta_items vi
                JOIN ventas v ON vi.venta_id=v.id
                WHERE DATE(v.fecha) BETWEEN %s AND %s AND v.estado != 'anulada'
                {extra_where}
                GROUP BY vi.nombre_producto ORDER BY total DESC
            """, base_params + extra_params)
            if resumen_prod:
                df_prod = pd.DataFrame(resumen_prod)
                df_prod["total"] = df_prod["total"].apply(fmt_money)
                df_prod.columns = ["Producto","Unidades Vendidas","Total"]
                st.dataframe(df_prod, use_container_width=True, hide_index=True)
    else:
        st.info("No hay ventas en el período seleccionado.")