import streamlit as st
import pandas as pd
from database import query, execute
from helpers import fmt_money, semaforo_deuda

@st.cache_data(ttl=10)
def cargar_vencidas():
    return query("""
        SELECT v.id, COALESCE(c.nombre,'—') cliente,
               v.fecha, v.total,
               COALESCE(p.pagado,0) pagado,
               EXTRACT(DAY FROM NOW()-v.fecha)::INTEGER dias
        FROM ventas v
        LEFT JOIN clientes c ON v.cliente_id=c.id
        LEFT JOIN (SELECT venta_id, SUM(monto) pagado FROM pagos_cuenta GROUP BY venta_id) p
          ON p.venta_id=v.id
        WHERE v.es_cuenta_corriente=TRUE AND v.estado='pendiente'
          AND EXTRACT(DAY FROM NOW()-v.fecha) > 30
        ORDER BY dias DESC
    """)

@st.cache_data(ttl=10)
def cargar_pendientes():
    return query("""
        SELECT v.id, COALESCE(c.nombre,'Consumidor Final') cliente,
               v.fecha, v.total,
               COALESCE(p.pagado,0) pagado,
               EXTRACT(DAY FROM NOW()-v.fecha)::INTEGER dias
        FROM ventas v
        LEFT JOIN clientes c ON v.cliente_id=c.id
        LEFT JOIN (SELECT venta_id, SUM(monto) pagado FROM pagos_cuenta GROUP BY venta_id) p
          ON p.venta_id=v.id
        WHERE v.es_cuenta_corriente=TRUE AND v.estado='pendiente'
        ORDER BY v.fecha ASC
    """)

@st.cache_data(ttl=10)
def cargar_resumen_cuenta_corriente():
    return query("""
        SELECT COALESCE(c.nombre,'Consumidor Final') cliente,
               COUNT(v.id) facturas,
               SUM(v.total) total,
               COALESCE(SUM(p.pagado),0) pagado
        FROM ventas v
        LEFT JOIN clientes c ON v.cliente_id=c.id
        LEFT JOIN (SELECT venta_id, SUM(monto) pagado FROM pagos_cuenta GROUP BY venta_id) p
          ON p.venta_id=v.id
        WHERE v.es_cuenta_corriente=TRUE AND v.estado='pendiente'
        GROUP BY v.cliente_id, c.nombre
        ORDER BY (SUM(v.total) - COALESCE(SUM(p.pagado),0)) DESC
    """)

@st.cache_data(ttl=10)
def cargar_pendientes_pago():
    return query("""
        SELECT v.id, COALESCE(c.nombre,'—') cliente, v.total,
               COALESCE(p.pagado,0) pagado
        FROM ventas v
        LEFT JOIN clientes c ON v.cliente_id=c.id
        LEFT JOIN (SELECT venta_id, SUM(monto) pagado FROM pagos_cuenta GROUP BY venta_id) p
          ON p.venta_id=v.id
        WHERE v.es_cuenta_corriente=TRUE AND v.estado='pendiente'
        ORDER BY v.fecha ASC
    """)

@st.cache_data(ttl=10)
def cargar_historial_pagos():
    return query("""
        SELECT pc.id, COALESCE(c.nombre,'—') cliente,
               pc.venta_id, pc.monto, pc.metodo_pago, pc.fecha, pc.notas
        FROM pagos_cuenta pc
        JOIN ventas v ON pc.venta_id=v.id
        LEFT JOIN clientes c ON v.cliente_id=c.id
        ORDER BY pc.fecha DESC LIMIT 50
    """)

# ── CUENTA CORRIENTE ──────────────────────────────────────────────────────────
def modulo_cuenta_corriente():
    st.title("📋 Cuenta Corriente")

    # Alertas de vencimiento
    vencidas = cargar_vencidas()
    if vencidas:
        st.markdown("### 🔴 Facturas Vencidas (más de 30 días)")
        for v in vencidas:
            saldo = float(v["total"]) - float(v["pagado"])
            st.markdown(f"""
            <div class="alerta-roja">
                <strong>Factura #{v['id']}</strong> — {v['cliente']} —
                Saldo: {fmt_money(saldo)} — {v['dias']} días sin pagar
            </div>""", unsafe_allow_html=True)
        st.markdown("---")

    tab1, tab2 = st.tabs(["📌 Pendientes", "💰 Registrar Pago"])

    with tab1:
        pendientes = cargar_pendientes()
        if pendientes:
            for p in pendientes:
                saldo = float(p["total"]) - float(p["pagado"])
                emoji, cls = semaforo_deuda(p["dias"] or 0)
                st.markdown(f"""
                <div class="{cls}">
                    {emoji} <strong>Factura #{p['id']}</strong> | {p['cliente']} |
                    Total: {fmt_money(p['total'])} | Pagado: {fmt_money(p['pagado'])} |
                    <strong>Saldo: {fmt_money(saldo)}</strong> | {p['dias'] or 0} días
                </div>""", unsafe_allow_html=True)

            st.markdown("---")
            st.subheader("📊 Resumen por cliente")
            resumen = cargar_resumen_cuenta_corriente()
            filas = []
            for r in resumen:
                saldo = float(r["total"]) - float(r["pagado"])
                filas.append({
                    "Cliente": r["cliente"],
                    "Facturas": r["facturas"],
                    "Total": fmt_money(r["total"]),
                    "Pagado": fmt_money(r["pagado"]),
                    "Saldo": fmt_money(saldo),
                })
            st.dataframe(pd.DataFrame(filas), use_container_width=True, hide_index=True)
        else:
            st.success("✅ No hay cuentas corrientes pendientes.")

    with tab2:
        st.subheader("💰 Registrar Pago")
        pendientes2 = cargar_pendientes_pago()
        if not pendientes2:
            st.info("No hay facturas pendientes.")
        else:
            opts = {
                f"Factura #{p['id']} — {p['cliente']} — Saldo: {fmt_money(float(p['total'])-float(p['pagado']))}": p
                for p in pendientes2
            }
            sel = st.selectbox("Seleccionar factura", list(opts.keys()))
            fac = opts[sel]
            saldo = float(fac["total"]) - float(fac["pagado"])

            with st.form("form_pago"):
                c1, c2, c3 = st.columns(3)
                monto = c1.number_input("Monto a pagar ($)", min_value=0.01,
                                        max_value=float(saldo), value=float(saldo), step=0.01)
                metodo = c2.selectbox("Método", ["Efectivo", "Tarjeta", "Transferencia"])
                notas = c3.text_input("Notas")
                if st.form_submit_button("✅ Registrar Pago", type="primary"):
                    execute(
                        "INSERT INTO pagos_cuenta (venta_id, monto, metodo_pago, notas) VALUES (%s,%s,%s,%s)",
                        (fac["id"], monto, metodo, notas or None)
                    )
                    nuevo_pagado = float(fac["pagado"]) + monto
                    if nuevo_pagado >= float(fac["total"]):
                        execute("UPDATE ventas SET estado='pagada' WHERE id=%s", (fac["id"],))
                    cargar_vencidas.clear()
                    cargar_pendientes.clear()
                    cargar_resumen_cuenta_corriente.clear()
                    cargar_pendientes_pago.clear()
                    cargar_historial_pagos.clear()
                    st.success(f"✅ Pago de {fmt_money(monto)} registrado.")
                    st.rerun()

        st.markdown("---")
        st.subheader("📜 Historial de Pagos")
        historial = cargar_historial_pagos()
        if historial:
            df = pd.DataFrame(historial)
            df["monto"] = df["monto"].apply(fmt_money)
            df["fecha"] = pd.to_datetime(df["fecha"]).dt.strftime("%d/%m/%Y %H:%M")
            df.columns = ["ID","Cliente","Factura #","Monto","Método","Fecha","Notas"]
            st.dataframe(df, use_container_width=True, hide_index=True)
        else:
            st.info("No hay pagos registrados.")