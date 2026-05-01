import streamlit as st
import pandas as pd
from database import query, execute
from helpers import fmt_money

@st.cache_data(ttl=10)
def cargar_clientes():
    return query("SELECT id, nombre, telefono, email FROM clientes ORDER BY nombre")

@st.cache_data(ttl=10)
def cargar_deudas_clientes():
    rows = query("""
        SELECT v.cliente_id,
               COALESCE(SUM(v.total - COALESCE(p.pagado,0)),0) deuda
        FROM ventas v
        LEFT JOIN (SELECT venta_id, SUM(monto) pagado FROM pagos_cuenta GROUP BY venta_id) p
          ON p.venta_id=v.id
        WHERE v.es_cuenta_corriente=TRUE AND v.estado='pendiente'
        GROUP BY v.cliente_id
    """)
    return {row['cliente_id']: float(row['deuda']) for row in rows}

# ── CLIENTES ──────────────────────────────────────────────────────────────────
def modulo_clientes():
    st.title("👥 Gestión de Clientes")
    tab1, tab2 = st.tabs(["📋 Listado", "➕ Nuevo Cliente"])

    with tab1:
        clientes = cargar_clientes()
        deudas = cargar_deudas_clientes()
        if clientes:
            buscar = st.text_input("🔍 Buscar cliente")
            filas = []
            for c in clientes:
                if buscar and buscar.lower() not in c["nombre"].lower():
                    continue
                deuda = deudas.get(c["id"], 0.0)
                filas.append({
                    "ID": c["id"],
                    "Nombre": c["nombre"],
                    "Teléfono": c["telefono"] or "—",
                    "Email": c["email"] or "—",
                    "Deuda": fmt_money(deuda) if deuda else "$ 0",
                })
            if filas:
                st.dataframe(pd.DataFrame(filas), use_container_width=True, hide_index=True)

            st.markdown("---")
            st.subheader("✏️ Editar / Eliminar")
            opts = {f"{c['nombre']} (ID {c['id']})": c for c in clientes}
            sel = st.selectbox("Seleccionar cliente", list(opts.keys()))
            if sel:
                cli = opts[sel]
                with st.form("form_edit_cliente"):
                    c1, c2, c3 = st.columns(3)
                    n = c1.text_input("Nombre", value=cli["nombre"])
                    t = c2.text_input("Teléfono", value=cli["telefono"] or "")
                    e = c3.text_input("Email", value=cli["email"] or "")
                    b1, b2 = st.columns(2)
                    if b1.form_submit_button("💾 Guardar", type="primary"):
                        execute("UPDATE clientes SET nombre=%s, telefono=%s, email=%s WHERE id=%s",
                                (n, t or None, e or None, cli["id"]))
                        cargar_clientes.clear()
                        cargar_deudas_clientes.clear()
                        st.success("Cliente actualizado.")
                        st.rerun()
                    if b2.form_submit_button("🗑️ Eliminar"):
                        execute("DELETE FROM clientes WHERE id=%s", (cli["id"],))
                        cargar_clientes.clear()
                        cargar_deudas_clientes.clear()
                        st.warning("Cliente eliminado.")
                        st.rerun()
        else:
            st.info("No hay clientes registrados.")

    with tab2:
        st.subheader("➕ Nuevo Cliente")
        with st.form("form_nuevo_cliente"):
            c1, c2, c3 = st.columns(3)
            nombre = c1.text_input("Nombre *")
            telefono = c2.text_input("Teléfono")
            email = c3.text_input("Email")
            if st.form_submit_button("✅ Registrar Cliente", type="primary"):
                if not nombre.strip():
                    st.error("El nombre es obligatorio.")
                else:
                    execute(
                        "INSERT INTO clientes (nombre, telefono, email) VALUES (%s,%s,%s)",
                        (nombre.strip(), telefono.strip() or None, email.strip() or None)
                    )
                    cargar_clientes.clear()
                    cargar_deudas_clientes.clear()
                    st.success(f"✅ Cliente '{nombre}' registrado.")
                    st.rerun()