import streamlit as st
import pandas as pd
from database import query, execute
from helpers import fmt_money, estado_stock

@st.cache_data(ttl=10)
def cargar_productos_inventario():
    return query("SELECT id, nombre, precio, stock, stock_minimo FROM productos ORDER BY nombre")

# ── INVENTARIO ────────────────────────────────────────────────────────────────
def modulo_inventario():
    st.title("📦 Gestión de Inventario")
    tab1, tab2 = st.tabs(["📋 Listado", "➕ Agregar / Editar"])

    with tab1:
        productos = cargar_productos_inventario()
        if productos:
            buscar = st.text_input("🔍 Buscar producto", placeholder="Nombre...")
            filas = []
            for p in productos:
                if buscar and buscar.lower() not in p["nombre"].lower():
                    continue
                estado, _ = estado_stock(p["stock"], p["stock_minimo"])
                filas.append({
                    "ID": p["id"],
                    "Producto": p["nombre"],
                    "Precio": fmt_money(p["precio"]),
                    "Stock": p["stock"],
                    "Mínimo": p["stock_minimo"],
                    "Estado": estado,
                })
            if filas:
                st.dataframe(pd.DataFrame(filas), use_container_width=True, hide_index=True)

            st.markdown("---")
            st.subheader("✏️ Editar / Eliminar producto")
            opciones = {f"{p['nombre']} (ID {p['id']})": p for p in productos}
            sel = st.selectbox("Seleccionar producto", list(opciones.keys()))
            if sel:
                prod = opciones[sel]
                with st.form("form_editar"):
                    c1, c2, c3, c4 = st.columns(4)
                    nombre_e = c1.text_input("Nombre", value=prod["nombre"])
                    precio_e = c2.number_input("Precio", value=float(prod["precio"]), min_value=0.0, step=0.01)
                    stock_e  = c3.number_input("Stock", value=int(prod["stock"]), min_value=0, step=1)
                    minimo_e = c4.number_input("Stock mínimo", value=int(prod["stock_minimo"]), min_value=0, step=1)
                    b1, b2 = st.columns(2)
                    if b1.form_submit_button("💾 Guardar cambios", type="primary"):
                        execute(
                            "UPDATE productos SET nombre=%s, precio=%s, stock=%s, stock_minimo=%s WHERE id=%s",
                            (nombre_e, precio_e, stock_e, minimo_e, prod["id"])
                        )
                        st.success("✅ Producto actualizado.")
                        st.rerun()
                    if b2.form_submit_button("🗑️ Eliminar producto"):
                        execute("DELETE FROM productos WHERE id=%s", (prod["id"],))
                        st.warning("Producto eliminado.")
                        st.rerun()
        else:
            st.info("No hay productos en el inventario.")

    with tab2:
        st.subheader("➕ Nuevo Producto")
        with st.form("form_nuevo_producto"):
            c1, c2, c3, c4 = st.columns(4)
            nombre = c1.text_input("Nombre *", placeholder="Ej: Harina 1kg")
            precio = c2.number_input("Precio *", min_value=0.0, step=0.01, value=0.0)
            stock  = c3.number_input("Stock inicial", min_value=0, step=1, value=0)
            minimo = c4.number_input("Stock mínimo", min_value=0, step=1, value=5)
            if st.form_submit_button("✅ Agregar Producto", type="primary"):
                if not nombre.strip():
                    st.error("El nombre es obligatorio.")
                elif precio <= 0:
                    st.error("El precio debe ser mayor a cero.")
                else:
                    execute(
                        "INSERT INTO productos (nombre, precio, stock, stock_minimo) VALUES (%s,%s,%s,%s)",
                        (nombre.strip(), precio, stock, minimo)
                    )
                    st.success(f"✅ Producto '{nombre}' agregado.")
                    st.rerun()