import streamlit as st
from database import query, execute, get_conn
from helpers import fmt_money

@st.cache_data(ttl=10)
def cargar_productos_disponibles():
    return query("SELECT id, nombre, precio, stock FROM productos WHERE stock > 0 ORDER BY nombre")

@st.cache_data(ttl=10)
def cargar_clientes():
    return query("SELECT id, nombre FROM clientes ORDER BY nombre")

# ── NUEVA VENTA ───────────────────────────────────────────────────────────────
def modulo_nueva_venta():
    st.title("🛒 Nueva Venta")

    if "carrito" not in st.session_state:
        st.session_state.carrito = []

    productos = cargar_productos_disponibles()
    clientes  = cargar_clientes()

    col_izq, col_der = st.columns([2, 1])

    with col_izq:
        st.subheader("➕ Agregar Productos")
        if not productos:
            st.warning("No hay productos con stock disponible.")
        else:
            with st.form("form_agregar_item", clear_on_submit=True):
                opts_prod = {
                    f"{p['nombre']} — {fmt_money(p['precio'])} (stock: {p['stock']})": p
                    for p in productos
                }
                sel_prod = st.selectbox("Producto", list(opts_prod.keys()))
                cant = st.number_input("Cantidad", min_value=1, step=1, value=1)
                if st.form_submit_button("➕ Agregar al carrito"):
                    prod = opts_prod[sel_prod]
                    existe = next(
                        (i for i, it in enumerate(st.session_state.carrito)
                         if it["producto_id"] == prod["id"]), None
                    )
                    nueva_cant = cant + (st.session_state.carrito[existe]["cantidad"] if existe is not None else 0)
                    if nueva_cant > prod["stock"]:
                        st.error(f"Stock insuficiente. Disponible: {prod['stock']}")
                    else:
                        if existe is not None:
                            st.session_state.carrito[existe]["cantidad"] = nueva_cant
                            st.session_state.carrito[existe]["subtotal"] = (
                                nueva_cant * st.session_state.carrito[existe]["precio_unitario"]
                            )
                        else:
                            st.session_state.carrito.append({
                                "producto_id": prod["id"],
                                "nombre": prod["nombre"],
                                "precio_unitario": float(prod["precio"]),
                                "cantidad": cant,
                                "subtotal": float(prod["precio"]) * cant,
                            })
                        st.rerun()

        st.subheader("🛒 Carrito")
        if not st.session_state.carrito:
            st.info("El carrito está vacío.")
        else:
            for i, item in enumerate(st.session_state.carrito):
                c1, c2, c3 = st.columns([3, 1, 1])
                c1.write(f"**{item['nombre']}** x{item['cantidad']} @ {fmt_money(item['precio_unitario'])}")
                c2.write(fmt_money(item["subtotal"]))
                if c3.button("❌", key=f"rm_{i}"):
                    st.session_state.carrito.pop(i)
                    st.rerun()
            if st.button("🗑️ Limpiar carrito"):
                st.session_state.carrito = []
                st.rerun()

    with col_der:
        st.subheader("💳 Finalizar Venta")
        subtotal = sum(it["subtotal"] for it in st.session_state.carrito)

        opts_cli = {"— Consumidor Final —": None}
        opts_cli.update({c["nombre"]: c["id"] for c in clientes})
        cliente_sel = st.selectbox("Cliente", list(opts_cli.keys()))
        cliente_id = opts_cli[cliente_sel]

        desc_tipo = st.radio(
            "Descuento",
            ["Sin descuento", "Porcentaje %", "Monto fijo $"],
            horizontal=True,
            key="desc_tipo"
        )
        descuento = 0.0
        if desc_tipo == "Porcentaje %":
            pct = st.number_input(
                "Porcentaje (%)",
                min_value=0.0,
                max_value=100.0,
                step=0.5,
                value=st.session_state.get("desc_pct", 0.0),
                key="desc_pct"
            )
            descuento = subtotal * pct / 100
        elif desc_tipo == "Monto fijo $":
            descuento = st.number_input(
                "Monto descuento ($)",
                min_value=0.0,
                step=0.01,
                value=st.session_state.get("desc_amount", 0.0),
                key="desc_amount"
            )
            if descuento > subtotal:
                st.warning("El descuento no puede superar el subtotal.")
                descuento = subtotal

        total = max(subtotal - descuento, 0)

        st.markdown(f"""
        <div class="summary-card">
            <p style="margin:4px 0">Subtotal: <strong>{fmt_money(subtotal)}</strong></p>
            <p style="margin:4px 0;color:#dc2626">Descuento: <strong>- {fmt_money(descuento)}</strong></p>
            <hr style="margin:8px 0">
            <p style="margin:4px 0;font-size:20px">Total: <strong>{fmt_money(total)}</strong></p>
        </div>
        """, unsafe_allow_html=True)

        metodo = st.selectbox("Método de pago", ["Efectivo", "Tarjeta", "Transferencia", "Cuenta Corriente"])
        es_cc = metodo == "Cuenta Corriente"
        if es_cc and not cliente_id:
            st.warning("⚠️ La cuenta corriente requiere un cliente registrado.")

        notas = st.text_area("Notas (opcional)", height=60)

        if st.button("✅ Confirmar Venta", type="primary", disabled=not st.session_state.carrito):
            if es_cc and not cliente_id:
                st.error("Seleccioná un cliente para cuenta corriente.")
            elif not st.session_state.carrito:
                st.error("El carrito está vacío.")
            else:
                estado_venta = "pendiente" if es_cc else "pagada"
                metodo_real = "Cuenta Corriente" if es_cc else metodo

                conn = get_conn()
                try:
                    with conn.cursor() as cur:
                        cur.execute("""
                            INSERT INTO ventas (cliente_id, subtotal, descuento, total, metodo_pago,
                                                es_cuenta_corriente, estado, notas)
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id
                        """, (cliente_id, subtotal, descuento, total, metodo_real,
                              es_cc, estado_venta, notas))
                        vid = cur.fetchone()["id"]

                        for item in st.session_state.carrito:
                            cur.execute("""
                                INSERT INTO venta_items (venta_id, producto_id, nombre_producto,
                                                          cantidad, precio_unitario, subtotal)
                                VALUES (%s,%s,%s,%s,%s,%s)
                            """, (vid, item["producto_id"], item["nombre"],
                                  item["cantidad"], item["precio_unitario"], item["subtotal"]))
                            cur.execute(
                                "UPDATE productos SET stock = stock - %s WHERE id=%s",
                                (item["cantidad"], item["producto_id"])
                            )
                    conn.commit()
                    cargar_productos_disponibles.clear()
                    st.session_state.carrito = []
                    st.success(f"✅ Venta #{vid} registrada por {fmt_money(total)}")
                    if es_cc:
                        st.info("📋 Factura agregada a cuenta corriente.")
                    st.rerun()
                except Exception as e:
                    conn.rollback()
                    st.error(f"Error al registrar la venta: {e}")
                finally:
                    conn.close()