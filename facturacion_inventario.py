"""
Sistema de Facturación y Gestión de Inventario — Con Cuenta Corriente
Dependencias: pip install streamlit pandas
Ejecutar:     python -m streamlit run facturacion_inventario.py --server.address 0.0.0.0
"""

import streamlit as st
import sqlite3
import pandas as pd
from datetime import datetime, date
from pathlib import Path

st.set_page_config(
    page_title="Sistema de Facturación",
    page_icon="🧾",
    layout="wide",
    initial_sidebar_state="expanded",
)

DB_PATH = Path("inventario.db")

# ─────────────────────────────────────────
# BASE DE DATOS
# ─────────────────────────────────────────
def get_connection():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    with get_connection() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS productos (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                nombre    TEXT    NOT NULL UNIQUE,
                precio    REAL    NOT NULL CHECK(precio > 0),
                stock     INTEGER NOT NULL CHECK(stock >= 0),
                creado_en TEXT    DEFAULT (datetime('now','localtime'))
            );
            CREATE TABLE IF NOT EXISTS clientes (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                nombre    TEXT    NOT NULL UNIQUE,
                telefono  TEXT,
                email     TEXT,
                creado_en TEXT    DEFAULT (datetime('now','localtime'))
            );
            CREATE TABLE IF NOT EXISTS ventas (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                cliente     TEXT,
                cliente_id  INTEGER REFERENCES clientes(id),
                metodo_pago TEXT,
                descuento   REAL DEFAULT 0,
                total       REAL NOT NULL,
                es_cuenta_corriente INTEGER DEFAULT 0,
                fecha       TEXT DEFAULT (datetime('now','localtime'))
            );
            CREATE TABLE IF NOT EXISTS venta_items (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                venta_id    INTEGER NOT NULL REFERENCES ventas(id),
                producto_id INTEGER NOT NULL REFERENCES productos(id),
                cantidad    INTEGER NOT NULL,
                precio_unit REAL    NOT NULL,
                subtotal    REAL    NOT NULL
            );
            CREATE TABLE IF NOT EXISTS cuenta_corriente (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                cliente_id  INTEGER NOT NULL REFERENCES clientes(id),
                venta_id    INTEGER NOT NULL REFERENCES ventas(id),
                total       REAL NOT NULL,
                saldo       REAL NOT NULL,
                estado      TEXT DEFAULT 'pendiente',
                dias_alerta INTEGER DEFAULT 30,
                fecha       TEXT DEFAULT (datetime('now','localtime'))
            );
            CREATE TABLE IF NOT EXISTS pagos_cc (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                cuenta_id       INTEGER NOT NULL REFERENCES cuenta_corriente(id),
                cliente_id      INTEGER NOT NULL REFERENCES clientes(id),
                monto           REAL NOT NULL,
                metodo_pago     TEXT,
                observacion     TEXT,
                fecha           TEXT DEFAULT (datetime('now','localtime'))
            );
        """)

# ─────────────────────────────────────────
# PRODUCTOS
# ─────────────────────────────────────────
def agregar_producto(nombre, precio, stock):
    try:
        with get_connection() as conn:
            conn.execute(
                "INSERT INTO productos (nombre, precio, stock) VALUES (?, ?, ?)",
                (nombre.strip(), precio, stock)
            )
        return True, f"Producto **{nombre}** agregado con éxito."
    except sqlite3.IntegrityError:
        return False, f"El producto **{nombre}** ya existe."

def actualizar_producto(producto_id, precio, stock):
    with get_connection() as conn:
        conn.execute("UPDATE productos SET precio=?, stock=? WHERE id=?", (precio, stock, producto_id))
    return True, "Producto actualizado."

def eliminar_producto(producto_id):
    with get_connection() as conn:
        conn.execute("DELETE FROM productos WHERE id=?", (producto_id,))
    return True, "Producto eliminado."

def get_productos():
    with get_connection() as conn:
        return pd.read_sql_query(
            "SELECT id, nombre, precio, stock, creado_en FROM productos ORDER BY nombre", conn
        )

# ─────────────────────────────────────────
# CLIENTES
# ─────────────────────────────────────────
def agregar_cliente(nombre, telefono="", email=""):
    try:
        with get_connection() as conn:
            conn.execute(
                "INSERT INTO clientes (nombre, telefono, email) VALUES (?, ?, ?)",
                (nombre.strip(), telefono.strip(), email.strip())
            )
        return True, f"Cliente **{nombre}** agregado."
    except sqlite3.IntegrityError:
        return False, f"El cliente **{nombre}** ya existe."

def get_clientes():
    with get_connection() as conn:
        return pd.read_sql_query(
            "SELECT id, nombre, telefono, email FROM clientes ORDER BY nombre", conn
        )

def get_cliente_id(nombre):
    with get_connection() as conn:
        row = conn.execute("SELECT id FROM clientes WHERE nombre=?", (nombre,)).fetchone()
        return row["id"] if row else None

# ─────────────────────────────────────────
# VENTAS
# ─────────────────────────────────────────
def registrar_venta_completa(cliente, metodo_pago, descuento_pct, items, es_cc=False, cliente_id=None, dias_alerta=30):
    conn = get_connection()
    try:
        conn.execute("BEGIN")
        for item in items:
            row = conn.execute("SELECT stock FROM productos WHERE id=?", (item["producto_id"],)).fetchone()
            if not row or row["stock"] < item["cantidad"]:
                conn.execute("ROLLBACK")
                return False, f"Stock insuficiente para '{item['nombre']}'."

        subtotal_bruto  = sum(i["cantidad"] * i["precio_unit"] for i in items)
        descuento_monto = subtotal_bruto * (descuento_pct / 100)
        total           = subtotal_bruto - descuento_monto

        cur = conn.execute(
            "INSERT INTO ventas (cliente, cliente_id, metodo_pago, descuento, total, es_cuenta_corriente) VALUES (?,?,?,?,?,?)",
            (cliente, cliente_id, metodo_pago, descuento_pct, total, 1 if es_cc else 0)
        )
        venta_id = cur.lastrowid

        for item in items:
            conn.execute(
                "INSERT INTO venta_items (venta_id, producto_id, cantidad, precio_unit, subtotal) VALUES (?,?,?,?,?)",
                (venta_id, item["producto_id"], item["cantidad"], item["precio_unit"], item["cantidad"] * item["precio_unit"])
            )
            conn.execute("UPDATE productos SET stock = stock - ? WHERE id=?", (item["cantidad"], item["producto_id"]))

        if es_cc and cliente_id:
            conn.execute(
                "INSERT INTO cuenta_corriente (cliente_id, venta_id, total, saldo, estado, dias_alerta) VALUES (?,?,?,?,?,?)",
                (cliente_id, venta_id, total, total, "pendiente", dias_alerta)
            )

        conn.execute("COMMIT")
        return True, {
            "venta_id": venta_id, "cliente": cliente, "metodo_pago": metodo_pago,
            "items": items, "subtotal_bruto": subtotal_bruto,
            "descuento_pct": descuento_pct, "descuento_monto": descuento_monto,
            "total": total, "es_cc": es_cc,
            "fecha": datetime.now().strftime("%d/%m/%Y %H:%M"),
        }
    except Exception as e:
        conn.execute("ROLLBACK")
        return False, str(e)
    finally:
        conn.close()

def get_ventas_recientes(limit=100):
    with get_connection() as conn:
        return pd.read_sql_query(f"""
            SELECT id, cliente, metodo_pago, descuento || '%' AS descuento,
                   total, es_cuenta_corriente, fecha
            FROM ventas ORDER BY fecha DESC LIMIT {limit}
        """, conn)

def get_items_por_venta(venta_id):
    with get_connection() as conn:
        return pd.read_sql_query("""
            SELECT p.nombre, vi.cantidad, vi.precio_unit, vi.subtotal
            FROM venta_items vi JOIN productos p ON vi.producto_id = p.id
            WHERE vi.venta_id = ?
        """, conn, params=(venta_id,))

# ─────────────────────────────────────────
# CUENTA CORRIENTE
# ─────────────────────────────────────────
def get_cuentas_pendientes():
    with get_connection() as conn:
        return pd.read_sql_query("""
            SELECT cc.id, c.nombre AS cliente, cc.venta_id,
                   cc.total, cc.saldo, cc.estado, cc.dias_alerta,
                   cc.fecha,
                   CAST(julianday('now','localtime') - julianday(cc.fecha) AS INTEGER) AS dias_transcurridos
            FROM cuenta_corriente cc
            JOIN clientes c ON cc.cliente_id = c.id
            WHERE cc.estado IN ('pendiente','parcial')
            ORDER BY dias_transcurridos DESC
        """, conn)

def get_cuentas_por_cliente(cliente_id):
    with get_connection() as conn:
        return pd.read_sql_query("""
            SELECT cc.id, cc.venta_id, cc.total, cc.saldo, cc.estado,
                   cc.fecha,
                   CAST(julianday('now','localtime') - julianday(cc.fecha) AS INTEGER) AS dias_transcurridos
            FROM cuenta_corriente cc
            WHERE cc.cliente_id = ?
            ORDER BY cc.fecha DESC
        """, conn, params=(cliente_id,))

def get_historial_pagos(cliente_id):
    with get_connection() as conn:
        return pd.read_sql_query("""
            SELECT p.fecha, cc.venta_id, p.monto, p.metodo_pago, p.observacion
            FROM pagos_cc p
            JOIN cuenta_corriente cc ON p.cuenta_id = cc.id
            WHERE p.cliente_id = ?
            ORDER BY p.fecha DESC
        """, conn, params=(cliente_id,))

def registrar_pago_cc(cuenta_id, cliente_id, monto, metodo_pago, observacion=""):
    conn = get_connection()
    try:
        conn.execute("BEGIN")
        row = conn.execute(
            "SELECT saldo, total FROM cuenta_corriente WHERE id=?", (cuenta_id,)
        ).fetchone()
        if not row:
            conn.execute("ROLLBACK")
            return False, "Cuenta no encontrada."
        saldo_actual = row["saldo"]
        if monto > saldo_actual:
            conn.execute("ROLLBACK")
            return False, f"El monto supera el saldo pendiente (${saldo_actual:,.2f})."

        nuevo_saldo = saldo_actual - monto
        nuevo_estado = "pagado" if nuevo_saldo <= 0 else "parcial"

        conn.execute(
            "UPDATE cuenta_corriente SET saldo=?, estado=? WHERE id=?",
            (nuevo_saldo, nuevo_estado, cuenta_id)
        )
        conn.execute(
            "INSERT INTO pagos_cc (cuenta_id, cliente_id, monto, metodo_pago, observacion) VALUES (?,?,?,?,?)",
            (cuenta_id, cliente_id, monto, metodo_pago, observacion)
        )
        conn.execute("COMMIT")
        return True, f"Pago de ${monto:,.2f} registrado. Saldo restante: ${nuevo_saldo:,.2f}"
    except Exception as e:
        conn.execute("ROLLBACK")
        return False, str(e)
    finally:
        conn.close()

def get_resumen_cc():
    with get_connection() as conn:
        return {
            "clientes_deudores": conn.execute(
                "SELECT COUNT(DISTINCT cliente_id) FROM cuenta_corriente WHERE estado IN ('pendiente','parcial')"
            ).fetchone()[0],
            "total_deuda": conn.execute(
                "SELECT COALESCE(SUM(saldo),0) FROM cuenta_corriente WHERE estado IN ('pendiente','parcial')"
            ).fetchone()[0],
            "facturas_vencidas": conn.execute(
                """SELECT COUNT(*) FROM cuenta_corriente
                   WHERE estado IN ('pendiente','parcial')
                   AND CAST(julianday('now','localtime') - julianday(fecha) AS INTEGER) > dias_alerta"""
            ).fetchone()[0],
            "cobrado_hoy": conn.execute(
                "SELECT COALESCE(SUM(monto),0) FROM pagos_cc WHERE date(fecha)=date('now','localtime')"
            ).fetchone()[0],
        }

def get_stats():
    with get_connection() as conn:
        return {
            "total_productos":  conn.execute("SELECT COUNT(*) FROM productos").fetchone()[0],
            "sin_stock":        conn.execute("SELECT COUNT(*) FROM productos WHERE stock=0").fetchone()[0],
            "stock_bajo":       conn.execute("SELECT COUNT(*) FROM productos WHERE stock>0 AND stock<=5").fetchone()[0],
            "ventas_total":     conn.execute("SELECT COALESCE(SUM(total),0) FROM ventas").fetchone()[0],
            "ventas_hoy":       conn.execute("SELECT COALESCE(SUM(total),0) FROM ventas WHERE date(fecha)=date('now','localtime')").fetchone()[0],
            "cant_ventas_hoy":  conn.execute("SELECT COUNT(*) FROM ventas WHERE date(fecha)=date('now','localtime')").fetchone()[0],
            "deuda_total":      conn.execute("SELECT COALESCE(SUM(saldo),0) FROM cuenta_corriente WHERE estado IN ('pendiente','parcial')").fetchone()[0],
            "cc_vencidas":      conn.execute(
                """SELECT COUNT(*) FROM cuenta_corriente
                   WHERE estado IN ('pendiente','parcial')
                   AND CAST(julianday('now','localtime') - julianday(fecha) AS INTEGER) > dias_alerta"""
            ).fetchone()[0],
        }

# ─────────────────────────────────────────
# INICIALIZACIÓN
# ─────────────────────────────────────────
init_db()

if "carrito" not in st.session_state:
    st.session_state.carrito = []
if "ultima_factura" not in st.session_state:
    st.session_state.ultima_factura = None

# ─────────────────────────────────────────
# SIDEBAR
# ─────────────────────────────────────────
with st.sidebar:
    st.title("🧾 FacturApp")
    st.divider()
    modulo = st.radio(
        "Navegación",
        ["📦 Inventario", "👥 Clientes", "🛒 Nueva Venta",
         "💳 Cuenta Corriente", "📋 Historial de Ventas"],
        label_visibility="collapsed"
    )
    st.divider()
    stats = get_stats()
    st.write(f"**Productos:** {stats['total_productos']}")
    st.write(f"**Sin stock:** {stats['sin_stock']}")
    st.write(f"**Stock crítico:** {stats['stock_bajo']}")
    st.divider()
    st.write(f"**Ventas hoy:** {stats['cant_ventas_hoy']}")
    st.write(f"**Total hoy:** ${stats['ventas_hoy']:,.2f}")
    st.divider()
    if stats["cc_vencidas"] > 0:
        st.error(f"⚠️ {stats['cc_vencidas']} facturas vencidas")
    st.write(f"**Deuda total CC:** ${stats['deuda_total']:,.2f}")


# ═══════════════════════════════════════════
# MÓDULO: INVENTARIO
# ═══════════════════════════════════════════
if modulo == "📦 Inventario":
    st.title("📦 Gestión de Inventario")

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total productos",    stats["total_productos"])
    c2.metric("Sin stock",          stats["sin_stock"])
    c3.metric("Stock crítico (≤5)", stats["stock_bajo"])
    c4.metric("Ventas totales",     f"${stats['ventas_total']:,.2f}")
    st.divider()

    with st.expander("➕ Agregar nuevo producto"):
        with st.form("form_agregar", clear_on_submit=True):
            col1, col2, col3 = st.columns(3)
            nombre = col1.text_input("Nombre del producto")
            precio = col2.number_input("Precio ($)", min_value=0.01, step=0.01, format="%.2f")
            stock  = col3.number_input("Stock inicial", min_value=0, step=1)
            if st.form_submit_button("Agregar producto", use_container_width=True):
                if not nombre.strip():
                    st.error("El nombre no puede estar vacío.")
                else:
                    ok, msg = agregar_producto(nombre, precio, int(stock))
                    st.success(msg) if ok else st.warning(msg)
                    st.rerun()

    st.subheader("Inventario actual")
    df = get_productos()
    if df.empty:
        st.info("No hay productos cargados todavía.")
    else:
        def estado(row):
            if row["stock"] == 0:  return "⛔ Sin stock"
            if row["stock"] <= 5:  return "⚠️ Crítico"
            if row["stock"] <= 20: return "🔶 Bajo"
            return "✅ Normal"
        df_show = df.copy()
        df_show["Estado"] = df_show.apply(estado, axis=1)
        df_show["precio"] = df_show["precio"].apply(lambda x: f"${x:,.2f}")
        df_show = df_show.rename(columns={"id":"ID","nombre":"Producto","precio":"Precio","stock":"Stock","creado_en":"Creado"})
        st.dataframe(df_show[["ID","Producto","Precio","Stock","Estado","Creado"]], use_container_width=True, hide_index=True)

    st.subheader("Editar / Eliminar producto")
    df_raw = get_productos()
    if not df_raw.empty:
        sel = st.selectbox("Seleccioná un producto", df_raw["nombre"].tolist())
        if sel:
            fila = df_raw[df_raw["nombre"] == sel].iloc[0]
            pid  = int(fila["id"])
            with st.form("form_editar"):
                e1, e2 = st.columns(2)
                np_ = e1.number_input("Nuevo precio", value=float(fila["precio"]), step=0.01, format="%.2f")
                ns_ = e2.number_input("Nuevo stock",  value=int(fila["stock"]),   step=1, min_value=0)
                if st.form_submit_button("💾 Guardar cambios", use_container_width=True):
                    ok, msg = actualizar_producto(pid, np_, ns_)
                    st.success(msg)
                    st.rerun()
            if st.button("🗑️ Eliminar este producto"):
                ok, msg = eliminar_producto(pid)
                st.success(msg)
                st.rerun()


# ═══════════════════════════════════════════
# MÓDULO: CLIENTES
# ═══════════════════════════════════════════
elif modulo == "👥 Clientes":
    st.title("👥 Gestión de Clientes")

    with st.expander("➕ Agregar nuevo cliente"):
        with st.form("form_cliente", clear_on_submit=True):
            c1, c2, c3 = st.columns(3)
            nom = c1.text_input("Nombre / Razón social")
            tel = c2.text_input("Teléfono")
            mail= c3.text_input("Email")
            if st.form_submit_button("Agregar cliente", use_container_width=True):
                if not nom.strip():
                    st.error("El nombre no puede estar vacío.")
                else:
                    ok, msg = agregar_cliente(nom, tel, mail)
                    st.success(msg) if ok else st.warning(msg)
                    st.rerun()

    st.subheader("Clientes registrados")
    df_cl = get_clientes()
    if df_cl.empty:
        st.info("No hay clientes registrados todavía.")
    else:
        df_cl_show = df_cl.rename(columns={"id":"ID","nombre":"Cliente","telefono":"Teléfono","email":"Email"})
        st.dataframe(df_cl_show, use_container_width=True, hide_index=True)

        st.subheader("Estado de cuenta por cliente")
        sel_cl = st.selectbox("Seleccioná un cliente", df_cl["nombre"].tolist(), key="sel_cl_estado")
        if sel_cl:
            cid = get_cliente_id(sel_cl)
            df_cc = get_cuentas_por_cliente(cid)
            df_pag = get_historial_pagos(cid)

            total_deuda = df_cc[df_cc["estado"].isin(["pendiente","parcial"])]["saldo"].sum() if not df_cc.empty else 0
            m1, m2, m3 = st.columns(3)
            m1.metric("Deuda total", f"${total_deuda:,.2f}")
            m2.metric("Facturas pendientes", len(df_cc[df_cc["estado"].isin(["pendiente","parcial"])]) if not df_cc.empty else 0)
            m3.metric("Pagos realizados", len(df_pag) if not df_pag.empty else 0)

            if not df_cc.empty:
                st.write("**Facturas:**")
                df_cc_show = df_cc.copy()
                df_cc_show["total"]  = df_cc_show["total"].apply(lambda x: f"${x:,.2f}")
                df_cc_show["saldo"]  = df_cc_show["saldo"].apply(lambda x: f"${x:,.2f}")
                df_cc_show.columns   = ["ID","Venta N°","Total","Saldo","Estado","Fecha","Días"]
                st.dataframe(df_cc_show, use_container_width=True, hide_index=True)

            if not df_pag.empty:
                st.write("**Historial de pagos:**")
                df_pag["monto"] = df_pag["monto"].apply(lambda x: f"${x:,.2f}")
                df_pag.columns  = ["Fecha","Venta N°","Monto","Método","Observación"]
                st.dataframe(df_pag, use_container_width=True, hide_index=True)


# ═══════════════════════════════════════════
# MÓDULO: NUEVA VENTA
# ═══════════════════════════════════════════
elif modulo == "🛒 Nueva Venta":
    st.title("🛒 Nueva Venta")

    df_prod = get_productos()
    if df_prod.empty:
        st.warning("No hay productos en inventario. Cargá productos primero.")
        st.stop()

    # ── Factura confirmada ──
    if st.session_state.ultima_factura:
        f = st.session_state.ultima_factura
        tipo = "💳 CUENTA CORRIENTE" if f["es_cc"] else "✅ VENTA AL CONTADO"
        st.success(f"{tipo} — Venta #{f['venta_id']} registrada correctamente")
        st.subheader(f"FACTURA — N° {f['venta_id']:06d}")
        st.write(f"📅 {f['fecha']}   |   👤 {f['cliente']}   |   💳 {f['metodo_pago']}")
        if f["es_cc"]:
            st.warning("⏳ Esta venta quedó registrada en cuenta corriente — pago pendiente.")
        st.divider()

        rows = []
        for item in f["items"]:
            rows.append({
                "Producto":  item["nombre"],
                "Cantidad":  item["cantidad"],
                "Precio u.": f"${item['precio_unit']:,.2f}",
                "Subtotal":  f"${item['cantidad'] * item['precio_unit']:,.2f}",
            })
        st.dataframe(pd.DataFrame(rows), hide_index=True, use_container_width=True)
        st.divider()

        _, col_v = st.columns([2, 1])
        with col_v:
            st.write(f"Subtotal:  **${f['subtotal_bruto']:,.2f}**")
            if f["descuento_pct"] > 0:
                st.write(f"Descuento ({f['descuento_pct']}%):  **-${f['descuento_monto']:,.2f}**")
            st.markdown(f"### TOTAL: ${f['total']:,.2f}")

        st.divider()
        if st.button("🔄 Nueva venta", use_container_width=True):
            st.session_state.ultima_factura = None
            st.session_state.carrito = []
            st.rerun()
        st.stop()

    # ── Datos del cliente ──
    st.subheader("Datos del cliente")
    df_clientes = get_clientes()
    col_c1, col_c2, col_c3 = st.columns(3)

    es_cc = col_c1.checkbox("💳 Venta en Cuenta Corriente")

    if es_cc and not df_clientes.empty:
        nombre_cl = col_c2.selectbox("Cliente", df_clientes["nombre"].tolist())
        cliente_id = get_cliente_id(nombre_cl)
        cliente = nombre_cl
        dias_alerta = col_c3.number_input("Alertar a los (días)", min_value=1, max_value=365, value=30, step=1)
        metodo_pago = "Cuenta Corriente"
        descuento = 0.0
    elif es_cc and df_clientes.empty:
        st.warning("No hay clientes registrados. Primero agregá un cliente en el módulo **👥 Clientes**.")
        st.stop()
    else:
        opciones_cl = ["Consumidor final"] + (df_clientes["nombre"].tolist() if not df_clientes.empty else [])
        cliente = col_c2.selectbox("Cliente", opciones_cl)
        cliente_id = get_cliente_id(cliente) if cliente != "Consumidor final" else None
        metodo_pago = col_c3.selectbox("Método de pago", ["Efectivo", "Tarjeta de débito", "Tarjeta de crédito", "Transferencia"])
        descuento = st.number_input("Descuento (%)", min_value=0.0, max_value=100.0, step=0.5, format="%.1f")
        dias_alerta = 30

    st.divider()

    # ── Agregar productos ──
    st.subheader("Agregar productos")
    with st.form("form_agregar_item", clear_on_submit=True):
        fi1, fi2, fi3 = st.columns([3, 1, 1])
        prod_elegido  = fi1.selectbox(
            "Producto", df_prod["nombre"].tolist(),
            format_func=lambda x: (
                f"{x}  ✅" if int(df_prod[df_prod['nombre']==x]['stock'].values[0]) > 0
                else f"{x}  ⛔ sin stock"
            )
        )
        fila_elegida   = df_prod[df_prod["nombre"] == prod_elegido].iloc[0]
        stock_elegido  = int(fila_elegida["stock"])
        precio_elegido = float(fila_elegida["precio"])
        fi2.metric("Stock",  stock_elegido)
        fi3.metric("Precio", f"${precio_elegido:,.2f}")
        qty = st.number_input("Cantidad", min_value=1, max_value=max(stock_elegido,1), step=1, disabled=(stock_elegido == 0))
        agregar_btn = st.form_submit_button("➕ Agregar al carrito", use_container_width=True, disabled=(stock_elegido == 0))

    if agregar_btn:
        ya_en_carrito = sum(i["cantidad"] for i in st.session_state.carrito if i["producto_id"] == int(fila_elegida["id"]))
        if ya_en_carrito + qty > stock_elegido:
            st.error(f"❌ Stock insuficiente. Disponible: {stock_elegido}, ya en carrito: {ya_en_carrito}.")
        else:
            existe = False
            for item in st.session_state.carrito:
                if item["producto_id"] == int(fila_elegida["id"]):
                    item["cantidad"] += qty
                    existe = True
                    break
            if not existe:
                st.session_state.carrito.append({
                    "producto_id": int(fila_elegida["id"]),
                    "nombre":      prod_elegido,
                    "cantidad":    qty,
                    "precio_unit": precio_elegido,
                })
            st.rerun()

    st.divider()

    # ── Carrito ──
    st.subheader("🛒 Carrito")
    if not st.session_state.carrito:
        st.info("El carrito está vacío. Agregá productos arriba.")
    else:
        for idx, item in enumerate(st.session_state.carrito):
            col_n, col_q, col_p, col_s, col_x = st.columns([3, 1, 2, 2, 1])
            col_n.write(f"**{item['nombre']}**")
            col_q.write(f"{item['cantidad']} u.")
            col_p.write(f"${item['precio_unit']:,.2f}")
            col_s.write(f"**${item['cantidad'] * item['precio_unit']:,.2f}**")
            if col_x.button("🗑️", key=f"del_{idx}"):
                st.session_state.carrito.pop(idx)
                st.rerun()

        st.divider()
        subtotal_bruto  = sum(i["cantidad"] * i["precio_unit"] for i in st.session_state.carrito)
        descuento_monto = subtotal_bruto * (descuento / 100)
        total           = subtotal_bruto - descuento_monto

        _, col_res = st.columns([2, 1])
        with col_res:
            st.write(f"Subtotal:  **${subtotal_bruto:,.2f}**")
            if descuento > 0:
                st.write(f"Descuento ({descuento}%):  **-${descuento_monto:,.2f}**")
            st.markdown(f"### Total: ${total:,.2f}")
            if es_cc:
                st.info("💳 Este total quedará pendiente de pago en cuenta corriente.")

        st.divider()
        cola, colb = st.columns(2)
        if cola.button("🗑️ Vaciar carrito", use_container_width=True):
            st.session_state.carrito = []
            st.rerun()
        label_btn = "💳 Confirmar — Cuenta Corriente" if es_cc else "✅ Confirmar venta"
        if colb.button(label_btn, use_container_width=True, type="primary"):
            ok, resultado = registrar_venta_completa(
                cliente, metodo_pago, descuento,
                st.session_state.carrito,
                es_cc=es_cc, cliente_id=cliente_id, dias_alerta=int(dias_alerta) if es_cc else 30
            )
            if ok:
                st.session_state.ultima_factura = resultado
                st.rerun()
            else:
                st.error(f"❌ {resultado}")

    st.divider()
    st.subheader("Stock actualizado")
    df_act = get_productos()
    if not df_act.empty:
        df_mini = df_act[["nombre","precio","stock"]].copy()
        df_mini.columns = ["Producto","Precio","Stock"]
        df_mini["Precio"] = df_mini["Precio"].apply(lambda x: f"${x:,.2f}")
        st.dataframe(df_mini, use_container_width=True, hide_index=True)


# ═══════════════════════════════════════════
# MÓDULO: CUENTA CORRIENTE
# ═══════════════════════════════════════════
elif modulo == "💳 Cuenta Corriente":
    st.title("💳 Cuenta Corriente")

    resumen = get_resumen_cc()
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Clientes deudores",  resumen["clientes_deudores"])
    c2.metric("Deuda total",        f"${resumen['total_deuda']:,.2f}")
    c3.metric("Facturas vencidas",  resumen["facturas_vencidas"],
              delta="⚠️ Vencidas" if resumen["facturas_vencidas"] > 0 else None,
              delta_color="inverse")
    c4.metric("Cobrado hoy",        f"${resumen['cobrado_hoy']:,.2f}")
    st.divider()

    # ── Todas las facturas pendientes ──
    st.subheader("📋 Facturas pendientes de pago")
    df_cc = get_cuentas_pendientes()

    if df_cc.empty:
        st.success("✅ No hay facturas pendientes de pago.")
    else:
        # Colorear según días transcurridos
        def badge_dias(row):
            d = row["dias_transcurridos"]
            alerta = row["dias_alerta"]
            if d > alerta:      return f"🔴 {d} días — VENCIDA"
            if d > alerta * 0.7: return f"🟡 {d} días"
            return f"🟢 {d} días"

        df_show = df_cc.copy()
        df_show["estado_dias"] = df_show.apply(badge_dias, axis=1)
        df_show["total"]  = df_show["total"].apply(lambda x: f"${x:,.2f}")
        df_show["saldo"]  = df_show["saldo"].apply(lambda x: f"${x:,.2f}")
        df_show = df_show[["cliente","venta_id","total","saldo","estado","estado_dias","fecha"]]
        df_show.columns   = ["Cliente","Venta N°","Total","Saldo pendiente","Estado","Tiempo","Fecha"]
        st.dataframe(df_show, use_container_width=True, hide_index=True)

        # Alertas de vencidas
        vencidas = df_cc[df_cc["dias_transcurridos"] > df_cc["dias_alerta"]]
        if not vencidas.empty:
            st.error(f"⚠️ Hay **{len(vencidas)}** factura(s) vencidas:")
            for _, v in vencidas.iterrows():
                st.write(f"- **{v['cliente']}** — Venta #{v['venta_id']} — Saldo: ${v['saldo']:,.2f} — {v['dias_transcurridos']} días sin pagar")

    st.divider()

    # ── Registrar pago ──
    st.subheader("💰 Registrar pago")
    df_clientes = get_clientes()

    if df_clientes.empty:
        st.info("No hay clientes registrados.")
    else:
        col_p1, col_p2 = st.columns(2)
        cliente_pago = col_p1.selectbox("Cliente", df_clientes["nombre"].tolist(), key="cl_pago")
        cid_pago = get_cliente_id(cliente_pago)
        df_cuentas_cl = get_cuentas_por_cliente(cid_pago)
        df_pend = df_cuentas_cl[df_cuentas_cl["estado"].isin(["pendiente","parcial"])] if not df_cuentas_cl.empty else pd.DataFrame()

        if df_pend.empty:
            col_p2.info("Este cliente no tiene deudas pendientes.")
        else:
            deuda_total_cl = df_pend["saldo"].sum()
            col_p2.metric("Deuda total del cliente", f"${deuda_total_cl:,.2f}")

            opciones_fact = {
                f"Venta #{r['venta_id']} — Saldo: ${r['saldo']:,.2f} ({r['dias_transcurridos']} días)": r["id"]
                for _, r in df_pend.iterrows()
            }

            with st.form("form_pago_cc"):
                fact_sel_label = st.selectbox("Factura a pagar", list(opciones_fact.keys()))
                cuenta_id_sel  = opciones_fact[fact_sel_label]
                saldo_sel = float(df_pend[df_pend["id"] == cuenta_id_sel]["saldo"].values[0])

                p1, p2, p3 = st.columns(3)
                monto_pago  = p1.number_input("Monto del pago ($)", min_value=0.01, max_value=float(saldo_sel), value=float(saldo_sel), step=0.01, format="%.2f")
                metodo_pago = p2.selectbox("Método de pago", ["Efectivo","Transferencia","Tarjeta de débito","Tarjeta de crédito"])
                observacion = p3.text_input("Observación (opcional)")

                if st.form_submit_button("💾 Registrar pago", use_container_width=True, type="primary"):
                    ok, msg = registrar_pago_cc(cuenta_id_sel, cid_pago, monto_pago, metodo_pago, observacion)
                    if ok:
                        st.success(f"✅ {msg}")
                        st.rerun()
                    else:
                        st.error(f"❌ {msg}")

    st.divider()

    # ── Historial de cobros del día ──
    st.subheader("📅 Cobros del día")
    with get_connection() as conn:
        df_hoy = pd.read_sql_query("""
            SELECT c.nombre AS cliente, p.monto, p.metodo_pago, p.observacion, p.fecha
            FROM pagos_cc p
            JOIN clientes c ON p.cliente_id = c.id
            WHERE date(p.fecha) = date('now','localtime')
            ORDER BY p.fecha DESC
        """, conn)
    if df_hoy.empty:
        st.info("No se registraron cobros hoy.")
    else:
        df_hoy["monto"] = df_hoy["monto"].apply(lambda x: f"${x:,.2f}")
        df_hoy.columns  = ["Cliente","Monto","Método","Observación","Hora"]
        st.dataframe(df_hoy, use_container_width=True, hide_index=True)


# ═══════════════════════════════════════════
# MÓDULO: HISTORIAL
# ═══════════════════════════════════════════
elif modulo == "📋 Historial de Ventas":
    st.title("📋 Historial de Ventas")

    stats = get_stats()
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Ventas hoy",        stats["cant_ventas_hoy"])
    c2.metric("Total hoy",         f"${stats['ventas_hoy']:,.2f}")
    c3.metric("Total acumulado",   f"${stats['ventas_total']:,.2f}")
    c4.metric("Productos activos", stats["total_productos"])
    st.divider()

    df_v = get_ventas_recientes(100)
    if df_v.empty:
        st.info("Aún no se han registrado ventas.")
    else:
        df_show = df_v.copy()
        df_show["total"] = df_show["total"].apply(lambda x: f"${x:,.2f}")
        df_show["es_cuenta_corriente"] = df_show["es_cuenta_corriente"].apply(lambda x: "💳 CC" if x else "✅ Contado")
        df_show.columns = ["ID","Cliente","Método de pago","Descuento","Total","Tipo","Fecha"]
        st.dataframe(df_show, use_container_width=True, hide_index=True)

        st.subheader("Ver detalle de una venta")
        venta_sel = st.selectbox("Seleccioná una venta por ID", df_v["id"].tolist())
        if venta_sel:
            df_items = get_items_por_venta(venta_sel)
            if not df_items.empty:
                df_items.columns = ["Producto","Cantidad","Precio u.","Subtotal"]
                df_items["Precio u."] = df_items["Precio u."].apply(lambda x: f"${x:,.2f}")
                df_items["Subtotal"]  = df_items["Subtotal"].apply(lambda x: f"${x:,.2f}")
                st.dataframe(df_items, use_container_width=True, hide_index=True)

        st.subheader("Resumen por producto")
        with get_connection() as conn:
            df_res = pd.read_sql_query("""
                SELECT p.nombre AS Producto,
                       SUM(vi.cantidad) AS "Unidades vendidas",
                       SUM(vi.subtotal) AS "Total recaudado"
                FROM venta_items vi JOIN productos p ON vi.producto_id = p.id
                GROUP BY p.nombre ORDER BY 3 DESC
            """, conn)
        if not df_res.empty:
            df_res["Total recaudado"] = df_res["Total recaudado"].apply(lambda x: f"${x:,.2f}")
            st.dataframe(df_res, use_container_width=True, hide_index=True)
