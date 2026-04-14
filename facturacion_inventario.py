import streamlit as st
import pandas as pd
from datetime import datetime, date, timedelta
import os
import psycopg2
from psycopg2.extras import RealDictCursor

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

# ── Conexión a PostgreSQL (Supabase) ─────────────────────────────────────────
def get_conn():
    """
    Lee DATABASE_URL desde variable de entorno (configurada en Render).
    Formato esperado:
      postgresql://usuario:password@host:puerto/base_de_datos
    """
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        st.error(
            "⚠️ No se encontró la variable de entorno **DATABASE_URL**.\n\n"
            "Configurala en Render → Environment → Add Environment Variable."
        )
        st.stop()
    conn = psycopg2.connect(db_url, cursor_factory=RealDictCursor)
    conn.autocommit = False
    return conn

def query(sql, params=None, fetch="all"):
    """Ejecuta una consulta y retorna resultados como lista de dicts."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params or ())
            if fetch == "one":
                row = cur.fetchone()
                conn.commit()
                return dict(row) if row else None
            elif fetch == "all":
                rows = cur.fetchall()
                conn.commit()
                return [dict(r) for r in rows]
            else:
                conn.commit()
                return None
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()

def execute(sql, params=None):
    """Ejecuta INSERT/UPDATE/DELETE y retorna el id generado si aplica."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params or ())
            # Si el SQL termina con RETURNING id, obtenemos el id
            try:
                row = cur.fetchone()
                last_id = row["id"] if row else None
            except Exception:
                last_id = None
            conn.commit()
            return last_id
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()

def execute_many(statements):
    """Ejecuta múltiples sentencias en una sola transacción."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            results = []
            for sql, params in statements:
                cur.execute(sql, params or ())
                try:
                    row = cur.fetchone()
                    results.append(row["id"] if row else None)
                except Exception:
                    results.append(None)
        conn.commit()
        return results
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()

# ── Inicializar tablas ────────────────────────────────────────────────────────
def init_db():
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
            CREATE TABLE IF NOT EXISTS productos (
                id SERIAL PRIMARY KEY,
                nombre TEXT NOT NULL,
                precio NUMERIC(12,2) NOT NULL,
                stock INTEGER NOT NULL DEFAULT 0,
                stock_minimo INTEGER NOT NULL DEFAULT 5,
                creado_en TIMESTAMP DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS clientes (
                id SERIAL PRIMARY KEY,
                nombre TEXT NOT NULL,
                telefono TEXT,
                email TEXT,
                creado_en TIMESTAMP DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS ventas (
                id SERIAL PRIMARY KEY,
                cliente_id INTEGER REFERENCES clientes(id),
                fecha TIMESTAMP DEFAULT NOW(),
                subtotal NUMERIC(12,2) NOT NULL,
                descuento NUMERIC(12,2) DEFAULT 0,
                total NUMERIC(12,2) NOT NULL,
                metodo_pago TEXT NOT NULL,
                es_cuenta_corriente BOOLEAN DEFAULT FALSE,
                estado TEXT DEFAULT 'pagada',
                notas TEXT
            );

            CREATE TABLE IF NOT EXISTS venta_items (
                id SERIAL PRIMARY KEY,
                venta_id INTEGER NOT NULL REFERENCES ventas(id),
                producto_id INTEGER NOT NULL REFERENCES productos(id),
                nombre_producto TEXT NOT NULL,
                cantidad INTEGER NOT NULL,
                precio_unitario NUMERIC(12,2) NOT NULL,
                subtotal NUMERIC(12,2) NOT NULL
            );

            CREATE TABLE IF NOT EXISTS pagos_cuenta (
                id SERIAL PRIMARY KEY,
                venta_id INTEGER NOT NULL REFERENCES ventas(id),
                monto NUMERIC(12,2) NOT NULL,
                fecha TIMESTAMP DEFAULT NOW(),
                metodo_pago TEXT NOT NULL,
                notas TEXT
            );
            """)
        conn.commit()
    except Exception as e:
        conn.rollback()
        st.error(f"Error al inicializar la base de datos: {e}")
    finally:
        conn.close()

# ── Helpers ───────────────────────────────────────────────────────────────────
def fmt_money(v):
    return f"$ {float(v):,.2f}"

def estado_stock(stock, minimo):
    if stock == 0:
        return "Sin stock", "badge-sinstock"
    elif stock <= minimo // 2:
        return "Crítico", "badge-critico"
    elif stock <= minimo:
        return "Bajo", "badge-bajo"
    return "Normal", "badge-normal"

def semaforo_deuda(dias):
    if dias <= 7:
        return "🟢", "alerta-verde"
    elif dias <= 30:
        return "🟡", "alerta-amarilla"
    return "🔴", "alerta-roja"

# ══════════════════════════════════════════════════════════════════════════════
# MÓDULOS
# ══════════════════════════════════════════════════════════════════════════════

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

    hoy = date.today().isoformat()
    mes_inicio = date.today().replace(day=1).isoformat()

    ventas_hoy = query(
        "SELECT COALESCE(SUM(total),0) v FROM ventas WHERE DATE(fecha)=%s AND estado != 'anulada'",
        (hoy,), fetch="one"
    )["v"]
    ventas_mes = query(
        "SELECT COALESCE(SUM(total),0) v FROM ventas WHERE DATE(fecha)>=%s AND estado != 'anulada'",
        (mes_inicio,), fetch="one"
    )["v"]
    num_ventas_hoy = query(
        "SELECT COUNT(*) v FROM ventas WHERE DATE(fecha)=%s AND estado != 'anulada'",
        (hoy,), fetch="one"
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
        ultimas = query("""
            SELECT v.id, COALESCE(c.nombre,'Consumidor Final') cliente,
                   v.fecha, v.total, v.metodo_pago, v.estado
            FROM ventas v LEFT JOIN clientes c ON v.cliente_id=c.id
            WHERE v.estado != 'anulada'
            ORDER BY v.fecha DESC LIMIT 8
        """)
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
        alertas = query("""
            SELECT nombre, stock, stock_minimo FROM productos
            WHERE stock <= stock_minimo ORDER BY stock ASC LIMIT 10
        """)
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


# ── INVENTARIO ────────────────────────────────────────────────────────────────
def modulo_inventario():
    st.title("📦 Gestión de Inventario")
    tab1, tab2 = st.tabs(["📋 Listado", "➕ Agregar / Editar"])

    with tab1:
        productos = query("SELECT id, nombre, precio, stock, stock_minimo FROM productos ORDER BY nombre")
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


# ── CLIENTES ──────────────────────────────────────────────────────────────────
def modulo_clientes():
    st.title("👥 Gestión de Clientes")
    tab1, tab2 = st.tabs(["📋 Listado", "➕ Nuevo Cliente"])

    with tab1:
        clientes = query("SELECT id, nombre, telefono, email FROM clientes ORDER BY nombre")
        if clientes:
            buscar = st.text_input("🔍 Buscar cliente")
            filas = []
            for c in clientes:
                if buscar and buscar.lower() not in c["nombre"].lower():
                    continue
                deuda = query("""
                    SELECT COALESCE(SUM(v.total - COALESCE(p.pagado,0)),0) v
                    FROM ventas v
                    LEFT JOIN (SELECT venta_id, SUM(monto) pagado FROM pagos_cuenta GROUP BY venta_id) p
                      ON p.venta_id=v.id
                    WHERE v.cliente_id=%s AND v.es_cuenta_corriente=TRUE AND v.estado='pendiente'
                """, (c["id"],), fetch="one")["v"]
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
                        st.success("Cliente actualizado.")
                        st.rerun()
                    if b2.form_submit_button("🗑️ Eliminar"):
                        execute("DELETE FROM clientes WHERE id=%s", (cli["id"],))
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
                    st.success(f"✅ Cliente '{nombre}' registrado.")
                    st.rerun()


# ── NUEVA VENTA ───────────────────────────────────────────────────────────────
def modulo_nueva_venta():
    st.title("🛒 Nueva Venta")

    if "carrito" not in st.session_state:
        st.session_state.carrito = []

    productos = query("SELECT id, nombre, precio, stock FROM productos WHERE stock > 0 ORDER BY nombre")
    clientes  = query("SELECT id, nombre FROM clientes ORDER BY nombre")

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

        desc_tipo = st.radio("Descuento", ["Sin descuento", "Porcentaje %", "Monto fijo $"], horizontal=True)
        descuento = 0.0
        if desc_tipo == "Porcentaje %":
            pct = st.number_input("Porcentaje (%)", min_value=0.0, max_value=100.0, step=0.5, value=0.0)
            descuento = subtotal * pct / 100
        elif desc_tipo == "Monto fijo $":
            descuento = st.number_input("Monto descuento ($)", min_value=0.0, step=0.01, value=0.0)
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


# ── CUENTA CORRIENTE ──────────────────────────────────────────────────────────
def modulo_cuenta_corriente():
    st.title("📋 Cuenta Corriente")

    # Alertas de vencimiento
    vencidas = query("""
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
        pendientes = query("""
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
            resumen = query("""
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
        pendientes2 = query("""
            SELECT v.id, COALESCE(c.nombre,'—') cliente, v.total,
                   COALESCE(p.pagado,0) pagado
            FROM ventas v
            LEFT JOIN clientes c ON v.cliente_id=c.id
            LEFT JOIN (SELECT venta_id, SUM(monto) pagado FROM pagos_cuenta GROUP BY venta_id) p
              ON p.venta_id=v.id
            WHERE v.es_cuenta_corriente=TRUE AND v.estado='pendiente'
            ORDER BY v.fecha ASC
        """)
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
                    st.success(f"✅ Pago de {fmt_money(monto)} registrado.")
                    st.rerun()

        st.markdown("---")
        st.subheader("📜 Historial de Pagos")
        historial = query("""
            SELECT pc.id, COALESCE(c.nombre,'—') cliente,
                   pc.venta_id, pc.monto, pc.metodo_pago, pc.fecha, pc.notas
            FROM pagos_cuenta pc
            JOIN ventas v ON pc.venta_id=v.id
            LEFT JOIN clientes c ON v.cliente_id=c.id
            ORDER BY pc.fecha DESC LIMIT 50
        """)
        if historial:
            df = pd.DataFrame(historial)
            df["monto"] = df["monto"].apply(fmt_money)
            df["fecha"] = pd.to_datetime(df["fecha"]).dt.strftime("%d/%m/%Y %H:%M")
            df.columns = ["ID","Cliente","Factura #","Monto","Método","Fecha","Notas"]
            st.dataframe(df, use_container_width=True, hide_index=True)
        else:
            st.info("No hay pagos registrados.")


# ── HISTORIAL DE VENTAS ───────────────────────────────────────────────────────
def modulo_historial():
    st.title("📜 Historial de Ventas")

    col1, col2, col3 = st.columns(3)
    with col1:
        fecha_desde = st.date_input("Desde", value=date.today() - timedelta(days=30))
    with col2:
        fecha_hasta = st.date_input("Hasta", value=date.today())
    with col3:
        clientes_lista = query("SELECT id, nombre FROM clientes ORDER BY nombre")
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


# ══════════════════════════════════════════════════════════════════════════════
# NAVEGACIÓN PRINCIPAL
# ══════════════════════════════════════════════════════════════════════════════
def main():
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
            total_prods   = query("SELECT COUNT(*) v FROM productos", fetch="one")["v"]
            total_clientes = query("SELECT COUNT(*) v FROM clientes", fetch="one")["v"]
            deuda = query("""
                SELECT COALESCE(SUM(v.total - COALESCE(p.pagado,0)),0) v
                FROM ventas v
                LEFT JOIN (SELECT venta_id, SUM(monto) pagado FROM pagos_cuenta GROUP BY venta_id) p
                  ON p.venta_id=v.id
                WHERE v.es_cuenta_corriente=TRUE AND v.estado='pendiente'
            """, fetch="one")["v"]
            st.markdown(f"""
            <div style="color:#94a3b8;font-size:12px;padding:8px">
                📦 {total_prods} productos<br>
                👥 {total_clientes} clientes<br>
                💳 Deuda: {fmt_money(deuda)}
            </div>
            """, unsafe_allow_html=True)
        except Exception:
            pass

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
