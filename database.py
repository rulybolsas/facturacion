import os
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
import streamlit as st

# Cargar variables de entorno desde .env
load_dotenv(override=True)

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