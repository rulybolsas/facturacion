import streamlit as st
import hashlib
import time
import os
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv(override=True)

def generate_token(user, password):
    """Genera un token simple basado en usuario, contraseña y timestamp."""
    timestamp = str(int(time.time() // 3600))  # Token válido por 1 hora
    data = f"{user}:{password}:{timestamp}:{os.getenv('SECRET_KEY', '123456')}"
    token = hashlib.sha256(data.encode()).hexdigest()
    return token

def verify_token(token, user, password):
    """Verifica si un token es válido."""
    current_hour = str(int(time.time() // 3600))
    prev_hour = str(int(time.time() // 3600) - 1)

    for hour in [current_hour, prev_hour]:
        data = f"{user}:{password}:{hour}:{os.getenv('SECRET_KEY', '123456')}"
        expected_token = hashlib.sha256(data.encode()).hexdigest()
        if token == expected_token:
            return True
    return False

def login():
    """Muestra el formulario de login y verifica credenciales."""
    st.title("🔐 Inicio de Sesión")

    # Centrar el formulario
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        st.markdown("""
        <div style="text-align: center; padding: 20px;">
            <h2>Sistema de Facturación</h2>
            <p>Ingresa tus credenciales para continuar</p>
        </div>
        """, unsafe_allow_html=True)

        with st.form("login_form"):
            username = st.text_input("Usuario", placeholder="Ingresa tu usuario")
            password = st.text_input("Contraseña", type="password", placeholder="Ingresa tu contraseña")
            remember = st.checkbox("Recordar sesión", value=True)

            submitted = st.form_submit_button("🚀 Iniciar Sesión", type="primary")

            if submitted:
                # Verificar credenciales
                correct_user = os.getenv("APP_USER", "admin")
                correct_pass = os.getenv("APP_PASSWORD", "123456")

                if username == correct_user and password == correct_pass:
                    st.session_state.logged_in = True
                    st.session_state.username = username

                    if remember:
                        token = generate_token(username, password)
                        st.session_state.auth_token = token
                        if hasattr(st, "experimental_set_query_params"):
                            st.experimental_set_query_params(token=token, user=username)
                        else:
                            st.query_params = {"token": [token], "user": [username]}
                    else:
                        if hasattr(st, "experimental_set_query_params"):
                            st.experimental_set_query_params()
                        else:
                            st.query_params = {}

                    st.experimental_rerun()
                else:
                    st.error("❌ Usuario o contraseña incorrectos")

def logout():
    """Cierra la sesión y limpia datos."""
    if st.sidebar.button("🚪 Cerrar Sesión"):
        st.session_state.logged_in = False
        if 'auth_token' in st.session_state:
            del st.session_state.auth_token
        if 'username' in st.session_state:
            del st.session_state.username

        # Limpiar query params
        st.query_params.clear()

        st.rerun()

def check_login():
    """Verifica si el usuario está logueado, incluyendo auto-login desde query params."""
    if "logged_in" not in st.session_state:
        st.session_state.logged_in = False

    # Si ya está logueado en session_state, continuar
    if st.session_state.logged_in:
        return True

    # Verificar auto-login desde query params
    query_params = st.query_params
    if "token" in query_params and "user" in query_params:
        token = query_params["token"]
        user = query_params["user"]
        password = os.getenv("APP_PASSWORD")

        if verify_token(token, user, password):
            st.session_state.logged_in = True
            st.session_state.username = user
            st.session_state.auth_token = token
            return True
        else:
            # Token inválido, limpiar
            st.query_params.clear()

    return False