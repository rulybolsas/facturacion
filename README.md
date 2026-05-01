# Sistema de Facturación

Aplicación web para gestión de facturación, inventario y clientes construida con Streamlit y PostgreSQL.

## 🔐 Inicio de Sesión

La aplicación requiere autenticación. Las credenciales se configuran en el archivo `.env`:

```env
APP_USER=admin
APP_PASSWORD=tu_password_segura
SECRET_KEY=tu_clave_secreta_unica
```

**Nota**: Cambia la contraseña y clave secreta por valores seguros antes de usar en producción.

### 🔄 Persistencia de Sesión

- **Recordar sesión**: Al marcar "Recordar sesión" en el login, se genera un token seguro que permite mantener la sesión activa incluso al recargar la página.
- **Cerrar sesión**: El botón "🚪 Cerrar Sesión" en la barra lateral permite cerrar la sesión manualmente.
- **Seguridad**: Los tokens tienen una validez de 1 hora y usan una clave secreta para encriptación.

## 🚀 Instalación y Configuración

### 1. Clonar o descargar el proyecto

### 2. Instalar dependencias

```bash
pip install -r requirements.txt
```

### 3. Configurar la base de datos

#### Opción A: PostgreSQL Local
1. Instala PostgreSQL en tu sistema (https://www.postgresql.org/download/)
2. Crea una base de datos llamada `facturacion_db`
3. Crea un usuario con permisos
4. Edita el archivo `.env` con tus credenciales:

```env
DATABASE_URL=postgresql://tu_usuario:tu_password@localhost:5432/facturacion_db
```

#### Opción B: Usar Supabase (para desarrollo rápido)
1. Crea una cuenta en [Supabase](https://supabase.com)
2. Crea un nuevo proyecto
3. Ve a Settings > Database y copia la connection string
4. Pégala en el archivo `.env`:

```env
DATABASE_URL=postgresql://postgres:[password]@db.[project-ref].supabase.co:5432/postgres
```

### 4. Ejecutar la aplicación

```bash
python -m streamlit run main.py
```

La aplicación estará disponible en: http://localhost:8501

## 📁 Estructura del Proyecto

```
facturacion/
├── main.py                 # Punto de entrada principal
├── config.py              # Configuración de Streamlit y CSS
├── database.py            # Funciones de conexión a BD
├── helpers.py             # Funciones auxiliares
├── modules/               # Módulos de la aplicación
│   ├── dashboard.py
│   ├── inventory.py
│   ├── clients.py
│   ├── new_sale.py
│   ├── current_account.py
│   └── history.py
├── .env                   # Variables de entorno (no subir a git)
├── requirements.txt       # Dependencias
└── render.yaml           # Configuración para despliegue en Render
```

## 🛠️ Tecnologías

- **Frontend**: Streamlit
- **Backend**: Python
- **Base de datos**: PostgreSQL
- **Despliegue**: Render

## 📋 Funcionalidades

- 📊 Dashboard con métricas en tiempo real
- 📦 Gestión de inventario
- 👥 Gestión de clientes
- 🛒 Registro de ventas
- 📋 Sistema de cuenta corriente
- 📜 Historial de ventas

## 🔧 Desarrollo

Para desarrollo local, asegúrate de tener PostgreSQL corriendo y configura la variable `DATABASE_URL` en el archivo `.env`.

## 🚀 Despliegue

El proyecto está configurado para desplegarse en Render. Las variables de entorno se configuran en el panel de Render.