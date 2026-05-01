creame esta app utilizando dash

# Sistema de Facturación - Especificación de Migración

## 1. Propósito de la aplicación

Aplicación web de facturación y gestión comercial para comercios pequeños. Permite:
- Administrar productos e inventario.
- Registrar clientes.
- Generar ventas con carrito y descuentos.
- Registrar ventas a contado o a cuenta corriente.
- Controlar cuentas corrientes y pagos.
- Ver historial de ventas y métricas operativas.

La aplicación actual está construida en Streamlit y usa PostgreSQL para persistencia.

---

## 2. Requisitos generales

- Autenticación básica mediante credenciales en variables de entorno.
- Navegación de una sola página con panel lateral y secciones.
- Persistencia en base de datos relacional.
- Estado de carrito en sesión de usuario.
- Consultas repetidas cacheadas para mejorar experiencia.
- Interfaz responsiva tipo dashboard con métricas y tablas.
- Sin usuarios registrados dinámicamente: solo login con credenciales fijas.

---

## 3. Configuración y entorno

Variables necesarias en `.env` o configuración del servidor:
- `DATABASE_URL`: conexión PostgreSQL completa.
- `APP_USER`: nombre de usuario de acceso.
- `APP_PASSWORD`: contraseña de acceso.
- `SECRET_KEY`: clave usada para token de "recordar sesión".

---

## 4. Modelo de datos

### `productos`
- `id` (SERIAL PK)
- `nombre` (TEXT, obligatorio)
- `precio` (NUMERIC)
- `stock` (INTEGER)
- `stock_minimo` (INTEGER)
- `creado_en` (TIMESTAMP)

### `clientes`
- `id` (SERIAL PK)
- `nombre` (TEXT, obligatorio)
- `telefono` (TEXT)
- `email` (TEXT)
- `creado_en` (TIMESTAMP)

### `ventas`
- `id` (SERIAL PK)
- `cliente_id` (FK clientes)
- `fecha` (TIMESTAMP)
- `subtotal` (NUMERIC)
- `descuento` (NUMERIC)
- `total` (NUMERIC)
- `metodo_pago` (TEXT)
- `es_cuenta_corriente` (BOOLEAN)
- `estado` (TEXT, "pagada" o "pendiente")
- `notas` (TEXT)

### `venta_items`
- `id` (SERIAL PK)
- `venta_id` (FK ventas)
- `producto_id` (FK productos)
- `nombre_producto` (TEXT)
- `cantidad` (INTEGER)
- `precio_unitario` (NUMERIC)
- `subtotal` (NUMERIC)

### `pagos_cuenta`
- `id` (SERIAL PK)
- `venta_id` (FK ventas)
- `monto` (NUMERIC)
- `fecha` (TIMESTAMP)
- `metodo_pago` (TEXT)
- `notas` (TEXT)

---

## 5. Flujo de autenticación

### Login
- Página inicial si no hay sesión valida.
- Formulario con campos:
  - Usuario
  - Contraseña
  - Recordar sesión
- Verifica `APP_USER` y `APP_PASSWORD`.
- Si es correcto, guarda `logged_in` en sesión.
- Si selecciona "Recordar sesión", genera token y lo guarda en query params.

### Logout
- Botón disponible en la barra lateral.
- Limpia sesión y query params.
- Redirige al login.

### Auto-login
- Busca token y usuario en `query_params`.
- Valida token con `SECRET_KEY`, aceptando token de la hora actual o previa.

---

## 6. Navegación principal

Barra lateral con secciones:
- Dashboard
- Inventario
- Clientes
- Nueva Venta
- Cuenta Corriente
- Historial de Ventas

Además muestra en el sidebar un resumen de:
- Total de productos
- Total de clientes
- Deuda total en cuentas corrientes

---

## 7. Funcionalidad por módulo

### 7.1 Dashboard

Muestra indicadores clave:
- Ventas de hoy
- Ventas del mes
- Número de ventas hoy
- Deuda pendiente de cuenta corriente
- Productos sin stock
- Productos bajo stock mínimo

Mostrar:
- Tabla de últimas 8 ventas.
- Alertas de inventario para productos con stock <= stock_minimo.

### 7.2 Inventario

Listado de productos:
- Buscar por nombre.
- Ver precio, stock, stock mínimo y estado.
- Estado calculado:
  - Sin stock
  - Crítico
  - Bajo
  - Normal

Editar producto:
- Cambiar nombre, precio, stock, stock mínimo.
- Guardar cambios.
- Eliminar producto.

Nuevo producto:
- Nombre obligatorio.
- Precio obligatorio > 0.
- Stock inicial.
- Stock mínimo.

### 7.3 Clientes

Listado de clientes:
- Buscar por nombre.
- Mostrar teléfono, email, deuda.
- Deuda calculada a partir de ventas pendientes y pagos registrados.

Editar cliente:
- Nombre
- Teléfono
- Email
- Guardar o eliminar.

Nuevo cliente:
- Nombre obligatorio.
- Teléfono y email opcionales.

### 7.4 Nueva Venta

Flujo principal de venta:
- Lista de productos con stock > 0.
- Seleccionar producto y cantidad.
- Agregar al carrito.
- Carrito en sesión del usuario.
- Eliminar ítem o limpiar carrito.

Opciones de pago:
- Efectivo
- Tarjeta
- Transferencia
- Cuenta Corriente

Descuentos:
- Sin descuento
- Porcentaje (%)
- Monto fijo ($)

Resumen y confirmación:
- Subtotal
- Descuento
- Total final
- Notas opcionales
- Validación: si es cuenta corriente requiere cliente registrado.

Registro de venta:
- Inserta en tabla `ventas`.
- Inserta en `venta_items` por producto.
- Actualiza stock de productos.
- Si pago contado, estado = `pagada`.
- Si cuenta corriente, estado = `pendiente`.

### 7.5 Cuenta Corriente

Sección de gestión de créditos y pagos.

Mostrar:
- Facturas vencidas (más de 30 días) con alerta roja.
- Listado de facturas pendientes.
- Resumen por cliente con facturas, total, pagado y saldo.

Registrar pago:
- Seleccionar factura pendiente.
- Ingresar monto, método y notas.
- Registrar pago en `pagos_cuenta`.
- Si se paga en su totalidad, marcar `ventas.estado = 'pagada'`.

Historial de pagos:
- Mostrar últimos 50 pagos registrados.
- Ver cliente, factura, monto, método, fecha y notas.

### 7.6 Historial de Ventas

Filtros:
- Fecha desde / hasta.
- Cliente (Todos o cliente específico).

Resultados:
- Lista de ventas con subtotal, descuento, total, método y estado.
- Métricas de período:
  - Total vendido
  - Cantidad de ventas
  - Ticket promedio

Detalle de venta:
- Seleccionar venta y ver sus productos.
- Mostrar nombre producto, cantidad, precio unitario y subtotal.

Producto agregado:
- Resumen agregado por producto en el período filtrado.

---

## 8. Comportamiento técnico importante

### Sesión y estado
- El carrito se mantiene en `session_state`.
- La navegación del app se basa en rerenders de Streamlit.
- No hay APIs REST; todo es renderizado en el servidor.

### Cache y rendimiento
- Se usan decoradores de cache (`st.cache_data`) para reducir consultas repetidas en:
  - Dashboard
  - Inventario
  - Clientes
  - Cuenta corriente
  - Historial
  - Nueva venta (productos y clientes)
- Debe invalidarse cache al modificar datos.

### Errores y validaciones
- Venta a cuenta corriente sin cliente: alerta.
- Descuento mayor al subtotal: se corrige al máximo permitido.
- Login incorrecto: muestra error.
- Si no hay datos, se muestran mensajes de información en cada pantalla.

### Estilo y UI
- Custom CSS aplica estilo a sidebar, métricas, botones y alertas.
- El dashboard usa métricas y tarjetas.
- Tablas y forms se usan para CRUD.

---

## 9. Sugerencias para migración

### 9.1 Arquitectura recomendada
- Frontend: UI con rutas/páginas claras.
- Backend: API REST para CRUD y queries.
- Estado: sesión del usuario + caché en frontend si hay múltiples filtros.
- Autenticación: simple token en sesión o JWT equivalente.

### 9.2 Priorizar
1. Data model y transacciones de venta.
2. Login + sesión.
3. Carrito y checkout.
4. Inventario y clientes CRUD.
5. Cuenta corriente y pagos.
6. Dashboard e historial.

### 9.3 Consideraciones
- Mantén el `carrito` como estado efímero en sesión, no en la base de datos.
- No se requiere registro de usuarios dinámico.
- La app actual depende solo de la base de datos para persistencia, no de servicios externos.

---

## 10. Lista de funcionalidades mínimas para reconstruir

- [ ] Login con credenciales fijas.
- [ ] Navegación por secciones.
- [ ] Panel lateral con resumen rápido.
- [ ] CRUD de productos.
- [ ] CRUD de clientes.
- [ ] Registro de ventas con carrito.
- [ ] Descuento por porcentaje o monto fijo.
- [ ] Pago contado y cuenta corriente.
- [ ] Actualización de stock.
- [ ] Registro de pagos a cuenta corriente.
- [ ] Historial de ventas y detalle de factura.
- [ ] Dashboard con métricas clave.
- [ ] Alertas de inventario y vencimiento.
- [ ] Cache de consultas repetidas y limpieza al actualizar.
