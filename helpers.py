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