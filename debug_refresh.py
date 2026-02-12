"""Verify pipeline data grows across runs"""
from src.analytics.kpi_queries import compute_summary_kpis
kpis = compute_summary_kpis()
print(f"Revenue: ${kpis['total_revenue']:,.2f}")
print(f"Active Users: {kpis['active_users']}")
print(f"Total Orders: {kpis['total_orders']}")
