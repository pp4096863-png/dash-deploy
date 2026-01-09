import sys
sys.path.insert(0, 'c:\\Users\\lenovo\\Desktop\\sales dash')

from dashboard import transform_data, load_data

print("Testing transform_data()...")
result = transform_data()
print(f"\n✅ Transform completed: {result}")

print("\n\nLoading data...")
orders, revenues, cash, merged, measure_cols = load_data()

print(f"✅ Orders shape: {orders.shape if hasattr(orders, 'shape') else 'empty'}")
print(f"✅ Revenues shape: {revenues.shape if hasattr(revenues, 'shape') else 'empty'}")
print(f"✅ Cash shape: {cash.shape if hasattr(cash, 'shape') else 'empty'}")
print(f"✅ Merged shape: {merged.shape if hasattr(merged, 'shape') else 'empty'}")

if hasattr(merged, 'shape') and merged.shape[0] > 0:
    print(f"\n📊 Sample merged data:")
    print(merged.head(2))
