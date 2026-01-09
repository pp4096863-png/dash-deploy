import pandas as pd

# Test with actual format from Google Sheets
test_dates = ['1/22/2026', '2/22/2026', '3/22/2026', 'January/22', '1/2026']

def parse_flexible_date(val):
    try:
        if pd.isna(val):
            return pd.NaT
        
        val_str = str(val).strip()
        
        # Try different formats
        formats = [
            '%m/%d/%Y',      # 1/22/2026
            '%m/%d/%y',      # 1/22/26
            '%d/%m/%Y',      # 22/1/2026
            '%d/%m/%y',      # 22/1/26
            '%B/%y',         # January/22
            '%b/%y',         # Jan/22
        ]
        
        for fmt in formats:
            try:
                result = pd.to_datetime(val_str, format=fmt)
                print(f"  {val} -> {result} (format: {fmt})")
                return result
            except:
                continue
        
        # Last resort: let pandas try
        result = pd.to_datetime(val_str, errors='coerce')
        print(f"  {val} -> {result} (pandas default)")
        return result
    except Exception as e:
        print(f"  Error: {val} -> {e}")
        return pd.NaT

print("Testing flexible date parsing:")
for d in test_dates:
    result = parse_flexible_date(d)
    if pd.notna(result):
        print(f"    Year: {result.year}, Month: {result.month}")
