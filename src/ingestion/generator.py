import pandas as pd
from faker import Faker
import random
from datetime import datetime
from pathlib import Path

fake = Faker()

def generate_transactions(num=100):
    """Generate dummy retail transactions with intentional data quality issues."""
    
    transactions = []
    for i in range(num):
        # Intentionally introduce NULLs (5% rate)
        user_id = fake.random_int(1000, 2000) if random.random() > 0.05 else None
        product_id = fake.random_int(100, 200) if random.random() > 0.05 else None
        amount = round(random.uniform(10.0, 500.0), 2) if random.random() > 0.05 else None
        quantity = random.randint(1, 10) if random.random() > 0.05 else None
        
        transaction = {
            'transaction_id': i + 1,
            'user_id': user_id,
            'product_id': product_id,
            'timestamp': fake.date_time_this_month(),
            'quantity': quantity,
            'amount': amount,
            'store_id': random.randint(1, 10)
        }
        transactions.append(transaction)
    
    df = pd.DataFrame(transactions)
    
    # Intentionally introduce duplicates (3% rate)
    num_duplicates = int(num * 0.03)
    if num_duplicates > 0:
        duplicate_indices = random.sample(range(len(df)), num_duplicates)
        duplicates = df.iloc[duplicate_indices].copy()
        df = pd.concat([df, duplicates], ignore_index=True)
    
    # Shuffle to mix duplicates
    df = df.sample(frac=1).reset_index(drop=True)
    
    return df

def save_transactions(df):
    """Save transactions to CSV with timestamp in filename."""
    output_dir = Path("data/raw")
    output_dir.mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = output_dir / f"transactions_{timestamp}.csv"
    
    df.to_csv(filename, index=False, encoding='utf-8')
    print(f"✅ Generated {len(df)} transactions → {filename}")
    print(f"   📊 Stats: {df.isnull().sum().sum()} NULLs, {df.duplicated().sum()} duplicates")

if __name__ == "__main__":
    print("🚀 Starting Data Generator...")
    transactions = generate_transactions(1000)
    save_transactions(transactions)
    print("✅ Generation complete!")
