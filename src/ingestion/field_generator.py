"""
Schema-Driven Field Generator
==============================
Generates fake data fields based on schema configuration.
Maps field types to Faker generators for dynamic data generation.
"""
import random
from typing import Any, Dict, List
from datetime import datetime, timedelta
from faker import Faker


class FieldGenerator:
    """Generates field values based on schema configuration."""
    
    def __init__(self, faker_instance: Faker = None):
        """
        Initialize field generator.
        
        Args:
            faker_instance: Faker instance to use (creates new one if None)
        """
        self.fake = faker_instance or Faker()
        self._id_counters = {}  # Track ID counters per prefix
        self._fk_pools = {}  # Store foreign key pools
    
    def generate(self, field_name: str, field_config: Dict[str, Any], row_index: int = 0) -> Any:
        """
        Generate a field value based on configuration.
        
        Args:
            field_name: Name of the field
            field_config: Field configuration dict
            row_index: Current row index (for ID generation)
        
        Returns:
            Generated field value
        """
        field_type = field_config.get("type", "string")
        
        # Handle null injection
        null_rate = field_config.get("null_rate", 0.0)
        if random.random() < null_rate:
            return None
        
        # Generate based on type
        if field_type == "id":
            return self._generate_id(field_config, row_index)
        elif field_type == "choice":
            return random.choice(field_config["options"])
        elif field_type == "float":
            return round(random.uniform(field_config["min"], field_config["max"]), 2)
        elif field_type == "int":
            return random.randint(field_config["min"], field_config["max"])
        elif field_type == "datetime":
            return self._generate_datetime(field_config)
        elif field_type == "date":
            return self._generate_date(field_config)
        elif field_type == "string":
            return self._generate_string(field_config)
        elif field_type == "email":
            return self.fake.email()
        elif field_type == "name":
            return self.fake.name()
        elif field_type == "fk":
            # Foreign keys handled separately
            return None
        else:
            # Default to string
            return str(self.fake.word())
    
    def _generate_id(self, config: Dict[str, Any], row_index: int) -> str:
        """Generate ID with prefix and counter."""
        prefix = config.get("prefix", "ID")
        width = config.get("width", 4)
        
        # Use row_index as counter
        return f"{prefix}_{row_index:0{width}d}"
    
    def _generate_datetime(self, config: Dict[str, Any]) -> str:
        """Generate datetime string."""
        start = config.get("start", "-30d")
        end = config.get("end", "today")
        
        # Parse relative dates
        start_date = self._parse_relative_date(start)
        end_date = self._parse_relative_date(end)
        
        dt = self.fake.date_time_between(start_date=start_date, end_date=end_date)
        return dt.isoformat()
    
    def _generate_date(self, config: Dict[str, Any]) -> str:
        """Generate date string."""
        start = config.get("start", "-30d")
        end = config.get("end", "today")
        
        start_date = self._parse_relative_date(start)
        end_date = self._parse_relative_date(end)
        
        date = self.fake.date_between(start_date=start_date, end_date=end_date)
        return date.isoformat()
    
    def _generate_string(self, config: Dict[str, Any]) -> str:
        """Generate string using Faker method."""
        faker_method = config.get("faker_method", "word")
        
        if hasattr(self.fake, faker_method):
            method = getattr(self.fake, faker_method)
            return str(method())
        else:
            return str(self.fake.word())
    
    def _parse_relative_date(self, date_str: str):
        """Parse relative date strings like '-30d', 'today'."""
        if date_str == "today":
            return datetime.now()
        elif date_str == "now":
            return datetime.now()
        elif date_str.endswith("d"):
            # Relative days: "-30d"
            days = int(date_str[:-1])
            return datetime.now() + timedelta(days=days)
        elif date_str.endswith("y"):
            # Relative years: "-2y"
            years = int(date_str[:-1])
            return datetime.now() + timedelta(days=years * 365)
        else:
            # Try parsing as ISO date
            try:
                return datetime.fromisoformat(date_str)
            except:
                return datetime.now()
    
    def register_fk_pool(self, table_name: str, field_name: str, values: List[Any]):
        """
        Register a pool of foreign key values for reference.
        
        Args:
            table_name: Name of the referenced table
            field_name: Name of the referenced field
            values: List of valid foreign key values
        """
        key = f"{table_name}.{field_name}"
        self._fk_pools[key] = values
    
    def generate_fk(self, fk_config: Dict[str, Any]) -> Any:
        """
        Generate a foreign key value from registered pool.
        
        Args:
            fk_config: FK configuration with 'references' field
        
        Returns:
            Random value from the FK pool
        """
        references = fk_config.get("references", "")
        
        if references in self._fk_pools:
            pool = self._fk_pools[references]
            if pool:
                return random.choice(pool)
        
        return None


# CLI test
if __name__ == "__main__":
    print("=== Field Generator Test ===\n")
    
    fake = Faker()
    Faker.seed(42)
    random.seed(42)
    
    gen = FieldGenerator(fake)
    
    # Test different field types
    test_configs = {
        "product_id": {"type": "id", "prefix": "PRD", "width": 4},
        "pastry_name": {"type": "choice", "options": ["Croissant", "Baguette", "Muffin"]},
        "price": {"type": "float", "min": 2.0, "max": 25.0},
        "freshness_hours": {"type": "int", "min": 2, "max": 48},
        "bake_date": {"type": "date", "start": "-7d", "end": "today"},
        "baker_name": {"type": "name"},
        "amount_nullable": {"type": "float", "min": 5.0, "max": 100.0, "null_rate": 0.3},
    }
    
    print("Generating 5 sample rows:\n")
    for i in range(1, 6):
        print(f"Row {i}:")
        for field_name, config in test_configs.items():
            value = gen.generate(field_name, config, row_index=i)
            print(f"  {field_name}: {value}")
        print()
