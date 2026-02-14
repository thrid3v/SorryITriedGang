# Complex RAG Queries - Test Reference

## Quick Start

### Option 1: Quick Test (10 Most Complex Queries)
```bash
python quick_rag_test.py
```

### Option 2: Full Test Suite (50+ Queries)
```bash
# Test all categories
python test_rag_queries.py

# Test specific category
python test_rag_queries.py --category "Temporal Queries"

# Interactive mode
python test_rag_queries.py --interactive

# Test single query
python test_rag_queries.py --query "What was my revenue last month?"

# Show SQL in output
python test_rag_queries.py --show-sql
```

---

## 50+ Complex Queries by Category

### 1. Temporal Queries (7 queries)
- What was my total revenue last month?
- Show me revenue comparison between this month and last month
- What is my year-to-date revenue?
- How much did I earn in Q1 2026?
- What was revenue yesterday compared to the day before?
- Show me weekly revenue trend for the past 4 weeks
- What is my revenue growth rate month over month?

### 2. Customer Analytics (7 queries)
- Who are my top 10 customers by total spending?
- Show me customer lifetime value for all customers
- How many new customers did I acquire last month?
- What is the average order value per customer?
- Which customers haven't purchased in the last 30 days?
- Show me customer retention rate
- What cities generate the most revenue?

### 3. Product Analytics (7 queries)
- What are my best selling products this month?
- Which products have the highest profit margins?
- Show me products that are frequently bought together
- What is the revenue contribution of each product category?
- Which products are underperforming?
- Show me product sales by category and month
- What products should I promote based on sales trends?

### 4. Inventory Management (7 queries)
- Which products are running low on stock?
- Show me inventory turnover ratio by category
- What products need immediate reordering?
- What is my average inventory level by product?
- Which products have the slowest turnover?
- Show me stock levels for Electronics category
- What is my total inventory value?

### 5. Advanced Analytics (7 queries)
- Show me revenue by day of week to identify peak sales days
- What is the correlation between product price and sales volume?
- Calculate customer cohort analysis by signup month
- Show me the Pareto analysis - which products generate 80% of revenue?
- What is the average time between customer purchases?
- Show me seasonal demand patterns by category
- What is my customer acquisition cost trend?

### 6. Multi-Dimensional Queries (6 queries)
- Show me revenue by category, city, and month
- What are the top products in each city?
- Compare customer spending patterns between different cities
- Show me revenue breakdown by payment method and category
- What is the average order value by customer segment and product category?
- Show me inventory levels and sales velocity by product

### 7. Business Intelligence (7 queries)
- What percentage of revenue comes from repeat customers?
- Show me the distribution of order values (small, medium, large)
- What is my average customer acquisition rate per month?
- Which product categories have the highest return on investment?
- Show me revenue per customer by city
- What is the average basket size?

---

## Top 10 Most Complex Queries

These are the most challenging queries that test RAG's capabilities:

1. **Year-over-Year Comparison**
   ```
   What was my revenue last month compared to the same month last year?
   ```

2. **Multi-Dimensional Ranking**
   ```
   Show me top 5 products by revenue in each city
   ```

3. **Pareto Analysis**
   ```
   Which customers are in the top 20% by spending (Pareto analysis)?
   ```

4. **Temporal Filtering**
   ```
   What products sold more than 10 units in the last 7 days?
   ```

5. **Customer Segmentation**
   ```
   Show me new vs returning customer revenue breakdown for this month
   ```

6. **Market Basket Analysis**
   ```
   What products are frequently purchased together with Electronics?
   ```

7. **Inventory-Sales Correlation**
   ```
   Which products have high inventory but low sales in the last month?
   ```

8. **Cohort Analysis**
   ```
   Show me customer retention rate by signup month
   ```

9. **Revenue Attribution**
   ```
   What percentage of total revenue comes from the top 10 products?
   ```

10. **Seasonal Comparison**
    ```
    Compare Q1 and Q2 revenue by product category
    ```

---

## Manual Testing via CLI

Test individual queries directly:

```bash
# Basic query
python src/analytics/nl_query.py "What was my revenue last month?"

# Complex temporal query
python src/analytics/nl_query.py "Show me revenue comparison between this month and last month"

# Customer analytics
python src/analytics/nl_query.py "Who are my top 10 customers by total spending?"

# Product analysis
python src/analytics/nl_query.py "What products are frequently bought together?"

# Inventory management
python src/analytics/nl_query.py "Which products are running low on stock?"
```

---

## Testing via API

```bash
# Test via REST API
curl -X POST http://localhost:8000/api/ask \
  -H "Content-Type: application/json" \
  -d '{"question": "What was my total revenue last month?"}'

# Pretty print JSON response
curl -X POST http://localhost:8000/api/ask \
  -H "Content-Type: application/json" \
  -d '{"question": "Show me top 10 customers by spending"}' | python -m json.tool
```

---

## Interactive Testing

For exploratory testing, use interactive mode:

```bash
python test_rag_queries.py --interactive
```

Commands in interactive mode:
- Type any query to test it
- `list` - Show all available test queries
- `category Temporal` - Show queries for a specific category
- `quit` - Exit

---

## What to Look For

When testing RAG queries, check:

1. **RAG Match Quality**
   - Distance score < 1.0 = good match
   - Distance score < 0.5 = excellent match
   - Correct category matched

2. **SQL Generation**
   - Valid SQL syntax
   - Correct table joins
   - Appropriate filters and aggregations
   - Proper date handling

3. **Results**
   - Non-empty results (when data exists)
   - Correct data types
   - Reasonable values

4. **Summary**
   - Accurate natural language summary
   - Highlights key insights

---

## Expected Challenges

Some queries may fail or produce suboptimal results:

❌ **Likely to struggle:**
- Year-over-year comparisons (requires complex date logic)
- Cohort analysis (requires window functions)
- Pareto analysis (requires percentile calculations)
- Multi-level grouping (category → city → month)

✅ **Should work well:**
- Simple temporal queries (last month, this year)
- Top N queries (top customers, top products)
- Basic aggregations (SUM, COUNT, AVG)
- Single-dimension filtering

---

## Tips for Best Results

1. **Be specific with time periods**
   - ✅ "last month" 
   - ✅ "this year"
   - ❌ "recently" (ambiguous)

2. **Use clear metric names**
   - ✅ "revenue", "sales", "spending"
   - ❌ "money", "earnings" (less common)

3. **Specify dimensions clearly**
   - ✅ "by category", "by city", "by customer"
   - ✅ "top 10", "bottom 5"

4. **Combine related concepts**
   - ✅ "revenue by category last month"
   - ✅ "top customers by spending this year"
