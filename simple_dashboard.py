#!/usr/bin/env python3
import clickhouse_connect
import json
from flask import Flask, jsonify, render_template_string

app = Flask(__name__)

# Connect to ClickHouse
client = clickhouse_connect.get_client(
    host='localhost',
    port=8123,
    database='ecommerce'
)

@app.route('/')
def dashboard():
    return render_template_string('''
<!DOCTYPE html>
<html>
<head>
    <title>E-commerce Dashboard</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; }
        .metric { background: #f5f5f5; padding: 15px; margin: 10px; border-radius: 5px; }
        table { border-collapse: collapse; width: 100%; }
        th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
        th { background-color: #f2f2f2; }
    </style>
</head>
<body>
    <h1>E-commerce ClickHouse Dashboard</h1>
    
    <div class="metric">
        <h3>Quick Stats API Endpoints:</h3>
        <ul>
            <li><a href="/api/total-products">Total Products</a></li>
            <li><a href="/api/top-categories">Top Categories</a></li>
            <li><a href="/api/top-brands">Top Brands</a></li>
            <li><a href="/api/performance">Performance Metrics</a></li>
        </ul>
    </div>
    
    <iframe src="/api/summary" width="100%" height="600" frameborder="0"></iframe>
</body>
</html>
    ''')

@app.route('/api/summary')
def summary():
    try:
        # Get multiple metrics
        queries = {
            'total_products': 'SELECT COUNT(*) as count FROM ecommerce.ecom_offers',
            'avg_price': 'SELECT round(avg(price), 2) as avg FROM ecommerce.ecom_offers',
            'top_categories': '''
                SELECT category_id, COUNT(*) as count, round(avg(price), 2) as avg_price
                FROM ecommerce.ecom_offers 
                GROUP BY category_id 
                ORDER BY count DESC 
                LIMIT 10
            ''',
            'top_brands': '''
                SELECT vendor, COUNT(*) as count, round(avg(price), 2) as avg_price
                FROM ecommerce.ecom_offers 
                WHERE vendor != '' 
                GROUP BY vendor 
                ORDER BY count DESC 
                LIMIT 10
            '''
        }
        
        results = {}
        for name, query in queries.items():
            try:
                result = client.query(query)
                if 'category_id' in name or 'brands' in name:
                    results[name] = result.result_rows
                else:
                    results[name] = result.result_rows[0][0]
            except Exception as e:
                results[name] = f"Error: {str(e)}"
        
        return render_template_string('''
<!DOCTYPE html>
<html>
<head>
    <title>Detailed Results</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; }
        .metric { background: #007bff; color: white; padding: 20px; margin: 10px; border-radius: 5px; }
        table { border-collapse: collapse; width: 100%; margin: 20px 0; }
        th, td { border: 1px solid #ddd; padding: 12px; text-align: left; }
        th { background-color: #f2f2f2; }
        h2 { color: #333; }
    </style>
</head>
<body>
    <h1>E-commerce Analytics Results</h1>
    
    <h2>Total Products: {{ results.total_products }}</h2>
    
    <h2>Average Price: ₽{{ results.avg_price }}</h2>
    
    <h2>Top 10 Categories</h2>
    <table>
        <tr><th>Category ID</th><th>Products Count</th><th>Avg Price</th></tr>
        {% for row in results.top_categories %}
        <tr><td>{{ row[0] }}</td><td>{{ '{:,}'.format(row[1]) }}</td><td>₽{{ '{:,}'.format(row[2]) }}</td></tr>
        {% endfor %}
    </table>
    
    <h2>Top 10 Brands</h2>
    <table>
        <tr><th>Brand</th><th>Products Count</th><th>Avg Price</th></tr>
        {% for row in results.top_brands %}
        <tr><td>{{ row[0] }}</td><td>{{ '{:,}'.format(row[1]) }}</td><td>₽{{ '{:,}'.format(row[2]) }}</td></tr>
        {% endfor %}
    </table>
</body>
</html>
        ''', results=results)
        
    except Exception as e:
        return f"Error: {str(e)}", 500

@app.route('/api/total-products')
def total_products():
    try:
        result = client.query('SELECT COUNT(*) FROM ecommerce.ecom_offers')
        return jsonify({'total_products': result.result_rows[0][0]})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/top-categories')
def top_categories():
    try:
        result = client.query('''
            SELECT category_id, COUNT(*) as count, round(avg(price), 2) as avg_price
            FROM ecommerce.ecom_offers 
            GROUP BY category_id 
            ORDER BY count DESC 
            LIMIT 10
        ''')
        return jsonify(dict(top_categories=result.result_rows))
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
