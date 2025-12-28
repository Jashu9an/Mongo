from flask import Flask, jsonify
import clickhouse_connect
import json

app = Flask(__name__)

# Connect to ClickHouse
try:
    client = clickhouse_connect.get_client(
        host='localhost',
        port=8123,
        database='ecommerce'
    )
    print("Connected to ClickHouse successfully")
except Exception as e:
    print(f"ClickHouse connection error: {e}")
    client = None

@app.route('/metrics/ecommerce')
def ecommerce_metrics():
    """ReturnClickHouse metrics for Prometheus/Grafana"""
    if not client:
        return jsonify({'error': 'ClickHouse not connected'})
    
    try:
        queries = {
            'total_products': 'SELECT COUNT(*) FROM ecommerce.ecom_offers',
            'avg_price': 'SELECT round(avg(price), 2) FROM ecommerce.ecom_offers',
            'unique_categories': 'SELECT countDistinct(category_id) FROM ecommerce.ecom_offers',
            'unique_brands': 'SELECT countDistinct(vendor) FROM ecommerce.ecom_offers WHERE vendor != \'\'',
            'total_events': 'SELECT COUNT(*) FROM ecommerce.raw_events',
        }
        
        # Performance comparison queries
        perf_queries = {
            'raw_query_time': 'SELECT round(avg(query_duration_ms) / 1000, 3) FROM system.query_log WHERE query LIKE \'%ecom_offers%\' AND type = \'QueryFinish\' AND event_time > now() - INTERVAL 1 HOUR',
            'mv_query_time': 'SELECT round(avg(query_duration_ms) / 1000, 3) FROM system.query_log WHERE query LIKE \'%catalog_by_category_mv%\' AND type = \'QueryFinish\' AND event_time > now() - INTERVAL 1 HOUR',
            'mv_speedup_ratio_raw': 'SELECT round(max(query_duration_ms) / 1000, 3) FROM system.query_log WHERE query LIKE \'%ecom_offers%\' AND type = \'QueryFinish\' AND event_time > now() - INTERVAL 1 HOUR',
            'mv_speedup_ratio_mv': 'SELECT round(avg(query_duration_ms) / 1000, 3) FROM system.query_log WHERE query LIKE \'%catalog_by_category_mv%\' AND type = \'QueryFinish\' AND event_time > now() - INTERVAL 1 HOUR',
            'qps_total': 'SELECT count() / 3600 FROM system.query_log WHERE type = \'QueryFinish\' AND event_time > now() - INTERVAL 1 HOUR',
            'memory_usage_bytes': 'SELECT toUInt64(sum(memory_usage))/1024/1024 as memory_mb FROM system.query_log WHERE type = \'QueryFinish\' AND event_time > now() - INTERVAL 1 HOUR',
        }
        
        metrics = {}
        for name, query in queries.items():
            try:
                result = client.query(query)
                metrics[name] = result.result_rows[0][0]
            except Exception as e:
                metrics[name] = 0
        
        # Performance metrics
        for name, query in perf_queries.items():
            try:
                result = client.query(query)
                if 'speedup_ratio' in name:
                    if name.endswith('_raw'):
                        metrics['speedup_raw'] = result.result_rows[0][0] if result.result_rows[0][0] else 1
                    elif name.endswith('_mv'):
                        mv_time = result.result_rows[0][0] if result.result_rows[0][0] else 1
                        raw_time = metrics.get('speedup_raw', 1)
                        speedup = round(raw_time / mv_time, 2) if mv_time > 0 else 1
                        metrics[name] = speedup
                        metrics['speedup_ratio'] = speedup
                else:
                    metrics[name] = result.result_rows[0][0] if result.result_rows[0][0] else 0
            except Exception as e:
                metrics[name] = 0
                
        # Format for Prometheus
        prometheus_metrics = []
        for key, value in metrics.items():
            if key not in ['speedup_raw']:  # Skip internal metric
                prometheus_metrics.append(f'ecommerce_{key} {value}')
            
        return '\n'.join(prometheus_metrics), 200, {'Content-Type': 'text/plain'}
        
    except Exception as e:
        return jsonify({'error': str(e)})

@app.route('/health')
def health():
    return jsonify({'status': 'ok', 'clickhouse': client is not None})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)
