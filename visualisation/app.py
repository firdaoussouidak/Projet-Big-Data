from flask import Flask, render_template, jsonify
import json
from datetime import datetime, timedelta
import random

app = Flask(__name__)

class DataSimulator:
    def __init__(self):
        self.sensor_types = ['temperature', 'humidity', 'pressure', 'light']
        self.locations = ['Tangier-Centre', 'Tangier-Port', 'Tangier-Aeroport', 
                         'Tangier-Universite', 'Tangier-Zone-Industrielle']
    
    def get_realtime_stats(self):
        """Statistiques en temps réel"""
        stats = []
        for sensor_type in self.sensor_types:
            for location in self.locations:
                stats.append({
                    'sensor_type': sensor_type,
                    'location': location,
                    'count': random.randint(50, 200),
                    'avg_value': round(random.uniform(20, 80), 2),
                    'anomalies': random.randint(0, 10)
                })
        return stats
    
    def get_hourly_data(self):
        """Données horaires pour graphiques"""
        hours = []
        now = datetime.now()
        
        for i in range(24):
            hour_time = (now - timedelta(hours=23-i)).strftime('%H:00')
            hours.append({
                'hour': hour_time,
                'temperature': round(random.uniform(18, 30), 1),
                'humidity': round(random.uniform(40, 80), 1),
                'pressure': round(random.uniform(1000, 1020), 1),
                'anomalies': random.randint(0, 5)
            })
        
        return hours
    
    def get_sensor_distribution(self):
        """Distribution des capteurs"""
        distribution = []
        for sensor_type in self.sensor_types:
            distribution.append({
                'type': sensor_type,
                'count': random.randint(100, 500),
                'anomaly_rate': round(random.uniform(1, 8), 2)
            })
        return distribution
    
    def get_location_stats(self):
        """Statistiques par localisation"""
        stats = []
        for location in self.locations:
            stats.append({
                'location': location,
                'sensors': random.randint(10, 25),
                'messages': random.randint(500, 2000),
                'anomalies': random.randint(5, 50),
                'avg_battery': round(random.uniform(50, 95), 1)
            })
        return stats
    
    def get_alerts(self):
        """Alertes récentes"""
        alerts = []
        alert_types = ['Anomalie detectee', 'Batterie faible', 'Valeur critique']
        
        for i in range(10):
            alerts.append({
                'sensor_id': f'SENSOR_{random.randint(1, 20):03d}',
                'type': random.choice(alert_types),
                'severity': random.choice(['high', 'medium', 'low']),
                'timestamp': (datetime.now() - timedelta(minutes=random.randint(1, 60))).strftime('%H:%M:%S'),
                'message': f'Alerte capteur: valeur anormale detectee'
            })
        
        return alerts
    
    def get_kpis(self):
        """Indicateurs clés de performance"""
        return {
            'total_sensors': random.randint(15, 25),
            'active_sensors': random.randint(12, 24),
            'total_messages': random.randint(5000, 15000),
            'total_anomalies': random.randint(50, 200),
            'anomaly_rate': round(random.uniform(1, 5), 2),
            'avg_battery': round(random.uniform(65, 85), 1),
            'low_battery_sensors': random.randint(2, 8)
        }

simulator = DataSimulator()

@app.route('/')
def index():
    """Page principale du dashboard"""
    return render_template('dashboard.html')

@app.route('/api/kpis')
def api_kpis():
    """API pour les KPIs"""
    return jsonify(simulator.get_kpis())

@app.route('/api/realtime')
def api_realtime():
    """API pour les données en temps réel"""
    return jsonify(simulator.get_realtime_stats())

@app.route('/api/hourly')
def api_hourly():
    """API pour les données horaires"""
    return jsonify(simulator.get_hourly_data())

@app.route('/api/distribution')
def api_distribution():
    """API pour la distribution des capteurs"""
    return jsonify(simulator.get_sensor_distribution())

@app.route('/api/locations')
def api_locations():
    """API pour les statistiques par localisation"""
    return jsonify(simulator.get_location_stats())

@app.route('/api/alerts')
def api_alerts():
    """API pour les alertes"""
    return jsonify(simulator.get_alerts())

if __name__ == '__main__':
    print("\n" + "=" * 70)
    print("  DASHBOARD IOT - VISUALISATION EN TEMPS REEL")
    print("=" * 70)
    print("\nServeur Flask demarre sur: http://localhost:5000")
    print("Appuyez sur Ctrl+C pour arreter\n")
    
    app.run(host='0.0.0.0', port=5000, debug=True)