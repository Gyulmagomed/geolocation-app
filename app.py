from flask import Flask, request, jsonify
from sqlite3 import connect, Error as SQLiteError
import logging
from datetime import datetime
from functools import wraps
from flask_cors import CORS
import json

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

CORS(app, resources={r"/*": {"origins": "*"}})

# Конфигурация
class Config:
    DATABASE = "geodata.db"
    MAX_COORDINATE_VALUE = {"latitude": 90, "longitude": 180}
    MIN_COORDINATE_VALUE = {"latitude": -90, "longitude": -180}

app.config.from_object(Config)

# Подключение к БД с обработкой ошибок
def get_db():
    """Получение соединения с БД"""
    try:
        conn = connect(app.config['DATABASE'])
        conn.row_factory = lambda cursor, row: {
            col[0]: row[idx] for idx, col in enumerate(cursor.description)
        } if cursor.description else None
        return conn
    except SQLiteError as e:
        logger.error(f"Ошибка подключения к БД: {e}")
        raise

def init_database():
    """Инициализация базы данных при запуске"""
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            
            # Создание таблицы
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS locations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    latitude REAL NOT NULL,
                    longitude REAL NOT NULL,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Создание индексов для оптимизации запросов
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_timestamp 
                ON locations(timestamp)
            ''')
            
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_coordinates 
                ON locations(latitude, longitude)
            ''')
            
            # Создание таблицы для логов ошибок (опционально)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS error_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    error_message TEXT,
                    endpoint TEXT,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            conn.commit()
            logger.info("База данных инициализирована успешно")
            
    except SQLiteError as e:
        logger.error(f"Ошибка инициализации БД: {e}")
        raise

def validate_coordinates(latitude, longitude):
    """
    Валидация координат
    
    Args:
        latitude: широта
        longitude: долгота
        
    Returns:
        tuple: (валидированные lat, lon) или None при ошибке
    """
    try:
        # Преобразование в float
        lat = float(latitude)
        lon = float(longitude)
        
        # Проверка диапазонов
        if not (Config.MIN_COORDINATE_VALUE['latitude'] <= lat <= 
                Config.MAX_COORDINATE_VALUE['latitude']):
            logger.warning(f"Некорректная широта: {lat}")
            return None
            
        if not (Config.MIN_COORDINATE_VALUE['longitude'] <= lon <= 
                Config.MAX_COORDINATE_VALUE['longitude']):
            logger.warning(f"Некорректная долгота: {lon}")
            return None
            
        return lat, lon
        
    except (ValueError, TypeError) as e:
        logger.warning(f"Ошибка преобразования координат: {e}")
        return None

def log_error_to_db(error_message, endpoint):
    """Логирование ошибок в БД"""
    try:
        with get_db() as conn:
            conn.execute(
                'INSERT INTO error_logs (error_message, endpoint) VALUES (?, ?)',
                (error_message[:500], endpoint)  # Ограничиваем длину сообщения
            )
            conn.commit()
    except SQLiteError as e:
        logger.error(f"Не удалось записать ошибку в БД: {e}")

def require_json(f):
    """Декоратор для проверки Content-Type"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not request.is_json:
            logger.warning("Запрос без Content-Type: application/json")
            return jsonify({
                "error": "Content-Type должен быть application/json"
            }), 415
        return f(*args, **kwargs)
    return decorated_function

def rate_limit(max_requests=100, window=60):
    """Простой rate limiting (для примера)"""
    requests = {}
    
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            client_ip = request.remote_addr
            current_time = datetime.now().timestamp()
            
            # Очистка старых записей
            requests[client_ip] = [
                req_time for req_time in requests.get(client_ip, [])
                if current_time - req_time < window
            ]
            
            # Проверка лимита
            if len(requests[client_ip]) >= max_requests:
                logger.warning(f"Превышен лимит запросов для IP: {client_ip}")
                return jsonify({
                    "error": "Превышен лимит запросов. Попробуйте позже."
                }), 429
            
            # Добавление текущего запроса
            requests[client_ip].append(current_time)
            
            return f(*args, **kwargs)
        return decorated_function
    return decorator

# Инициализация БД при запуске
with app.app_context():
    init_database()

# Эндпоинт для приема данных
@app.route('/save_location', methods=['POST'])
@require_json
@rate_limit(max_requests=60, window=60)  # 60 запросов в минуту
def save_location():
    """
    Сохранение геолокационных данных
    
    Пример запроса:
    POST /save_location
    Content-Type: application/json
    {
        "latitude": 55.7558,
        "longitude": 37.6173
    }
    """
    try:
        # Получение и проверка данных
        data = request.get_json(silent=True)
        
        if not data:
            logger.warning("Получен пустой JSON")
            return jsonify({"error": "Неверный формат JSON"}), 400
        
        # Извлечение координат
        lat = data.get('latitude')
        lon = data.get('longitude')
        
        if lat is None or lon is None:
            logger.warning("Отсутствуют координаты в запросе")
            return jsonify({
                "error": "Отсутствуют обязательные поля: latitude и longitude"
            }), 400
        
        # Валидация координат
        validated_coords = validate_coordinates(lat, lon)
        if not validated_coords:
            return jsonify({"error": "Неверные координаты"}), 400
        
        lat, lon = validated_coords
        
        # Сохранение в БД
        try:
            with get_db() as conn:
                cursor = conn.cursor()
                
                # Вставка данных с явным указанием timestamp
                cursor.execute('''
                    INSERT INTO locations (latitude, longitude, timestamp)
                    VALUES (?, ?, datetime('now'))
                ''', (lat, lon))
                
                # Получение ID новой записи
                location_id = cursor.lastrowid
                
                conn.commit()
                
                logger.info(f"Сохранена локация ID: {location_id}, lat: {lat}, lon: {lon}")
                
                return jsonify({
                    "status": "OK",
                    "message": "Локация сохранена успешно",
                    "location_id": location_id,
                    "latitude": lat,
                    "longitude": lon
                }), 200
                
        except SQLiteError as e:
            logger.error(f"Ошибка базы данных: {e}")
            log_error_to_db(str(e), '/save_location')
            return jsonify({
                "error": "Внутренняя ошибка сервера при сохранении данных"
            }), 500
            
    except Exception as e:
        logger.error(f"Неожиданная ошибка: {e}")
        log_error_to_db(str(e), '/save_location')
        return jsonify({
            "error": "Внутренняя ошибка сервера"
        }), 500

# Дополнительный эндпоинт для получения статистики
@app.route('/statistics', methods=['GET'])
def get_statistics():
    """Получение статистики по сохраненным локациям"""
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            
            # Общее количество записей
            cursor.execute('SELECT COUNT(*) as count FROM locations')
            total = cursor.fetchone()['count']
            
            # Последняя запись
            cursor.execute('''
                SELECT latitude, longitude, timestamp 
                FROM locations 
                ORDER BY timestamp DESC 
                LIMIT 1
            ''')
            last_location = cursor.fetchone()
            
            # Первая запись
            cursor.execute('''
                SELECT timestamp 
                FROM locations 
                ORDER BY timestamp ASC 
                LIMIT 1
            ''')
            first_record = cursor.fetchone()
            
            return jsonify({
                "status": "OK",
                "statistics": {
                    "total_locations": total,
                    "last_location": last_location,
                    "first_record_date": first_record['timestamp'] if first_record else None
                }
            }), 200
            
    except SQLiteError as e:
        logger.error(f"Ошибка при получении статистики: {e}")
        return jsonify({"error": "Ошибка получения статистики"}), 500

# Эндпоинт для проверки здоровья
@app.route('/health', methods=['GET'])
def health_check():
    """Проверка работоспособности сервиса"""
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT 1')
            db_ok = cursor.fetchone() is not None
            
        return jsonify({
            "status": "healthy",
            "database": "connected" if db_ok else "disconnected",
            "timestamp": datetime.now().isoformat()
        }), 200
        
    except SQLiteError as e:
        logger.error(f"Ошибка проверки здоровья: {e}")
        return jsonify({
            "status": "unhealthy",
            "database": "disconnected",
            "error": str(e)
        }), 503

# Глобальный обработчик ошибок
@app.errorhandler(404)
def not_found(error):
    return jsonify({"error": "Ресурс не найден"}), 404

@app.errorhandler(500)
def internal_error(error):
    logger.error(f"Internal server error: {error}")
    return jsonify({"error": "Внутренняя ошибка сервера"}), 500

if __name__ == '__main__':
    # В production используйте debug=False
    app.run(
        host='0.0.0.0',  # Доступ со всех интерфейсов
        port=5000,
        debug=False  # Всегда False в production!
    )