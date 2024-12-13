from flask import request, jsonify
from flask_smorest import Blueprint
from flask_jwt_extended import jwt_required
from app.extensions import get_db_connection

blp = Blueprint('items', name, url_prefix='/items', description='Operations on items')


def execute_query(query, params=None, fetch_one=False, fetch_all=False, commit=False):
    """
    Вспомогательная функция для выполнения запросов к базе данных.
    """
    connection = None
    try:
        connection = get_db_connection()
        cursor = connection.cursor()
        cursor.execute(query, params or ())
        
        if commit:
            connection.commit()
        
        if fetch_one:
            return cursor.fetchone()
        
        if fetch_all:
            return cursor.fetchall()
        
    except Exception as e:
        print(f"Ошибка при выполнении запроса: {e}")
        return None
    
    finally:
        if connection:
            connection.close()


@blp.route('/', methods=['GET', 'POST'])
@jwt_required()
def items():
    if request.method == 'GET':
        """
        Получить список всех элементов.
        """
        items = execute_query("SELECT * FROM items;", fetch_all=True)
        if items is None:
            return jsonify({"message": "Ошибка при получении данных"}), 500
        return jsonify(items)

    elif request.method == 'POST':
        """
        Создать новый элемент.
        """
        new_item = request.json
        if not new_item or not all(k in new_item for k in ['name', 'description', 'price']):
            return jsonify({"message": "Некорректные данные"}), 400

        query = """
        INSERT INTO items (name, description, price) 
        VALUES (%s, %s, %s) RETURNING id;
        """
        params = (new_item['name'], new_item['description'], new_item['price'])
        item_id = execute_query(query, params=params, fetch_one=True, commit=True)

        if item_id is None:
            return jsonify({"message": "Ошибка при создании элемента"}), 500

        return jsonify({"id": item_id[0]}), 201


@blp.route('/<int:item_id>', methods=['GET', 'PUT', 'DELETE'])
@jwt_required()
def item(item_id):
    if request.method == 'GET':
        """
        Получить элемент по ID.
        """
        query = "SELECT * FROM items WHERE id = %s;"
        item = execute_query(query, params=(item_id,), fetch_one=True)

        if item is None:
            return jsonify({"message": "Элемент не найден"}), 404

        return jsonify(item)

    elif request.method == 'PUT':
        """
        Обновить элемент по ID.
        """
        updated_data = request.json
        if not updated_data or not all(k in updated_data for k in ['name', 'description', 'price']):
            return jsonify({"message": "Некорректные данные"}), 400

        query = """
        UPDATE items 
        SET name = %s, description = %s, price = %s 
        WHERE id = %s;
        """
        params = (updated_data['name'], updated_data['description'], updated_data['price'], item_id)
        result = execute_query(query, params=params, commit=True)

        if result is None:
            return jsonify({"message": "Ошибка при обновлении элемента"}), 500

        return jsonify({"message": "Элемент успешно обновлен"}), 200

    elif request.method == 'DELETE':
        """
        Удалить элемент по ID.
        """
        query = "DELETE FROM items WHERE id = %s;"
        result = execute_query(query, params=(item_id,), commit=True)

        if result is None:
            return jsonify({"message": "Ошибка при удалении элемента"}), 500

        return jsonify({"message": "Элемент успешно удален"}), 200
