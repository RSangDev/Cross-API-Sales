from flask import Flask, jsonify, request
from faker import Faker
import faker_commerce 
import random

app = Flask(__name__)
fake = Faker('en_US')
# Registra o provedor externo corretamente
fake.add_provider(faker_commerce.Provider)

def generate_product_data(num_items=10, marketplace='ebay'):
    products = []
    for _ in range(num_items):
        product = {
            'id': fake.uuid4(),
            # Alterado de product_name() para ecommerce_name()
            'nome': fake.ecommerce_name() if random.random() > 0.2 else None,
            'preco': round(random.uniform(10, 1000), 2) if random.random() > 0.1 else 'invalido',
            'categoria': random.choice(['eletronicos', 'roupas', 'casa']) if random.random() > 0.15 else duplicate_category(),
            'moeda': 'USD' if marketplace == 'ebay' else 'BRL'
        }
        products.append(product)
    return products

def duplicate_category():
    return 'eletronicos'

@app.route('/ebay/produtos', methods=['GET'])
def ebay_products():
    num = int(request.args.get('num', 10))
    return jsonify(generate_product_data(num, 'ebay'))

@app.route('/mercadolivre/produtos', methods=['GET'])
def ml_products():
    num = int(request.args.get('num', 10))
    return jsonify(generate_product_data(num, 'ml'))

@app.route('/health', methods=['GET'])
def health():
    return 'OK', 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=False, port=5000)  # Muda para False