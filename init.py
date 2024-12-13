from flask import Flask
from .extensions import jwt
from .config import Config

def create_app():
    app = Flask(__name__)
    
    app.config.from_object(Config)
    
    jwt.init_app(app)

    from .resources.item import blp  
    app.register_blueprint(blp)  

    return app
