from flask import Flask

from app.config import Config
from app.extensions import db


def create_app() -> Flask:
    app = Flask(__name__)
    app.config.from_object(Config)

    db.init_app(app)

    from app.routes import main_bp

    app.register_blueprint(main_bp)

    with app.app_context():
        db.create_all()

    return app