from flask import Flask, render_template
import logging
from logging.handlers import RotatingFileHandler
from .routes.dash import dashboard_bp, init_dash_app

def create_app():
    app = Flask(__name__)
    app.config['SECRET_KEY'] = 'rickeyhenderson'

    # Register blueprints
    from .routes.base import base_bp
    from .routes.sports.baseball import baseball_bp

    app.register_blueprint(base_bp)
    app.register_blueprint(baseball_bp, url_prefix='/sports')
    app.register_blueprint(dashboard_bp, url_prefix='/dashboard')

    # Initialize Dash app
    init_dash_app(app)

    # Set up logging with timestamps
    handler = RotatingFileHandler('/var/www/basebotics/logs/flask_app.log', maxBytes=10000, backupCount=1)
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    app.logger.addHandler(handler)

    @app.before_request
    def initialize_logging():
        app.logger.info('Flask app started')

    @app.errorhandler(500)
    def internal_error(error):
        app.logger.error('Server Error: %s', error)
        return render_template('500.html'), 500

    @app.errorhandler(Exception)
    def unhandled_exception(e):
        app.logger.error('Unhandled Exception: %s', e)
        return render_template('500.html'), 500

    return app
