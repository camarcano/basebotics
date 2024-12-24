from flask import Flask, render_template
import logging
from logging.handlers import RotatingFileHandler
from .routes.dash import dashboard_bp, init_dash_app

def create_app():
    app = Flask(__name__)
    #app.config['SQLALCHEMY_DATABASE_URI'] = 'mysql+pymysql://carlos:yourpassword@localhost/balldatalab'
    app.config['SECRET_KEY'] = 'rickeyhenderson'

    # Initialize extensions
    # from .extensions import db  # Make sure db is defined in extensions
    # db.init_app(app)

    # Register blueprints
    from .routes.base import base_bp
    from .routes.sports.baseball import baseball_bp

    app.register_blueprint(base_bp)
    app.register_blueprint(baseball_bp, url_prefix='/sports')
    app.register_blueprint(dashboard_bp, url_prefix='/dashboard')

    # Initialize Dash app
    init_dash_app(app)

    return app

# Set up logging
handler = RotatingFileHandler('flask_app.log', maxBytes=10000, backupCount=1)
handler.setLevel(logging.INFO)
app.logger.addHandler(handler)

@app.before_first_request
def initialize_logging():
    app.logger.info('Flask app started')

@app.errorhandler(500)
def internal_error(error):
    app.logger.error('Server Error: %s', (error))
    return render_template('500.html'), 500

@app.errorhandler(Exception)
def unhandled_exception(e):
    app.logger.error('Unhandled Exception: %s', (e))
    return render_template('500.html'), 500