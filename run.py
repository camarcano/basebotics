from .app import create_app
import logging
from logging.handlers import RotatingFileHandler

app = create_app()

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

if __name__ == '__main__':
    app.run()
