from .views import dashboard_bp
from .dash_app001 import create_dash_app
from .dash_app002 import create_dash_app002

def init_dash_app(app):
    create_dash_app(app)
    create_dash_app002(app)

__all__ = ['dashboard_bp', 'init_dash_app']
