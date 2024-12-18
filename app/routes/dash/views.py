from flask import Blueprint, render_template
from .dash_app001 import create_dash_app
from .dash_app002 import create_dash_app002

dashboard_bp = Blueprint('dashboard', __name__, template_folder='templates')

@dashboard_bp.route('/dashboard001')
def dashboard001():
    return render_template('dash/dashboard001.html')

@dashboard_bp.route('/dashboard002')
def dashboard002():
    return render_template('dash/dashboard002.html')
