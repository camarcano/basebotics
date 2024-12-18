from flask import Blueprint, render_template

# Define the Blueprint
baseball_bp = Blueprint('baseball', __name__, template_folder='templates')

# Example route
@baseball_bp.route('/baseball')
def baseball():
    return render_template('sports/baseball.html', title="Baseball Home")
