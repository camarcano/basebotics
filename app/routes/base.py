from flask import Blueprint, render_template

# Define the Blueprint
base_bp = Blueprint('base', __name__, template_folder='templates')

# Define a route for the home page
@base_bp.route('/')
def home():
    return render_template('index.html')

# Define other routes as needed
@base_bp.route('/about')
def about():
    return render_template('about.html')
