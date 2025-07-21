from flask import Flask
from controllers.filter_controller import FilterController
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)
controller = FilterController()

@app.route("/filter-data", methods=["POST"])
def filter_data():
    return controller.filter_data()

if __name__ == "__main__":
    app.run(debug=True, port=5001)
