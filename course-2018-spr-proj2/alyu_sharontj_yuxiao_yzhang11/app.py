import flask
from flask import Flask, Response, request, render_template, redirect, url_for
from flask_pymongo import PyMongo


app = Flask(__name__)
app.config['MONGO_DBNAME']='repo'
mongo = PyMongo(app)


@app.route("/", methods=['GET'])
def welcome():
    return render_template('welcome.html')


@app.route('/generate')
def renderMap():
    return render_template('generate.html')



if __name__ == "__main__":
    app.run(port=5000, debug=True)
