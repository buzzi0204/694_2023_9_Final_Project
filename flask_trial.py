from flask import Flask, jsonify, request, render_template
import MySQLdb
import pandas as pd
from pymongo import MongoClient
from implementing_cache import Cache
from bson import json_util, Int64
from time import time
from search_class import searchFunctions

app = Flask(__name__, static_folder='static')

search = searchFunctions()
    


@app.route('/')
def home():
    return render_template("/html/form.html")

@app.route('/search', methods=['GET', 'POST'])
def helloworld():
    if request.method == "POST":
        option = request.form["options"]
        query = request.form["query"]
    if option == "keyword":
        data = search.get_keyword(query)
    elif option == "hashtag":
        data = search.get_hashtag(query)
    else:
        data = search.get_username(query)
    return render_template("/html/query.html", name = query,value = data.to_html())

@app.route('/top_10_tweets')
def top_10_tweets():
    data = search.get_top_10_tweets()
    return render_template("/html/top_10.html",value = data.to_html())

@app.route('/top_10_users')
def top_10_users():
    data = search.get_top_10_users()
    return render_template("/html/top_10.html",value = data.to_html())
  
if __name__ == '__main__':
    app.run(debug=True)