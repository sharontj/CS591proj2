import flask
from flask import Flask, Response, request, render_template, redirect, url_for
from flask_pymongo import PyMongo
# from alyu_sharontj_yuxiao_yzhang11.Mapping import *




import sys
sys.path.append("/Users/sharontj1/Desktop/CS591proj2/course-2018-spr-proj2/alyu_sharontj_yuxiao_yzhang11")
# from mapping import Mapping



import json
# import sys
sys.path.append('..')
import folium
print (folium.__file__)
print (folium.__version__)
from branca.colormap import linear
import dml


app = Flask(__name__)
app.config['MONGO_DBNAME']='repo'
mongo = PyMongo(app)



def Mapping():
    client = dml.pymongo.MongoClient()
    repo = client.repo
    repo.authenticate('alyu_sharontj_yuxiao_yzhang11', 'alyu_sharontj_yuxiao_yzhang11')
    dbresults = repo['alyu_sharontj_yuxiao_yzhang11.Result'].find()


    result = []
    for info in dbresults:
        zipcode = info['Zipcode']
        score = info['score']
        result.append((zipcode, score))

    print(result)

    dictR = dict(result)
    print(dictR['02215'])


    # import geo json data
    geo_json_data = json.load(open('ZIP_Codes.geojson'))


    colormap = linear.YlOrRd.scale(
        result[-1][1],
        result[0][1]
    )


    # create map, center it on Boston
    m = folium.Map([42.324725, -71.093327], tiles='CartoDB Positron', zoom_start=12)


    def styleFunc(feature):
        x = dictR.get(feature['properties']['ZIP5'],None)
        return {
            'fillColor': '#d3d3d3' if x is None else colormap(x),
            'color': 'black',
            'weight': 1,
            'dashArray': '5, 5',
            'fillOpacity': .9,
        }

    # apply the Boston zipcode areas outlines to the map
    folium.GeoJson(geo_json_data,
                  style_function= styleFunc
                  ).add_to(m)

    # display map
    m.save("templates/heatafter.html")








@app.route("/", methods=['GET'])
def welcome():
    return render_template('welcome.html')


@app.route('/generate')
def renderMap():

    Mapping()

    return render_template('generate.html')




if __name__ == "__main__":
    app.run(port=5000, debug=True)
