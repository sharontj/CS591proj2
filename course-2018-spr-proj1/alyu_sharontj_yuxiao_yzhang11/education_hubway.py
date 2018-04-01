import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import re
from alyu_sharontj_yuxiao_yzhang11.Util.Util import *



class education_hubway(dml.Algorithm):
    contributor = 'alyu_sharontj_yuxiao_yzhang11'
    reads = ['alyu_sharontj_yuxiao_yzhang11.education', 'alyu_sharontj_yuxiao_yzhang11.hubway'] #read the data of roads and trafficsignals from mongo
    writes = ['alyu_sharontj_yuxiao_yzhang11.education_hubway']


    @staticmethod
    def execute(trial=False):
        startTime = datetime.datetime.now()

        '''Set up the database connection.'''
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('alyu_sharontj_yuxiao_yzhang11', 'alyu_sharontj_yuxiao_yzhang11')


        '''get (schoolid,zipcode,latitude,longitute) from alyu_sharontj_yuxiao_yzhang11.education'''
        schoolinfo = []
        edudb = repo['alyu_sharontj_yuxiao_yzhang11.education']
        educur = edudb.find()  #filter not work
        for info in educur:
            school_id = info['properties']['SchoolId']
            if (school_id != "0"):
                address = info['properties']['Address']
                zipcode = address[-5: ]
                print(zipcode)
                Latitude = float(info['properties']['Latitude'])
                Longitude = float(info['properties']['Longitude'])
                schoolinfo.append((school_id, zipcode, (Latitude, Longitude)))
        # print(schoolinfo)


        hubwaydb = repo['alyu_sharontj_yuxiao_yzhang11.hubway']
        hubwayinfo = []
        match = {
            'status': "Existing"
        }
        hubwayExist = hubwaydb.aggregate([
            {
                '$match': match
            }
        ])
        for info in hubwayExist:
            hubway_id = info['id']
            Latitude = float(info['lat'])
            Longitude = float(info['lng'])
            hubwayinfo.append((hubway_id,(Latitude,Longitude)))
        # print(hubwayinfo)

        edu_hub = [(s[0],s[1], h[0], distance(s[2], h[1])) for (s, h) in product(schoolinfo, hubwayinfo)]
        print(len(edu_hub))

        edu_hub_3 = [ (s,zip,h,dis) for (s,zip,h,dis) in edu_hub if dis<3]
        print(len(edu_hub_3))

        repo.dropCollection("education_hubway")
        repo.createCollection("education_hubway")
        for i in edu_hub_3:
            single = {'schoolid':i[0], 'zip':i[1], 'dis':i[3]}
            repo['alyu_sharontj_yuxiao_yzhang11.education_hubway'].insert_one(single)


        repo.dropCollection("education_hubway_count")
        repo.createCollection("education_hubway_count")
        edu_hubdb = repo['alyu_sharontj_yuxiao_yzhang11.education_hubway']
        group = {
            '_id': { "schoolid":"$schoolid", "zip":"$zip" },
            'hubways': {"$sum": 1}
        }

        edu_hub_count = edu_hubdb.aggregate([
            {
                '$group': group
            }
        ])
        repo['alyu_sharontj_yuxiao_yzhang11.education_hubway_count'].insert(edu_hub_count)


        endTime = datetime.datetime.now()

        return {"start": startTime, "end": endTime}


    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        '''
            Create the provenance document describing everything happening
            in this script. Each run of the script will generate a new
            document describing that invocation event.
            '''

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('alyu_sharontj_yuxiao_yzhang11', 'alyu_sharontj_yuxiao_yzhang11')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/alyu_sharontj_yuxiao_yzhang11') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/alyu_sharontj_yuxiao_yzhang11') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        # doc.add_namespace('bdp', 'http://bostonopendata-boston.opendata.arcgis.com/datasets/')
        # doc.add_namespace('hdv', 'https://dataverse.harvard.edu/dataset.xhtml')

        this_script = doc.agent('alg:alyu_sharontj_yuxiao_yzhang11#education_hubway',
            { prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})


        education_input = doc.entity('dat:alyu_sharontj_yuxiao_yzhang11.education',
                                {prov.model.PROV_LABEL:'education',
                                 prov.model.PROV_TYPE:'ont:DataSet'})

        hubway_input = doc.entity('dat:alyu_sharontj_yuxiao_yzhang11.hubway',
                                  {prov.model.PROV_LABEL:'hubway',
                                   prov.model.PROV_TYPE:'ont:DataSet'})

        this_run = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime)#, 'ont:Query':'?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'})


        output = doc.entity('dat:alyu_sharontj_yuxiao_yzhang11.education_hubway',
            { prov.model.PROV_LABEL:'education_hubway', prov.model.PROV_TYPE: 'ont:DataSet'})


        doc.wasAssociatedWith(this_run, this_script)
        doc.used(this_run, education_input, startTime)
        doc.used(this_run, hubway_input, startTime)

        doc.wasAttributedTo(output, this_script)
        doc.wasGeneratedBy(output, this_run, endTime)
        doc.wasDerivedFrom(output, education_input, this_run, this_run, this_run)
        doc.wasDerivedFrom(output, hubway_input, this_run, this_run, this_run)
        repo.logout()


        return doc




education_hubway.execute()
doc = education_hubway.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
