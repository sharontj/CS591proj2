import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import csv



class garden(dml.Algorithm):
    contributor = 'alyu_sharontj_yuxiao_yzhang11'
    reads = []
    writes = ['alyu_sharontj_yuxiao_yzhang11.garden','alyu_sharontj_yuxiao_yzhang11.garden_count','alyu_sharontj_yuxiao_yzhang11.garden_new_zip' ]

    @staticmethod
    def execute(trial=False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('alyu_sharontj_yuxiao_yzhang11', 'alyu_sharontj_yuxiao_yzhang11')

        url = 'http://datamechanics.io/data/alyu_sharontj_yuxiao_yzhang11/garden_json.json'
        response_json = urllib.request.urlopen(url).read().decode("utf-8")
        #print("i am here!!!!")
        #print(response_json)
        r = json.loads(response_json)

        # url2014 = 'http://datamechanics.io/data/2014fireincident_anabos2.json'
        # response2014 = urllib.request.urlopen(url2014).read().decode("utf-8")
        # fire2014 = json.loads(response2014)

        # url2015 = 'http://datamechanics.io/data/2015fireincident_anabos2.json'
        # response2015 = urllib.request.urlopen(url2015).read().decode("utf-8")
        # fire2015 = json.loads(response2015)


        # dict_values = csvConvert()

        repo.dropCollection("garden")
        repo.createCollection("garden")
        repo['alyu_sharontj_yuxiao_yzhang11.garden'].insert_many(r)


        garden = repo['alyu_sharontj_yuxiao_yzhang11.garden'].find()

        repo.dropCollection("garden_new_zip")
        repo.createCollection("garden_new_zip")

        for i in garden:
            garden_new_zip = {}
            old_zip = i["zip_code"]
            new_zip = "0" + old_zip
            garden_new_zip["zip"] = new_zip
            garden_new_zip["location"] = i["location"]
            garden_new_zip["site"] = i["site"]
            repo['alyu_sharontj_yuxiao_yzhang11.garden_new_zip'].insert(garden_new_zip)


        repo.dropCollection("garden_count")
        repo.createCollection("garden_count")

        garden_with_new_zip = repo['alyu_sharontj_yuxiao_yzhang11.garden_new_zip']

        group = {
            '_id': "$zip",
            'count': {'$sum': 1}
        }

        gardenCount = garden_with_new_zip.aggregate([
            {
                '$group': group
            }
        ])

        repo['alyu_sharontj_yuxiao_yzhang11.garden_count'].insert(gardenCount)

        endTime = datetime.datetime.now()

        return {"start": startTime, "end": endTime}

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
        '''
            Create the provenance document describing everything happening
            in this script. Each run of the script will generate a new
            document describing that invocation event.
            '''

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('alyu_sharontj_yuxiao_yzhang11', 'alyu_sharontj_yuxiao_yzhang11')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont',
                          'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        doc.add_namespace('bdp', 'https://data.boston.gov/export/767/71c/')


        this_script = doc.agent('alg:alyu_sharontj_yuxiao_yzhang11#garden',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

        resource = doc.entity('dat:2013fireincident_anabos2',
                              {'prov:label': '311, Service Requests', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'json'})

        this_run = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.usage(this_run, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval',})

        output =  doc.entity('dat:alyu_sharontj_yuxiao_yzhang11.garden', {prov.model.PROV_LABEL:'garden', prov.model.PROV_TYPE:'ont:DataSet'})

        # get_lost = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        #
        doc.wasAssociatedWith(this_run, this_script)
        doc.used(this_run, resource, startTime)
        # doc.wasAssociatedWith(get_lost, this_script)
        # doc.usage(get_found, resource, startTime, None,
        #           {prov.model.PROV_TYPE: 'ont:Retrieval',
        #            'ont:Query': '?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'
        #            }
        #           )
        #
        # doc.usage(get_lost, resource, startTime, None,
        #           {prov.model.PROV_TYPE: 'ont:Retrieval',
        #            'ont:Query': '?type=Animal+Lost&$select=type,latitude,longitude,OPEN_DT'
        #            }
        #           )

        # lost = doc.entity('dat:alice_bob#lost',
        #                   {prov.model.PROV_LABEL: 'Animals Lost', prov.model.PROV_TYPE: 'ont:DataSet'})
        # doc.wasAttributedTo(lost, this_script)
        # doc.wasGeneratedBy(lost, get_lost, endTime)
        # doc.wasDerivedFrom(lost, resource, get_lost, get_lost, get_lost)
        #
        # found = doc.entity('dat:alice_bob#found',
        #                    {prov.model.PROV_LABEL: 'Animals Found', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(output, this_script)
        doc.wasGeneratedBy(output, this_run, endTime)
        doc.wasDerivedFrom(output, resource, this_run, this_run, this_run)

        repo.logout()

        return doc


garden.execute()
doc = garden.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
