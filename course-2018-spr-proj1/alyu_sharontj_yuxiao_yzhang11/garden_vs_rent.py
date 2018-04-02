import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import pprint
import csv



class garden_vs_rent(dml.Algorithm):


    contributor = 'alyu_sharontj_yuxiao_yzhang11'
    reads = ['alyu_sharontj_yuxiao_yzhang11.garden_count','alyu_sharontj_yuxiao_yzhang11.average_rent_zip']
    writes = ['alyu_sharontj_yuxiao_yzhang11.garden_vs_rent']

    @staticmethod
    def execute(trial=False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('alyu_sharontj_yuxiao_yzhang11', 'alyu_sharontj_yuxiao_yzhang11')




        garden_count = repo['alyu_sharontj_yuxiao_yzhang11.garden_count']
        average_rent = repo['alyu_sharontj_yuxiao_yzhang11.average_rent_zip']

        # print("i am here ")
        # s = average_rent.find_one({"Zip": "11111"})
        # print(s == None)
        #pprint.pprint(average_rent.find_one({"Zip": "02169"}))
        repo.dropCollection("garden_vs_rent")
        repo.createCollection("garden_vs_rent")

        garden_cur = garden_count.find()
        rent_cur = average_rent.find()
        for i in rent_cur:
            garden_vs_rent= {}
            rent_zip = i["Zip"]
            print("rent zip", rent_zip)
            garden_vs_rent["Zip"] = rent_zip
            garden_vs_rent["Average"] = i["Average"]


            if (garden_count.find_one({"_id": rent_zip}) != None):
                print(garden_count.find_one({"_id": rent_zip})['count'])

                garden_vs_rent["garden_count"] = garden_count.find_one({"_id": rent_zip})['count']
            else:
                garden_vs_rent["garden_count"] = 0

            repo['alyu_sharontj_yuxiao_yzhang11.garden_vs_rent'].insert(garden_vs_rent)





        # gardeninfo = []
        # garden_cur = garden_count.find()  # filter not work
        # for info in garden_cur:
        #     zip = info['_id']
        #     count = info['count']
        #     #print("zip is", zip)
        #     #print("count is ", count)
        #
        #     gardeninfo.append((zip,count))
        # #print("garden ", gardeninfo)
        #
        # rent_info = []
        # rent_cur = average_rent.find()
        # for info in rent_cur:
        #     zip = info['Zip']
        #     average = info['Average']
        #     #print("zip is", zip)
        #     #print("average is ", average)
        #
        #     rent_info.append((zip, average))
        # #print("rent info is ", rent_info)
        # def product(R, S):
        #     return [(t, u) for t in R for u in S]
        #
        # def select(R, s):
        #     return [t for t in R if s(t)]
        #
        # def project(R, p):
        #     return [p(t) for t in R]
        #
        # product_rent_garden = project(select(product(rent_info,gardeninfo), lambda t: t[0][0] == t[1][0]), lambda t: (t[0][0], t[0][1],t[1][1]) )
        # print("printing!!!!!")
        # print(product_rent_garden)




        #repo['alyu_sharontj_yuxiao_yzhang11.fire_count'].insert(fireCount)
        print("INNN garden_vs_rent")
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


        this_script = doc.agent('alg:alyu_sharontj_yuxiao_yzhang11#garden_vs_rent',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

        resource = doc.entity('dat:2013fireincident_anabos2',
                              {'prov:label': '311, Service Requests', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'json'})

        garden_input = doc.entity('dat:alyu_sharontj_yuxiao_yzhang11.garden',
                                  {prov.model.PROV_LABEL: 'garden',
                                   prov.model.PROV_TYPE: 'ont:DataSet'})

        rent_input = doc.entity('dat:alyu_sharontj_yuxiao_yzhang11.average_rent_zip',
                                  {prov.model.PROV_LABEL: 'garden',
                                   prov.model.PROV_TYPE: 'ont:DataSet'})

        this_run = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.usage(this_run, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval',})

        output =  doc.entity('dat:alyu_sharontj_yuxiao_yzhang11.garden_vs_rent', {prov.model.PROV_LABEL:'fire', prov.model.PROV_TYPE:'ont:DataSet'})

        # get_lost = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        #
        doc.wasAssociatedWith(this_run, this_script)
        doc.used(this_run, resource, startTime)
        doc.used(this_run, garden_input, startTime)
        doc.used(this_run, rent_input, startTime)

        doc.wasAttributedTo(output, this_script)
        doc.wasGeneratedBy(output, this_run, endTime)
        doc.wasDerivedFrom(output, resource, this_run, this_run, this_run)
        doc.wasDerivedFrom(output, garden_input, this_run, this_run, this_run)
        doc.wasDerivedFrom(output, rent_input, this_run, this_run, this_run)

        repo.logout()

        return doc


garden_vs_rent.execute()
doc = garden_vs_rent.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
