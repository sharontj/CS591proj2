import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import re
# from alyu_sharontj_yuxiao_yzhang11.Util.Util import *

class education_hubway(dml.Algorithm):
    contributor = 'alyu_sharontj_yuxiao_yzhang11'
    reads = ['alyu_sharontj_yuxiao_yzhang11.education, alyu_sharontj_yuxiao_yzhang11.hubway'] #read the data of roads and trafficsignals from mongo
    writes = ['alyu_sharontj_yuxiao_yzhang11.education_hubway']


    @staticmethod
    def execute(trial=False):
        startTime = datetime.datetime.now()

        '''Set up the database connection.'''
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('alyu_sharontj_yuxiao_yzhang11', 'alyu_sharontj_yuxiao_yzhang11') 

        '''get (schoolid,zipcode,latitude,longitute) from alyu_sharontj_yuxiao_yzhang11.education'''
        schoolinfo=[]
        edudb=repo['alyu_sharontj_yuxiao_yzhang11.education']
        cursor = edudb.find()  #filter not work
        for info in cursor:
            school_id = info['properties']['SchoolId']
            zipcode = info['properties']['Zipcode']
            Latitude = info['properties']['Latitude']
            Longitude = info['properties']['Longitude']
            schoolinfo.append((school_id, zipcode, Latitude, Longitude))


        #
        # validRoadName = select(roadName, lambda t: t[0] != 'NA') #filter unknown road names
        # #print(len(validRoadName)) #20233
        # #print(validRoadName)
        # # print(len(validRoadName))
        #
        # '''get (roadname,num_signals) from db.alyu_sharontj.TrafficSignals
        # '''
        # Sig_Roadname=[]
        # sigDb=repo['alyu_sharontj.TrafficSignals']
        # cursor = sigDb.find()
        # for info in cursor:
        #     #print(str(type(info['"FULLNAME"'])))
        #     fullname = info['properties']['Location']
        #     fullname = fullname.replace(".", "")
        #     names = re.split("&|,|@", fullname)
        #
        #     for n in names:
        #         newinfo = (n.strip(), 1)  #store data in the form (('BlueHillAve',1))
        #         # print(newinfo)
        #         Sig_Roadname.append(newinfo)
        #
        # validSigname = select(Sig_Roadname, lambda t: t[0] != '') #filter unknown road names
        # sig_num= aggregate(validSigname, sum)
        # # for i in Road_num:
        # #     print(i)
        #
        # '''combine (roadname,length) with (roadname, num_signals)
        #     expected output: (roadname, (length, num_signals, density))
        # '''
        # def reducer(k, vs):
        #     rlen = max([len for (len, num) in vs])
        #     snum = max([num for (len, num) in vs])
        #     if rlen==0:
        #         return k, (rlen, snum, 0)
        #     else:
        #         return k, (rlen, snum, float(snum)/float(rlen))
        # x = map(lambda k, v: [(k, (v, 0))], validRoadName)\
        #     +map(lambda k, v: [(k, (0, v))], sig_num)
        # result = reduce(reducer, x)
        #
        # # for i in result:
        # #     print(i)
        # # print(len(result))
        #
        # repo.dropCollection("TrafficSignal_Density")
        # repo.createCollection("TrafficSignal_Density")
        # for k,v in result:
        #     oneline={'RoadName': k, 'RoadLength': v[0], 'Signal_num': v[1], 'TrafficSignal_Density': v[2]}
        #     repo['alyu_sharontj.TrafficSignal_Density'].insert_one(oneline)


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
        repo.authenticate('alyu_sharontj', 'alyu_sharontj')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/alyu_sharontj') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/alyu_sharontj') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        # doc.add_namespace('bdp', 'http://bostonopendata-boston.opendata.arcgis.com/datasets/')
        # doc.add_namespace('hdv', 'https://dataverse.harvard.edu/dataset.xhtml')

        this_script = doc.agent('alg:alyu_sharontj#TrafficSignal_Density',
            { prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})


        road_input = doc.entity('dat:alyu_sharontj.Roads',
                                {prov.model.PROV_LABEL:'Roads',
                                 prov.model.PROV_TYPE:'ont:DataSet'})

        signal_input = doc.entity('dat:alyu_sharontj.TrafficSignals',
                                  {prov.model.PROV_LABEL:'Traffic Signals',
                                   prov.model.PROV_TYPE:'ont:DataSet'})

        this_run = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime)#, 'ont:Query':'?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'})


        output = doc.entity('dat:alyu_sharontj.TrafficSignal_Density',
            { prov.model.PROV_LABEL:'TrafficSignal_Density', prov.model.PROV_TYPE: 'ont:DataSet'})


        doc.wasAssociatedWith(this_run, this_script)
        doc.used(this_run, road_input, startTime)
        doc.used(this_run, signal_input, startTime)

        doc.wasAttributedTo(output, this_script)
        doc.wasGeneratedBy(output, this_run, endTime)
        doc.wasDerivedFrom(output, road_input, this_run, this_run, this_run)
        doc.wasDerivedFrom(output, signal_input, this_run, this_run, this_run)
        repo.logout()


        return doc



TrafficSignal_Density.execute()
# doc = TrafficSignal_Density.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
