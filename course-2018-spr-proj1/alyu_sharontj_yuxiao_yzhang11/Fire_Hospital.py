import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import csv
# from alyu_sharontj_yuxiao_yzhang11.Util.Util import *



class Fire_Hospital(dml.Algorithm):
    contributor = 'alyu_sharontj_yuxiao_yzhang11'
    reads = ['alyu_sharontj_yuxiao_yzhang11.fire_count', 'alyu_sharontj_yuxiao_yzhang11.hospital']
    writes = ['alyu_sharontj_yuxiao_yzhang11.Fire_Hospital']

    @staticmethod
    def execute(trial=True):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('alyu_sharontj_yuxiao_yzhang11', 'alyu_sharontj_yuxiao_yzhang11')

        def union(R, S):
            return R + S

        def difference(R, S):
            return [t for t in R if t not in S]

        def intersect(R, S):
            return [t for t in R if t in S]

        def project(R, p):
            return [p(t) for t in R]

        def select(R, s):
            return [t for t in R if s(t)]

        def product(R, S):
            return [(t, u) for t in R for u in S]

        def aggregate(R, f):
            keys = {r[0] for r in R}
            return [(key, f([v for (k, v) in R if k == key])) for key in keys]

        def map(f, R):
            return [t for (k, v) in R for t in f(k, v)]

        def reduce(f, R):
            keys = {k for (k, v) in R}
            return [f(k1, [v for (k2, v) in R if k1 == k2]) for k1 in keys]


        '''get hospital_count = (zipcode,hospital_count) from db.alyu_sharontj_yuxiao_yzhang11.hospital'''

        hospital_count=[]
        zip = []
        hospitalDB=repo['alyu_sharontj_yuxiao_yzhang11.hospital']
        cursor = hospitalDB.find()
        for info in cursor:
            street=info['street']
            tmp = street.split(",")[2].split(" ")[2]
            zip.append(tmp)

        hospital_count = aggregate(project(zip,lambda t: (t,1)),sum)

        # print(hospital_count)


        '''get fire_count = (zipcode,fire_count) from db.alyu_sharontj_yuxiao_yzhang11.fire_count'''

        fire_count=[]
        zip = []
        firecountDB=repo['alyu_sharontj_yuxiao_yzhang11.fire_count']
        cursor = firecountDB.find()
        for info in cursor:
            tmp = (info['_id'], info['count']/3)
            fire_count.append(tmp)

        # print(fire_count)

        '''combine hospital_count = (zipcode,hospital_count)  with fire_count = (zipcode,fire_count)
                   expected output: (zipcode, fire_count / hospital_count)
               '''

        zipcode_list=["02110","02210","02132","02109","02199","02108","02113", "02116","02163","02136","02111","02129", "02114", \
                      "02131", "02118", "02130", "02127", "02135", "02126", "02125", "02215", "02134", "02122", "02128", "02115", "02124", "02120", "02119", "02121"]


        fire_hospital_1 = project(select(product(fire_count,hospital_count), lambda t: t[0][0] == t[1][0]), lambda t: (t[0][0], t[0][1]/t[1][1]))
        fire_hospital_2 =[]

        fire_zips = project(fire_count, lambda t: t[0])
        hospital_zips = project(hospital_count, lambda t: t[0])
        differ1 = difference(fire_zips,hospital_zips)
        # differ2 = difference(hospital_zips,fire_zips)
        # print(differ1)
        # print(differ2)

        for i in differ1:
            tmp = (i,3000)
            fire_hospital_2.append(tmp)


        # print(fire_hospital_1)
        #
        # print(fire_hospital_2)

        fire_hospital = union(fire_hospital_1,fire_hospital_2)

        # print(fire_hospital)


        repo.dropCollection("Fire_Hospital")
        repo.createCollection("Fire_Hospital")
        for k,v in fire_hospital:
            oneline={'Zipcode': k, 'fire/hospital': v }
            repo['alyu_sharontj_yuxiao_yzhang11.Fire_Hospital'].insert_one(oneline)



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


        this_script = doc.agent('alg:alyu_sharontj_yuxiao_yzhang11#Fire_Hospital',
            { prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})


        hospital_input = doc.entity('dat:alyu_sharontj_yuxiao_yzhang11.hospital',
                                {prov.model.PROV_LABEL:'hospital',
                                 prov.model.PROV_TYPE:'ont:DataSet'})

        fire_count_input = doc.entity('dat:alyu_sharontj_yuxiao_yzhang11.fire_count',
                                  {prov.model.PROV_LABEL:'fire_count',
                                   prov.model.PROV_TYPE:'ont:DataSet'})

        this_run = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime)#, 'ont:Query':'?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'})


        output = doc.entity('dat:alyu_sharontj_yuxiao_yzhang11.Fire_Hospital',
            { prov.model.PROV_LABEL:'Fire_Hospital', prov.model.PROV_TYPE: 'ont:DataSet'})


        doc.wasAssociatedWith(this_run, this_script)
        doc.used(this_run, hospital_input, startTime)
        doc.used(this_run, fire_count_input, startTime)

        doc.wasAttributedTo(output, this_script)
        doc.wasGeneratedBy(output, this_run, endTime)
        doc.wasDerivedFrom(output, hospital_input, this_run, this_run, this_run)
        doc.wasDerivedFrom(output, fire_count_input, this_run, this_run, this_run)
        repo.logout()


        return doc




# Fire_Hospital.execute()
# doc = Fire_Hospital.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
