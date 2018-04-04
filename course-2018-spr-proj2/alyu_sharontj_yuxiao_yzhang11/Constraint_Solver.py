import z3
import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import csv
import numpy
import statistics as stats

# from alyu_sharontj_yuxiao_yzhang11.Util.Util import *



class Constraint_Solver(dml.Algorithm):
    contributor = 'alyu_sharontj_yuxiao_yzhang11'
    reads = ['alyu_sharontj_yuxiao_yzhang11.garden', 'alyu_sharontj_yuxiao_yzhang11.education',\
             'alyu_sharontj_yuxiao_yzhang11.Fire_Hospital_vs_Rent','alyu_sharontj_yuxiao_yzhang11.average_rent_zip']
    writes = ['alyu_sharontj_yuxiao_yzhang11.Result']

    @staticmethod
    def execute(trial=False):
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


        '''get rent = (zipcode,rent) from db.alyu_sharontj_yuxiao_yzhang11.average_rent_zip'''

        rentinfo = []
        rentdb = repo['alyu_sharontj_yuxiao_yzhang11.average_rent_zip']
        rentcur = rentdb.find()
        for info in rentcur:
            zipcode= info['Zip']
            rent = info['Average']
            rentinfo.append((zipcode, rent))
        rentdict = dict(rentinfo)
        # print("rent info:"+str(rentinfo))


        '''get number of schools = (zipcode,education_count) from db.alyu_sharontj_yuxiao_yzhang11.education_rent'''

        schoolinfo = []
        edudb = repo['alyu_sharontj_yuxiao_yzhang11.education']
        educur = edudb.find()
        for info in educur:
            edu_id= info['properties']['SchoolId']
            if (edu_id != "0"):
                address = info['properties']['Address']
                edu_zip = address[-5:]
                schoolinfo.append((edu_zip, 1))
        eduinfo= aggregate(schoolinfo, sum)
        edudict = dict(eduinfo)



        '''get fire_hospital = (zipcode,Fire_Hospital_vs_Rent) from db.alyu_sharontj_yuxiao_yzhang11.Fire_Hospital_vs_Rent'''
        # print("\n\n")
        fireinfo = []
        fire_hos_db = repo['alyu_sharontj_yuxiao_yzhang11.Fire_Hospital_vs_Rent']
        fire_hos_cur = fire_hos_db.find()
        for info in fire_hos_cur:
            zipcode = info['Zipcode']
            fire_hos_rate = info['fire/hospital']
            # print(str(zipcode)+","+ str(fire_hos_rate))
            fireinfo.append((zipcode, fire_hos_rate))
        firedict = dict(fireinfo)


        '''get number of garden = (zipcode,garden_count) from db.alyu_sharontj_yuxiao_yzhang11.garden_vs_rent'''

        gardeninfo = []
        gardendb = repo['alyu_sharontj_yuxiao_yzhang11.garden_vs_rent']
        gardencur = gardendb.find()
        for info in gardencur:
            zipcode = info['Zip']
            garden_count = info['garden_count']
            # print(str(zipcode)+","+ str(garden_count))
            gardeninfo.append((zipcode,garden_count))
        gardendict = dict(gardeninfo)

        '''find mean, std of each list'''
        def get_boundary(info):
            values = info.values()
            sum = 0
            counter = 0
            value_list = list(values)
            mean = stats.mean(value_list)
            std = stats.stdev(value_list)
            low = mean-3*std
            high = mean + 3*std
            return low, high




        zipcode_list=["02110","02210","02132","02109","02199","02108","02113", "02116","02163","02136","02111","02129", "02114", \
                      "02131", "02118", "02130", "02127", "02135", "02126", "02125", "02215", "02134", "02122", "02128", "02115",\
                      "02124", "02120", "02119", "02121"]

        scorelist = []
        weight= {"rent": 0.5, "edu": 0.3,"fire": 0.1, "garden": 0.1}


        def getscore(z, dict,factor):
            if(z in dict.keys()):
                low,high = get_boundary(dict)

                if(dict[z] <= high and  dict[z] >= low):
                    score2 = dict[z]*weight[factor]
                else:
                    score2 = 0
            else:
                score2 = 0
            return score2



        for zipcode in zipcode_list:
            rentscore = getscore(zipcode, rentdict,'rent')
            eduscore = getscore(zipcode, edudict, 'edu')
            firescore = getscore(zipcode, firedict,'fire')
            gardenscore = getscore(zipcode, gardendict, 'garden')

            score = rentscore + firescore + eduscore + gardenscore

            scorelist.append((zipcode, score))



        # def Takesecond(elem):
        #     return elem[1]


        results = sorted(scorelist,key=lambda x: x[1],reverse=True)
        # print("result is ")
        # print(results)
        #results = scorelist.sort(key=Takesecond, reverse=True)

        repo.dropCollection("Result")
        repo.createCollection("Result")

        for k,v in results:
            oneline={'Zipcode': k, 'score': v}
            print(oneline)
            repo['alyu_sharontj_yuxiao_yzhang11.Result'].insert_one(oneline)


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


        this_script = doc.agent('alg:alyu_sharontj_yuxiao_yzhang11#Constraint_Solver',
            { prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

# ['alyu_sharontj_yuxiao_yzhang11.garden', 'alyu_sharontj_yuxiao_yzhang11.education',\
#              'alyu_sharontj_yuxiao_yzhang11.Fire_Hospital_vs_Rent','alyu_sharontj_yuxiao_yzhang11.average_rent_zip']
        rent_input = doc.entity('dat:alyu_sharontj_yuxiao_yzhang11.average_rent_zip',
                                {prov.model.PROV_LABEL:'average_rent_zip',
                                 prov.model.PROV_TYPE:'ont:DataSet'})

        garden_input = doc.entity('dat:alyu_sharontj_yuxiao_yzhang11.garden',
                                  {prov.model.PROV_LABEL:'garden',
                                   prov.model.PROV_TYPE:'ont:DataSet'})

        education_input = doc.entity('dat:alyu_sharontj_yuxiao_yzhang11.education',
                                  {prov.model.PROV_LABEL:'education',
                                   prov.model.PROV_TYPE:'ont:DataSet'})

        firehospital_input = doc.entity('dat:alyu_sharontj_yuxiao_yzhang11.Fire_Hospital_vs_Rent',
                                  {prov.model.PROV_LABEL:'firehospital_input',
                                   prov.model.PROV_TYPE:'ont:DataSet'})

        this_run = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime)#, 'ont:Query':'?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'})


        output = doc.entity('dat:alyu_sharontj_yuxiao_yzhang11.Fire_Hospital_vs_Rent',
            { prov.model.PROV_LABEL:'Fire_Hospital_vs_Rent', prov.model.PROV_TYPE: 'ont:DataSet'})


        doc.wasAssociatedWith(this_run, this_script)
        doc.used(this_run, garden_input, startTime)
        doc.used(this_run, education_input, startTime)
        doc.used(this_run, rent_input, startTime)
        doc.used(this_run, firehospital_input, startTime)

        doc.wasAttributedTo(output, this_script)
        doc.wasGeneratedBy(output, this_run, endTime)
        doc.wasDerivedFrom(output, garden_input, this_run, this_run, this_run)
        doc.wasDerivedFrom(output, education_input, this_run, this_run, this_run)
        doc.wasDerivedFrom(output, rent_input, this_run, this_run, this_run)
        doc.wasDerivedFrom(output, firehospital_input, this_run, this_run, this_run)
        repo.logout()


        return doc




Constraint_Solver.execute()
doc = Constraint_Solver.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
