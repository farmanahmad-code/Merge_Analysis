import requests
import pandas as pd
import json
from elasticsearch import Elasticsearch
from datetime import datetime
import os
import logging
import yaml
import logging
import math
import random
today= datetime.strftime(datetime.now(), '%Y-%m-%d')
logging.basicConfig(filename='logs/'+today+'merge_log.log',format='%(asctime)s-%(levelname)s ========> %(message)s',level=logging.INFO)
config=yaml.safe_load(open("config.yml"))
es = Elasticsearch(hosts=config['es']['es_url'], port=9200, timeout=30)
def input_empi():
    while True:
        empi_list = input("Enter empis seperated by comma : ")
        empi_list = empi_list.split(",")
        empi_list =[e.strip() for e in empi_list]
        empi_list=list(filter(None,empi_list))
        empi_list=["{}".format(e) for e in empi_list]
        if len(empi_list)>=2:
            exist_flag=True
            for empi in empi_list:
                exist_flag=empi_exists_in_pd_empi(empi)
            if exist_flag==True:
                print("Empis entered are valid")
                logging.info("Empis entered are : "+str(empi_list))
                return empi_list
            else:
                print("All empis must exist in pd_empi")
                print("Try again")
        else:
            print("Enter atleast two valid empis")
            print("Try Again")
def empi_exists_in_pd_empi(empi):
    exist_flag=False
    query="""{{
    "query": {{
        "bool": {{
            "must": {{
                "match": {{
                    "empi": {{
                        "query":"{0}",
                        "type": "phrase"
                    }}
                }}
            }}
        }}
    }}
}}""".format(empi)
    response=es.count(body=query,index=config['es']['empi_index'])
    if response['count']>0:
        exist_flag=True
    return exist_flag

def elasticToDataframe(elasticResult,aggStructure,record={},fulllist=None):
    if fulllist==None:
        fulllist=[]
    for agg in aggStructure:
        buckets = elasticResult[agg['key']]['buckets']
        for bucket in buckets:
            record = record.copy()
            record[agg['key']] = bucket['key']
            if 'aggs' in agg:
                elasticToDataframe(bucket,agg['aggs'],record,fulllist)
            else:
                for var in agg['variables']:
                    record[var['dfName']] = bucket[var['elasticName']]
                fulllist.append(record)
    df = pd.DataFrame(fulllist)
    return df

def read_from_pd_empi(empi_list):
    empi_str=','.join(empi_list)
    query = "select * from "+config['es']['empi_index']+" where empi in ("+empi_str+") group by empi,fn,ln,gn,dob"
    elastic__source = "http://"+config['es']['es_url']+":9200/_sql"
    aggStructure=[{
"key":"empi","aggs":[{
"key":"fn","aggs":[{
"key":"ln","aggs":[{
"key":"gn","aggs":[{
"key":"dob",'variables':[{'elasticName':'doc_count','dfName':'count'}]}]}]}]}]}]
    es_res = requests.post(elastic__source, data=query)
    if es_res.status_code == 200:
        response = es_res.json()
        response = response['aggregations']
        output=elasticToDataframe(response,aggStructure)
        output=output.drop(['count'],axis=1)
        output.index+=1
        output['dob']=output['dob'].apply(lambda x :datetime.utcfromtimestamp(int(x)/1000).strftime('%Y-%m-%d'))
        output=output[['empi','fn','ln','gn','dob']]
        logging.info(output)
        print(output)
    return output


def merge_case_analysis(empi_list):
    gender_flag=False
    dob_flag=False
    fn_flag=False
    ln_flag=False
    merge_flag= False
    df = read_from_pd_empi(empi_list)
    total_rows = len(df.index)
    if df.gn.nunique() == 1:
        gender_flag=True
        print("Gender is same for all empis")
        logging.info("Gender is same for all empis")

    else:
        print("Gender is not same for all empis")
        logging.info("Gender is not same for all empis")

    if df.dob.nunique() == 1:
        dob_flag=True
        print("DOB is same for all empis")
        logging.info("DOB is same for all empis")
    else:
        print("DOB is not same for all empis")
        logging.info("DOB is not same for all empis")

    if df.fn.nunique() == 1:
        fn_flag=True
        print("First name is same for all empis")
        logging.info("First name is same for all empis")

    else:
        print("First name is not same for all empis")
        logging.info("First name is not same for all empis")

    if df.ln.nunique() == 1:
        ln_flag=True
        print("Last name is same for all empis")
        logging.info("Last name is same for all empis")

    else:
        print("Last name is not same for all empis")
        logging.info("Last name is not same for all empis")

    if gender_flag == True and dob_flag == True and fn_flag == True and ln_flag==True:
        print ("Merge is recommended")
        logging.info("Merge is recommended")
        merge_flag=True
    elif gender_flag == True and dob_flag== True:
        print ("Merge is possible")
        logging.info("Merge is possible")
        merge_flag=True
    else:
        print("For merging gender and dob must be same")
        logging.info("For merging gender and dob must be same")
    return gender_flag,dob_flag,fn_flag,ln_flag,merge_flag,df


def update_indexes_in_elastic(empi_list,field,value):
    index_list = [(config['es']['activity_index'],'activity'),(config['es']['empi_index'],'empi'),(config['es']['attribution_index'],'attribution')]
    for empi in empi_list:
        query="""{{
    "query": {{
        "bool": {{
            "must": {{
                "match": {{
                    "empi": {{
                        "query":"{0}",
                        "type": "phrase"
                    }}
                }}
            }}
        }}
    }}
,
    "script" : {{
      "inline" : "ctx._source.{1}='{2}';"
}}
}}"""
        query=query.format(empi,field,value)
        for index in index_list:
            response=es.update_by_query(body=query,  index=index[0], conflicts = 'proceed',refresh='wait_for')
            logging.info("Updating index : "+index[0])
            logging.info("empi : "+empi+"\tvalue : "+value+"\tfield :"+field)
            logging.info(response)
            print("Updating index : "+index[0])
            print("empi : "+empi+"\tvalue : "+value+"\tfield :"+field+"\ttotal_records :"+str(response['total'])+"\tupdated :"+str(response['updated']))
def encrypt_value(decrypted_value):
  '''
  :param decrypted_value: plain text

  Encryption logic --

   1. Generate random salt
   2. Convert each character to ASCII
   3. Add salt to each ASCII value of each character
   4. Convert to hex
   5. Join with ':'

  :return: hexa decimal values separated by ':', with last value as salt
  '''
  random_number = math.floor(random.uniform(0,1) * 89)+10
  x =[]
  for each_char in decrypted_value:
    char_to_ascii = ord(each_char)
    salted_ascii = char_to_ascii + int(random_number)
    ascii_to_hex = hex(salted_ascii)[2:]
    x.append(ascii_to_hex)
  x.append(int(random_number))
  final_value = ':'.join(str(y) for y in x)
  return final_value

def encrypted_json(decrypted_json):
  final_json = {}
  for each_key in decrypted_json:
    final_json.update({
      encrypt_value(each_key): encrypt_value(decrypted_json[each_key])
    })
  return final_json
def insert_json_in_mongo(empi_list):
    f=open("empi-merge-pipe-seperated.csv","w+")
    f.write("old_empis\n")
    for i in range(len(empi_list)):
        if i==len(empi_list)-1:
            f.write(empi_list[i])
        else:
            f.write(empi_list[i]+"|")
    f.close()
    API_URL="https://{}/analytics/empi/merge/create".format(config['merge-api']['api_base_url'])
    file_path = "empi-merge-pipe-seperated.csv"
    user_email = config['merge-api']['username']
    user_password = config['merge-api']['password']
    body = {
        "email": user_email,
        "password": user_password
    }
    encrypt_body = encrypted_json(body)
    encrypt_json = json.dumps(encrypt_body)
    AUTH_URL="https://{}/user/auth".format(config['merge-api']['auth_base_url'])
    headers = {"Content-Type":"application/json"}
    response = requests.post(AUTH_URL, data=encrypt_json, headers=headers)
    response_string = response.text
    
    response_json = json.loads(response_string)
    data_json = response_json['data']

    token = data_json['token']

    auth_token = "Authorization " + token

    files = {'data': open(file_path, 'rb')}
    headers = {'Authorization': auth_token}
    r = requests.post(API_URL, files=files, headers=headers)
    r = r.json() 
    
    if r['success']==False:
        raise Exception('JSON not inserted in mongo reason : '+str(r['error']))
    
def update_gender(empi_list):
    print("Since gender is different hence empis cannot be merged")
    if input("Enter yes to update gender : ").lower()=='yes':
        os.system('clear')
        while True:
            updated_gender = input("Enter correct gender : ").lower()
            if updated_gender == 'm' or updated_gender == 'f':
                update_indexes_in_elastic(empi_list,"gn",updated_gender)
                logging.info("Gender is updated to "+updated_gender)
                print("Gender is updated to " +updated_gender)
                return 1
            else:
                print("Gender should be either m or f")
def update_dob(empi_list,options):
    print("Since dob is different hence empis cannot be merged")
    if input("Enter yes to update dob : ").lower()=='yes':
        os.system('clear')
        while True :
            print("DOB should be one of the following : ",options)
            updated_dob = input('Enter correct dob : ')
            
            if updated_dob in options:
                try :
                    updated_dob = datetime.strptime(updated_dob, "%Y-%m-%d").date()
                    update_indexes_in_elastic(empi_list,"dob",str(updated_dob))
                    logging.info("DOB is updated to "+str(updated_dob))
                    print("DOB is updated to "+str(updated_dob))
                    return 1
                except ValueError as e:
                    print(str(e))
                    print("Error: must be format yyyy-mm-dd ")
                    print("Try again")
            else:
                print("Invalid Date")
                print("Try again")
def update_first_name(empi_list,options):
    options = [x.lower() for x in options]
    print("First name is different but still empis can be merged")
    if input("Enter yes if you want to update first name : ").lower()=='yes':
        os.system('clear')
        while True:
            print("First name should be one of the following : ",options)
            updated_fn = input("Enter correct first name : ").lower().strip()
            if updated_fn in options:
                update_indexes_in_elastic(empi_list,"fn",updated_fn)
                logging.info("First name is updated to " +updated_fn)
                print("First name is updated to" +updated_fn)
                return 1
            else:
                print("Invalid fn entered")
                print("Try again")
def update_last_name(empi_list,options):
    options = [x.lower() for x in options]
    print("Last name is different but still empis can be merged")
    if input("Enter yes if you want to update last name : ").lower()=='yes':
        os.system('clear')
        while True:
            print("Last name should be one of the following : ",options)
            updated_ln = input("Enter correct last name : ").lower().strip()
            if updated_ln in options:
                update_indexes_in_elastic(empi_list,"ln",updated_ln)
                print("Last name is updated to " +updated_ln)
                logging.info(("Last name is updated to" +updated_ln))
                return 1
            else:
                print("Invalid ln entered")
                print("Try again")

def run_merge_split_job():
    requests.packages.urllib3.disable_warnings()
    str_url = config['azkaban']['az_url']
    postdata = {'username':config['azkaban']['username'],'password':config['azkaban']['password']}
    login_url = str_url + '?action=login'
    response = requests.post(login_url, postdata, verify=False).json()
    postdata = {'session.id': response['session.id'], 'ajax': 'executeFlow', 'project': 'Merge_Split', 'flow': 'empi-merge'}
    fetch_url = str_url + '/executor?ajax=executeFlow'
    r = requests.get(fetch_url, postdata).json()
    print(r)

def main():
    logging.info('=========================================Starting Script==========================================================')
    while True:
        os.system('clear')
        merge_split_job_flag=False
        empi_list=input_empi()
        gender_flag,dob_flag,fn_flag,ln_flag,merge_flag,df=merge_case_analysis(empi_list)
        if input("Enter yes if you want to continue with merging : ").lower()=="yes":
            os.system('clear')
            logging.info("User chose to continue with merging")
            if gender_flag == False:
                update_gender(empi_list)
            if dob_flag == False:
                update_dob(empi_list,list(df['dob'].unique()))
            if fn_flag == False:
                update_first_name(empi_list,list(df['fn'].unique()))
            if ln_flag == False:
                update_last_name(empi_list,list(df['ln'].unique()))
            gender_flag,dob_flag,fn_flag,ln_flag,merge_flag,df=merge_case_analysis(empi_list)
            if merge_flag==True:
                insert_json_in_mongo(empi_list)
                logging.info("Json is inserted in mongo")
                print("Json is inserted in mongo")
                merge_split_job_flag=True
        print("------------------------------------------------------------------------------------")
        logging.info("------------------------------------------------------------------------------------")
        if input("Enter yes if you want to merge more EMPIs : ").lower()=="yes":
            logging.info("User chose to merge more empis")
            os.system('clear')
            continue
        else:
            if merge_split_job_flag == True:
                if input("Enter yes if you want to run merge split job : ").lower()=="yes":
                    run_merge_split_job()
                    print("Merge split job is executed on azkaban")
                    logging.info("Merge split job is executed on azkaban")
                    
                else:
                    print("JSON inserted in mongo but merge job not executed")
                    logging.info("JSON inserted in mongo but merge job not executed")
            print("Session ended")
            logging.info('=========================================Session ended==========================================================')
                
            exit()

try:
    main()
except KeyboardInterrupt:
    print('\nbye')
    logging.info("User exited using keyboard interrupt")
except Exception as e:
    logging.info(str(e))
    print(str(e))
