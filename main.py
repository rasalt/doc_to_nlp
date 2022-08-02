from google.cloud import documentai_v1beta3 as documentai
from google.cloud import storage
import json
project_id= 'smede-276406'
processor_id = 'c2756cced20720bb'
location = 'us' # Format is 'us' or 'eu'
bucketnamesrc = "smede-276406-dai-input"
bucketnamedst = "smede-276406-nlpinput"

file = "doc1.pdf"

import json
import google.auth

from google.auth.transport import requests
from google.oauth2 import service_account

from google.cloud import bigquery
from google.api_core import retry
import json
import logging
import nlp2fhir as nf

def hcnlp(inputtext):
    _PROJECT_ID = "smede-276406"
    _BASE_URL = "https://healthcare.googleapis.com/v1beta1/"
    _REGION = 'us-central1'
    URL='https://healthcare.googleapis.com/v1beta1/projects/smede-276406/locations/us-central1/services/nlp:analyzeEntities'

    from google.auth import compute_engine
    from google.cloud import storage
    import json
    credentials = compute_engine.Credentials()
    response = {}
    from google.auth.transport.requests import AuthorizedSession
    authed_session = AuthorizedSession(credentials)
    # Read the clinical notes from a GCS bucket object

    print("Unstructured input text blurb - Social History Example")
    print("------------------------------------------------------")
    print(inputtext)

    url = URL
    payload = {
        'nlp_service': NLP_SERVICE,
        'document_content': inputtext
    }
    res = authed_session.post(url, data=payload)
    response = res.json()
    print("Structured healthcare NLP Output")
    print("-----------------------------------")
    print(json.dumps(response,indent=4))
    return response

NLP_SERVICE='projects/smede-276406/locations/us-central1/services/nlp'
class BigQueryError(Exception):
    '''Exception raised whenever a BigQuery error happened'''

    def __init__(self, errors):
        super().__init__(self._format(errors))
        self.errors = errors

    def _format(self, errors):
        err = []
        for error in errors:
            err.extend(error['errors'])
        return json.dumps(err)

# [START healthcare_get_session]
def get_session():

    """Creates an authorized Requests Session."""
    session = requests.AuthorizedSession(_CREDENTIALS)

    return session
# [END healthcare_get_session]

# Note: In this example, the table for persisting the Healthcare NLP API was extended with
# an additional column to include the original raw text as well as the API extracted entities
def persist_nlp_in_bq(extracted_entities,raw_medical_text):
    """Stores both raw text and extracted entities in BigQuery"""
    _BQ_DATASET = 'himss'
    _BQ_NLP_TABLE = 'nlp'
    _BQ = bigquery.Client()
    row = json.loads(extracted_entities)
    row['rawText']=raw_medical_text

    table = _BQ.dataset(_BQ_DATASET).table(_BQ_NLP_TABLE)
    errors = _BQ.insert_rows_json(table,
                                json_rows=[row],
                                retry=retry.Retry(deadline=30))

    if errors != []:
        raise BigQueryError(errors)
    else:
       print("Successfully inserted NLP response into BigQuery")

def process_document(data, context):
    
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(data['bucket'])
    blob = bucket.blob(data['name'])
    file = data['name']
    print()
    import os

    opts = {}

    if location == "eu":
        opts = {"api_endpoint": "eu-documentai.googleapis.com"}

    client = documentai.DocumentProcessorServiceClient(client_options=opts)

    #client = documentai.DocumentProcessorServiceClient.from_service_account_json('key.json')
    #    client = documentai.DocumentProcessorServiceClient.f
    #client = documentai.DocumentProcessorServiceClient()

    # The full resource name of the processor, e.g.:
    # projects/project-id/locations/location/processor/processor-id
    # You must create new processors in the Cloud Console first
    name = f"projects/{project_id}/locations/{location}/processors/{processor_id}"
    
    bucket = storage_client.get_bucket(bucketnamesrc)

    #blob = bucket.blob(file)
    contents = blob.download_as_string()
    
    # Read the file into memory
    document = {"content": contents, "mime_type": "application/pdf"}

    # Configure the process request
    request = {"name": name, "document": document}
    # Use the Document AI client to process the sample form
    result = client.process_document(request=request)
    jsonstrdoc = documentai.Document.to_json(result.document)

    jsondoc = json.loads(jsonstrdoc)
    jsonlines = jsondoc['text']

    lines = jsonlines.splitlines()
    index = 0
    for line in lines:
        #print("-------------")
        #print(line)
        if line in ["PATIENT NAME", "PATIENT NAME:"]:
            patientname =  lines[index + 1 ]
        if line in ["CHART NUMBER", "CHART NUMBER:"]:
            chartnum = lines[index + 1]
        if line in ["DATE", "DATE:"]:
            date = lines[index + 1]
        if line in ["CASE", "CASE:"]:
            case = lines[index + 1]
        if line in ["NOTES", "NOTES:"]:
            notes = " ". join(lines[index+1 : len(lines)])
            break; 
        index = index + 1    
    print("Document processin complete.")
    
    # Write a json blob to another bucket
    bucketdst = storage_client.get_bucket(bucketnamedst)
    
    nlpinput = {"patientname": patientname, "chartnum": chartnum, "case": case, "date": date, "clinicalnotes": notes}


    blob = bucketdst.blob(file+".json")
    # take the upload outside of the for-loop otherwise you keep overwriting the whole file
    
    #Persist the Clinical Notes to a file
    blob.upload_from_string(data=json.dumps(nlpinput),content_type='application/json')
 
    #Obtain the HC NLP output
    nlp_output = hcnlp(json.dumps(nlpinput["clinicalnotes"]))

    #Persist NLP output to BQ
    persist_nlp_in_bq(json.dumps(nlp_output),nlpinput["clinicalnotes"])

    #NLP to fhir
    print("----------------NLP To FHIR ----------------------------------------------------------------")
    devConfig = {'DeviceDefinition': '9c1c9ebd-b400-4625-8d69-31a7b97c8f4d', 'Device': '017dd3a8-26bc-4b0b-a4b8-2783382cf960'}
    print("--------------------------------------------------------------------------------")
    logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)

    PROCESS_MEDICATIONS = False #Tests on 04-18
    CREATE_FRAMEWORK = False #Tests on 04-18
    PROCESS_PROBLEMS = True #Tests on 04-18
    PROCESS_PROCEDURES = True
    RISK_ASSESSMENT = True
    if PROCESS_PROBLEMS:
        problems = nf.getEntities(nlp_output, entitytype = "PROBLEM", subjectlist = ['PATIENT'],
                                        certaintylist = ["LIKELY", "SOMEWHAT_LIKELY"],
                                        temporallist = ['CLINICAL_HISTORY', "CURRENT"])
        logging.info("--------------------------------------------------------------------------------")
        print(problems)
        logging.info("--------------------------------------------------------------------------------")
        for p in problems:
            logging.info("This problem is {}".format(p))
            nf.fhir_conditions(p, devConfig)
             
    if PROCESS_MEDICATIONS:
        medicines = nf.getEntities(nlp_output, entitytype = "MEDICINE", subjectlist = ['PATIENT'],
                                certaintylist = ["LIKELY", "SOMEWHAT_LIKELY"],
                                temporallist = ['CLINICAL_HISTORY', "CURRENT"],
                                include_up = True)
        logging.info("--------------------------------------------------------------------------------")
        print(medicines)
        logging.info("--------------------------------------------------------------------------------")
        for m in medicines:
            logging.info("###############################################################################")        
            logging.info("This medicine is {}".format(m))
            nf.fhir_currentmedications(m, devConfig)
        
    if PROCESS_PROCEDURES:
        procedures = nf.getEntities(nlp_output, entitytype = "PROCEDURE", subjectlist = ['PATIENT'],
                                certaintylist = ["LIKELY", "SOMEWHAT_LIKELY"],
                                temporallist = ['CLINICAL_HISTORY', "CURRENT"],
                                include_up = True)
        logging.info("--------------------------------------------------------------------------------")
        print(procedures)
        logging.info("--------------------------------------------------------------------------------")    
        for p in procedures:
            logging.info("###############################################################################")
            logging.info("Procedure is {}".format(p))
            nf.fhir_procedure(p, devConfig)
        
    
    if RISK_ASSESSMENT:
        riskassessments = nf.getEntities(nlp_output, entitytype = "PROBLEM", subjectlist = ['PATIENT'],
                                        certaintylist = ["LIKELY", "SOMEWHAT_LIKELY"],
                                        temporallist = ['UPCOMING'])
        logging.info("--------------------------------------------------------------------------------")
        print(riskassessments)
        for r in riskassessments:
            logging.info("--------------------------------------------------------------------------------")    
            print("This risk is \n{}".format(r))
            nf.fhir_riskcondition(r, devConfig)     
        return
