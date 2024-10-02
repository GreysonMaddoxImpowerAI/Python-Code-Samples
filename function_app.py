import logging
import azure.functions as func
import requests
import json
from azure.storage.blob import BlobServiceClient
from datetime import timedelta, datetime

app = func.FunctionApp()

@app.schedule(schedule="0 10 * * * *", arg_name="myTimer", run_on_startup=True,
              use_monitor=False) 
def timer_trigger(myTimer: func.TimerRequest) -> None:
    #Initialize the URL to call via api
    url = 'https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/2024-10-01?adjusted=true&apiKey=w3sWtD0Uj9RSy36asVnppVYlqpluw7lu'
    #Run a get request and conver the repsonse to json
    val = requests.get(url=url).json()['results']
    #Initialize the output filename
    output_file = 'data.json'
    #Write the JSON to a file
    with open(output_file, 'w') as file:
        json.dump(val, file, indent=4)

    current_datetime = datetime.now()
    previous_datetime = current_datetime - timedelta(days=1)
    #PUT YOUR NAME HERE
    YourName = 'Greyson'
    
    #Initialize a connection string and the name of the file you want to upload, along with the container you want to upload the file to
    connection_string = 'DefaultEndpointsProtocol=https;AccountName=gpmaddoxoutput;AccountKey=7AxyyDFFyFK+t7wpHYdije0i7WvMBIrdWRaKA/iYyVD/AN9ib5pSUs785ItqAVMA0ob/b9QI+R5r+ASt7YjaHw==;EndpointSuffix=core.windows.net'
    blob_name = f"{previous_datetime.strftime('%m%d%Y')}{YourName}Data.json"
    container_name = 'test-container'
    #Upload the file to the blob storage
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client(container_name)
    with open(output_file, 'rb') as data:
        container_client.upload_blob(blob_name, data)

    if myTimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function executed.')