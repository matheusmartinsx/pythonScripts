###########################
# Author: Matheus Martins #
###########################
import csv
import requests
import os
import requests.packages.urllib3
from time import sleep
from azure.core.exceptions import HttpResponseError, ResourceExistsError
from azure.storage.blob import BlobServiceClient
from azure.storage.blob import ContentSettings
from urllib.error import HTTPError
import socket
import urllib

requests.packages.urllib3.disable_warnings()

try:
    #Create target Directory
    os.mkdir('images')
    print("Directory 'images' Created ") 
except FileExistsError:
    print("Directory 'images' already exists")
    
#Get the main file path to script
my_path = os.getcwd()
my_path = my_path.replace('\\','/')
#//Receive and save the user parameters
my_acc = str(input('Insert you Azure account name: '))
my_container = str(input('Insert your container name: '))
connection_string = str(input('Paste your Azure Storage Connection String here: ')) #OR os.getenv('AZURE_STORAGE_CONNECTION_STRING')

#Start a new Blob
blob_service_client = BlobServiceClient.from_connection_string(connection_string)
#Start a new container
container_client = blob_service_client.get_container_client(my_container)
#Creates the container
try:
    container_client.create_container()
except ResourceExistsError:
    pass

#Saves your blob URL
urlPre = 'https://'+my_acc+'.blob.core.windows.net/'+my_container+'/'

def downloadImages(prefix='', stopRefeed=True):
    '''
    Function set do download images from a csv file arranged as 'name - link'
    If there is more than one link in a row, the function will add a '_X' at the end of the name.
    
    This function can also receive a prefix to put before the name that will be saved, and a Re Download protection
    where it will skip any link that is already in your Azure informed container.
    '''
    hdr = {'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36'}
    sourceFile = str(input('Insert your base csv file name (Without extension): '))

    #Carregar CSV em um lista // Load the CSV into a list
    with open(my_path +'/'+ sourceFile +'.csv','r', encoding="utf8") as c:
        rows = list(csv.reader(c))
    downloaded = 0
    
    for i in range(len(rows)):
        print('Downloading image '+ str(i+1)+' of '+ str(len(rows)))
        imgLinks = []
        codigo = rows[i][0]
        for j in range(len(rows[i])):
            if stopRefeed:
                if urlPre in rows[i][j]:
                    continue
            if 'http' in rows[i][j]:
                imgLinks.append(rows[i][j])
            else:
                continue
        cont = 0
        imagem = ''
        num = ''
        for l in range(len(imgLinks)):
            if cont > 0:
                num = '_' + str(cont)
            try:
                req = urllib.request.Request(imgLinks[l], headers=hdr)
                response = urllib.request.urlopen(req, timeout=5)
                file = open(f'images/{prefix}{codigo}{num}.jpg', 'wb')
                file.write(response.read())
                file.close()
            except UnicodeEncodeError:
                response = requests.get(imgLinks[l], timeout=5)
                file = open(f'images/{prefix}{codigo}{num}.jpg', 'wb')
                file.write(response.content)
                file.close()
            except HTTPError as err:
                with open('error.csv', 'a', newline='', encoding='UTF-8') as f:
                    writer = csv.writer(f)
                    writer.writerow([codigo,str(err), imgLinks[l]])
                    print(f'Error downloading image: {err}')
                continue
            except socket.timeout:
                with open('error.csv', 'a', newline='', encoding='UTF-8') as f:
                    writer = csv.writer(f)
                    writer.writerow([codigo,'Timeout Err', imgLinks[l]])
                    print(f'Error downloading image: Connection Timeout')
                continue
            downloaded += 1
            imagem += f'{prefix}{codigo}{num}.jpg, '
            sleep(0.5)
            cont += 1
    print(f'{downloaded} images succesfully downloaded.')
                
def uploadImages():
    myFiles = [f for f in os.listdir(my_path+ '/images') if os.path.isfile(os.path.join(my_path+'/images', f))]
    my_content = ContentSettings(content_type='image/jpeg')

    for i in range(len(myFiles)):
        # Upload de imagens para o container // Upload images to container
        with open('images/'+myFiles[i], "rb") as f:
            fileName = myFiles[i].split('.')[0]
            urlUploads = (urlPre + str(myFiles[i]))
            container_client.upload_blob(name=str(myFiles[i]), data=f, overwrite=True,content_settings= my_content)
            print('Image ['+ myFiles[i] +'] upload completed. - '+ str(i+1) + '/'+str(len(myFiles)))
            print('Image URL:   ['+urlUploads+']')
        
downloadImages()
uploadImages()