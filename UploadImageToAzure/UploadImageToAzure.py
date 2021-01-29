###########################
# Author: Matheus Martins #
###########################
import csv
import requests
import shutil
import os
import requests.packages.urllib3
import time
from azure.core.exceptions import HttpResponseError, ResourceExistsError
from azure.storage.blob import BlobServiceClient

requests.packages.urllib3.disable_warnings()

try:
    # Create target Directory
    os.mkdir('images')
    print("Directory 'images' Created ") 
except FileExistsError:
    print("Directory 'images' already exists")
    
#Adquire o caminho do script//Get the main file path to script
my_path = os.getcwd()
my_path = my_path.replace('\\','/')
#Recebe e armazena os parametros//Receive and save the user parameters
sourceFile = str(input('Insert your base csv file name (Without extension): '))
my_acc = str(input('Insert you Azure account name: '))
my_container = str(input('Insert your container name: '))
my_path.replace("\\","/")
linkIndex = int(input('Insert the link index of the CSV File (Starting at 0): '))
nameIndex = int(input('Insert the name index of the CSV File: '))

connection_string = str(input('Paste your Azure Storage Connection String here: ')) #OR os.getenv('AZURE_STORAGE_CONNECTION_STRING')
#Inicia um novo blob service // Start a new Blob
blob_service_client = BlobServiceClient.from_connection_string(connection_string)
#Inicia um novo container // Start a new container
container_client = blob_service_client.get_container_client(my_container)
#Crie o container // Creates the container
try:
    container_client.create_container()
except ResourceExistsError:
    pass

#Criar CSV para atualizações // Creates the update CSV *CHANGE YOUR FILE PATH*
with open(my_path+'/atualizados.csv', 'a',newline='',encoding="utf8") as c:
    writer = csv.writer(c)
    writer.writerow(['EAN', 'Produto', 'Img_Link'])
time.sleep(0.2)
#Insere o inicio do link // Insert's the link prefix
urlPre = 'https://'+my_acc+'.blob.core.windows.net/'+my_container+'/'

#Carregar CSV em um lista // Load the CSV into a list
csvLinks = []
with open(my_path +'/'+ sourceFile +'.csv',newline='', encoding="utf8") as csvfile:
    rows = csv.reader(csvfile, delimiter=',')
    for row in rows:
        csvLinks.append(row)
imagensBaixadas = []
#Fazer download das imagens // Donwload the images
for i in range(len(csvLinks)):
    if csvLinks[i][nameIndex] == '' or 'http' not in csvLinks[i][linkIndex]:
        continue
    #Requisitar a URL da imagem, ignorando verificação// Request image URL, skipping verification
    try:
        img = requests.get(csvLinks[i][linkIndex], stream = True, verify = False)
    except:
        with open(my_path+'/naoBaixados.csv', 'a',newline='', encoding="utf8") as c:
            writer = csv.writer(c)
            writer.writerow([csvLinks[i]])
        continue
    #Se o status do link for diferente de 200 (link saudável), pule pro próximo// If the link status is not 200 (Health link), skip to next link
    if img.status_code != 200:
        with open(my_path+'/naoBaixados.csv', 'a',newline='', encoding="utf8") as c:
            writer = csv.writer(c)
            writer.writerow([csvLinks[i]])
        continue
    #Se não, salve a imagem na pasta 'imagens', com o primeiro campo do CSV como nome// If not, save the image to folder and use the first CSV field as the name
    else:
        file = open('images/'+ str(csvLinks[i][nameIndex]) +'.jpg', 'wb')
        file.write(img.content)
        file.close()
        nomeImagem = str(csvLinks[i][nameIndex])
        print('Imagem ['+ nomeImagem +'.jpg] baixada')
        imagensBaixadas.append(nomeImagem)
    time.sleep(0.5)
    #Criar novo CSV para exportação // Create a new and updated CSV
    with open(my_path+'/atualizados.csv', 'a', newline='', encoding="utf8") as c:
        writer = csv.writer(c)
        writer.writerow([csvLinks[i][0],csvLinks[i][1],urlPre+str(nomeImagem)+'.jpg'])
    
urlUploads = []
for i in range(len(imagensBaixadas)):
    # Upload de imagens para o container // Upload images to container
    with open('imagens/'+imagensBaixadas[i]+'.jpg', "rb") as f:
        container_client.upload_blob(name=str(imagensBaixadas[i]+'.jpg'), data=f, overwrite=True)
        print('Upload da imagem ['+imagensBaixadas[i]+'.jpg] concluído. - '+ str(i+1) + '/'+str(len(imagensBaixadas)))
        urlUploads.append(urlPre+str(imagensBaixadas[i]+'.jpg'))
        print('URL da Imagem: ['+urlUploads[i]+']')
    time.sleep(0.5)
    # Fim do Upload // End Upload