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

connection_string = 'YOUR_CONNECTION_STRING_HERE' #OR os.getenv('AZURE_STORAGE_CONNECTION_STRING')
#Inicia um novo blob service // Start a new Blob
blob_service_client = BlobServiceClient.from_connection_string(connection_string)
#Inicia um novo container // Start a new container
container_client = blob_service_client.get_container_client("YOUR_CONTAINER")
#Crie o container // Creates the container
try:
    container_client.create_container()
except ResourceExistsError:
    pass

#Criar CSV para atualizações // Creates the update CSV *CHANGE YOUR FILE PATH*
with open('C:/Users/Matheus Martins/Desktop/Progs/UploadImageToAzure/atualizados.csv', 'a',newline='',encoding="utf8") as c:
    writer = csv.writer(c)
    writer.writerow(['EAN', 'Produto', 'Img_Link'])
time.sleep(0.2)
#Insere o inicio do link // Insert's the link prefix
urlPre = 'https://YOUR_ACCOUNT.blob.core.windows.net/YOUR_CONTAINER/'

#Carregar CSV em um lista // Load the CSV into a list
csvLinks = []
with open('C:/Users/Matheus Martins/Desktop/Progs/UploadImageToAzure/fonteImagens.csv',newline='', encoding="utf8") as csvfile:
    rows = csv.reader(csvfile, delimiter=',')
    for row in rows:
        csvLinks.append(row)
imagensBaixadas = []
#Fazer download das imagens // Donwload the images
for i in range(len(csvLinks)):
    if csvLinks[i][0] == '' or 'http' not in csvLinks[i][2]:
        continue
    #Requisitar a URL da imagem, ignorando verificação// Request image URL, skipping verification
    try:
        img = requests.get(csvLinks[i][2], stream = True, verify = False)
    except:
        with open('C:/Users/Matheus Martins/Desktop/Progs/UploadImageToAzure/naoBaixados.csv', 'a',newline='', encoding="utf8") as c:
            writer = csv.writer(c)
            writer.writerow([csvLinks[i]])
        continue
    #Se o status do link for diferente de 200 (link saudável), pule pro próximo// If the link status is not 200 (Health link), skip to next link
    if img.status_code != 200:
        with open('C:/Users/Matheus Martins/Desktop/Progs/UploadImageToAzure/naoBaixados.csv', 'a',newline='', encoding="utf8") as c:
            writer = csv.writer(c)
            writer.writerow([csvLinks[i]])
        continue
    #Se não, salve a imagem na pasta 'imagens', com o primeiro campo do CSV como nome// If not, save the image to folder and use the first CSV field as the name
    else:
        file = open('imagens/'+ str(csvLinks[i][0]) +'.jpg', 'wb')
        file.write(img.content)
        file.close()
        nomeImagem = str(csvLinks[i][0])
        print('Imagem ['+ nomeImagem +'.jpg] baixada')
        imagensBaixadas.append(nomeImagem)
    time.sleep(0.5)
    #Criar novo CSV para exportação // Create a new and updated CSV
    with open('C:/Users/Matheus Martins/Desktop/Progs/UploadImageToAzure/atualizados.csv', 'a', newline='', encoding="utf8") as c:
        writer = csv.writer(c)
        writer.writerow([csvLinks[i][0],csvLinks[i][1],urlPre+str(nomeImagem)+'.jpg'])
    
urlUploads = []
for i in range(len(imagensBaixadas)):
    # Upload de imagens para o container // Upload images to container
    with open('imagens/'+imagensBaixadas[i]+'.jpg', "rb") as f:
        container_client.upload_blob(name=str(imagensBaixadas[i]+'.jpg'), data=f, overwrite=True)
        print('Upload da imagem ['+imagensBaixadas[i]+'.jpg] concluído. - '+ str(i+1) + '/'+str(len(imagensBaixadas))+ )
        urlUploads.append(urlPre+str(imagensBaixadas[i]+'.jpg'))
        print('URL da Imagem: ['+urlUploads[i]+']')
    time.sleep(0.5)
    # Fim do Upload // End Upload