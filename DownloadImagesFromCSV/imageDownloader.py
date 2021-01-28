##########################
# Author: Matheus Martins#
##########################

import csv
import requests
import shutil
import requests.packages.urllib3
import time

requests.packages.urllib3.disable_warnings()

#Carregar CSV em uma lista // Load CSV file inta a list
csvLinks = []
with open('C:/Users/Matheus Martins/Desktop/Progs/DownloadImagesFromCSV/fonteImagens.csv',newline='', encoding="utf8") as csvfile:
    rows = csv.reader(csvfile, delimiter=',')
    for row in rows:
        csvLinks.append(row)
        
#Fazer download das imagens // Download images
for i in range(len(csvLinks)):
    #Requisitar a URL da imagem, ignorando verificação // Request image URL, ignoring verifications
    img = requests.get(csvLinks[i][2], stream = True, verify = False)
    #Se o status do link for diferente de 200 (link saudável), pule pro próximo // If link status is not 200 (Health Link), skip to next
    if img.status_code != 200:
        continue
    #Se não, salve a imagem na pasta 'imagens', com o primeiro campo do CSV como nome // If not, save image to file, naming it after the first CSV field
    else:
        file = open('imagens/'+ str(csvLinks[i][0]) +'.jpg', 'wb')
        file.write(img.content)
        file.close()
    time.sleep(0.5)