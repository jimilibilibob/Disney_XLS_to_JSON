# -*- coding: utf-8 -*-
"""
Created on Tue Feb 12 10:47:49 2019

@author: Thomas
"""

import xlrd
import json
import os
from dateutil import tz
from dateutil import parser
import re
from pymongo import MongoClient



# Ecris le contenu de l'objet waitTime dans le fichier nommé filename
def save_json(waitTime, filename, data_json_path):
    # Garde le nom du fichier mais transforme le .xlsx en .json
    name = filename.replace(".xlsx", ".json")
    with open(data_json_path+name,"w",encoding="utf-8") as jsonFile :
    	jsonWaitTime=json.dumps(waitTime, ensure_ascii=False)
    	jsonFile.write(jsonWaitTime)

# La fonction permet de normaliser les noms des attractions
def normalize(attraction):
    attraction = ''.join(re.compile("™", re.IGNORECASE).split(attraction))
    attraction = ''.join(re.compile("®", re.IGNORECASE).split(attraction))
    attraction = ''.join(re.compile("\xa0", re.IGNORECASE).split(attraction))
    attraction = ''.join(re.compile("^'?nouveau'?\s?!? ", re.IGNORECASE).split(attraction))
    attraction = ''.join(re.compile("^'", re.IGNORECASE).split(attraction))
    attraction = ''.join(re.compile("'$", re.IGNORECASE).split(attraction))
    attraction = attraction.replace('’',"'")
    if 'Starport' in attraction:
        return 'Starport'
    if 'Star Tours' in attraction:
        return 'Star Tours'
    if attraction == 'Meet Mickey Mouse':
        return 'Rencontre avec Mickey'
    return attraction 

# Permet de convertir le fichier xlsx en Json
def excel_to_json(data_path, filename, geo_path):
        day_of_week = ["lundi","mardi","mercredi","jeudi","vendredi","samedi","dimanche"]
        to_zone = tz.tzlocal()
        # Ouvre le fichier xslx
        wb = xlrd.open_workbook(data_path+filename)
        sh = wb.sheet_by_name("Sheet1")
        header = sh.row_values(0)
        dt =""
        waitTime = []
        decalage = 0
        geo = ""
        jour = ""
        with open(geo_path, "r",encoding = "ISO-8859-1") as geo_json:
            geo = json.load(geo_json)
        # Si la premiere case est vide, le fichier xlsx est décalé, toute les données doivent donc être décalées
        # par rapport à la colonne.
        if sh.row_values(1)[0] == "" :
            decalage = 1
        for rownum in range(sh.nrows-1):
            # Affiche le pourcentage d'avancé
            if rownum*100%(sh.nrows-1) < 10:
                print( " - " + str(int(rownum*100/(sh.nrows-1))) + "%")
            if rownum!=0:
                # recupère la valeur de la ligne
                row_values = sh.row_values(rownum)
                # Permet de changer la date à un format local
                dt = parser.parse(row_values[decalage])
                dt = dt.astimezone(to_zone)
                jour = day_of_week[dt.weekday()]
                # Filtre par rapport à l'heure 
                if dt.hour > 8 and dt.hour < 20 :
                    # parcours la ligne
                    for value_index in range(1, len(row_values)-decalage) :
                        attente_value = row_values[value_index+decalage]
                        # attribution de la valeur -1 si l'attente n'est pas renseigné 
                        if row_values[value_index+decalage] == "":
                            attente_value = -1
                        else:
                            attente_value = int(attente_value)
                        attraction = normalize(header[value_index])
                        # créer le document json
                        document = {
                            "attraction": attraction,
                            "attente":attente_value,
                            "dateTime": dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
                            "jour" : jour
                        }
                        
                        # Recupere les positions des attractions via le ficher geo json
                        if attraction in geo.keys() :
                            document['position'] = geo[attraction]
                        waitTime.append(document)
        # Enregistre le document Json
        return waitTime

def toFile():
    # Chemin d'accès au données
    data_path = './excel/'
    #  Chemin d'écritures des Json 
    data_json_path = './json/'
    # Chemin d'accès au geo.json
    geo_path = './geo.json'
    # Parcous l'ensemble des documents de type xlsx
    for dirname, dirnames, filenames in os.walk(data_path):
        count = 0
        for filename in filenames:
            # vérifie que le fichier soit bien un excel de Disney
            if 'DisneylandParisMagicKingdom.xlsx' in filename :
                count=count+1
                print("--------------------------------")
                print("File " + str(count) +"/"+ str(len(filenames)) + " : ")
                print(filename)
                print("Percentage : ")
                json = excel_to_json(data_path, filename, geo_path)
                if len(json) > 1:
                    save_json(json, filename, data_json_path)
                print("File converted")    
    print("Data converted ! ") 

def toMongo():
    client = MongoClient()
    db = client.disney
    db.collection.drop()
    # Chemin d'accès au données
    data_path = './excel/'
    # Chemin d'accès au geo.json
    geo_path = './geo.json'
    # Parcous l'ensemble des documents de type xlsx
    for dirname, dirnames, filenames in os.walk(data_path):
        count = 0
        for filename in filenames:
            # vérifie que le fichier soit bien un excel de Disney
            if 'DisneylandParisMagicKingdom.xlsx' in filename :
                count=count+1
                print("--------------------------------")
                print("File " + str(count) +"/"+ str(len(filenames)) + " : ")
                print(filename)
                print("Percentage : ")
                json = excel_to_json(data_path, filename, geo_path)
                if len(json) > 1:
                    db.collection.insert_many(json)
                print("File converted")    
    print("Data converted ! ") 
    
if __name__ == "__main__":
    #toFile()
    toMongo()

            
