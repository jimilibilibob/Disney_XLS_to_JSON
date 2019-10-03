# -*- coding: utf-8 -*-
"""
Created on Tue Oct  1 13:14:32 2019

@author: Thomas
"""

import os
import json
from elasticsearch import Elasticsearch
from elasticsearch import helpers

# Retroune un fichier json depuis la liste des fichers
def gendata(jsons):
    for j in jsons:
        if 'position' in j.keys():
            yield {
                "_index": "disney",
                "attente":j["attente"],
                "attraction":j["attraction"],
                "dateTime":j["dateTime"],
                "jour":j["jour"],
                "position":j["position"]
            }
        else:
            yield {
                "_index": "disney",
                "attente":j["attente"],
                "attraction":j["attraction"],
                "dateTime":j["dateTime"],
                "jour":j["jour"]
            }
        
def main():
    # Ouvre la connexion à Elasticsarch
    es = Elasticsearch()
    data_json_path = './json/'
    
    print("--- Delete Index ---")
    # Supprime l'index disney et ignore l'erreur lui indiquant que celui-ci n'éxiste pas
    es.indices.delete(index='disney', ignore=[400,404])
    # Définition du mapping
    mapping = {
      "mappings": {
        "properties": {
          "attente": {
            "type": "integer"
          },
          "attraction": {
            "type": "keyword"
          },
          "dateTime": {
            "type": "date"
          },
          "jour": {
            "type": "keyword"
          },
          "position": {
            "type": "geo_point"
          }
        }
      }
    }
    print("--- Create Index ---")
    # Création de l'index avec son mapping 
    es.indices.create(index='disney', ignore=400, body=mapping)
    print('Start import')
    # Parcours tout les fichiers du dossier
    for dirname, dirnames, filenames in os.walk(data_json_path):
        count = 0
        for filename in filenames:
            count = count+1
            print("File " + str(count) +"/"+ str(len(filenames)) + " : ")
            print(filename)
            print("Import in progress... ")
            input_file = open(data_json_path+filename, encoding='utf-8')
            # Transforme le document texte en Json
            json_array = json.load(input_file)
            # Index les fichiers Json
            helpers.bulk(es, gendata(json_array))
            print(" Imported ")
            print("----------------")
    print('All data imported !')

if __name__ == "__main__":
    main()

