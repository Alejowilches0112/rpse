# coding=UTF-8
import re

from collections import Counter

import unicodedata
import json as js
import ujson as json
from pyspark.sql.types import StructType, StructField, StringType, LongType

from sparkcc import CCSparkJob
from bs4 import BeautifulSoup
from bs4.dammit import EncodingDetector

from palabrasclave import *
#import pyrebase
from mongodb import mongoRRAE

class RraeCountJob(CCSparkJob):
    """ Procesamiento del indice RRAE: Conteo"""
    name = "RRAEConteo"

    output_schema = StructType([
        StructField("key", StructType([
            StructField("name", StringType(), True),
            StructField("diario", StringType(), True),
            StructField("type", StringType(), True)]), True),
        StructField("cnt", LongType(), True)
    ])
    word_pattern = re.compile('\w+', re.UNICODE)
    #config = {"apiKey": "",
    #"authDomain": "",
    #"databaseURL": "",
    #"projectId": "",
    #"storageBucket": "",
    #"messagingSenderId": "",
    #"appId": "",
    #"measurementId": ""
    #}
    #firebase = pyrebase.initialize_app(config)
    #auth = firebase.auth()
    #user = auth.sign_in_with_email_and_password("email", "pass")
    #db = firebase.database()

    #mongoc = MongoClient("localhost:27017")
    #dbmongo = mongo.rrae
    x = mongoRRAE()
    
    def process_record(self, record):
        empresa = False
        if record.rec_type == 'response':
            # WARC response record
            contentType = record.http_headers.get_header('content-type', None)
            if contentType is not None and 'html' in contentType:
                # WARC es HTML
                payload = record.rec_headers.get_header(
                    'WARC-Identified-Payload-Type')
                if payload is not None and 'html' in payload:
                    html = record.content_stream().read()
                    diarioActual = self.x.find_diario_in_html(html)
                    titulo = self.get_html_title(html, record)
                    pagina = self.get_html_text(html, diarioActual)
                    pagina = self.get_html_body(pagina, record)
                    titulominusculas = titulo.lower()
                    # Detectar palabras claves en título
                    for clave in Ecopetrol:
                        if(clave.decode('utf-8').lower() in titulominusculas):
                            data = {"diario": diarioActual, "empresa": "Ecopetrol", "clave": clave, "titulo": titulominusculas, "html": pagina}
                            self.x.insert_mongo_files(data)
                            #self.dbmongo.reviews.insert_one(data)
                            #self.db.child("rrae/empresas/file_process/dinero").push(data,self.user['idToken'])
                            self.process_record_word(pagina, titulominusculas)
                            empresa = True
                            yield ("Ecopetrol clave", diarioActual , clave), 1
                            yield ("Ecopetrol", diarioActual , "Total"), 1

                    # Detectar palabras conglomerado en título
                    for clave in BPC:
                        if(clave.decode('utf-8').lower() in titulominusculas):
                            empresa = True
                            data = {"diario": diarioActual,"empresa": "BPC", "clave": clave, "titulo": titulominusculas, "html": pagina} 
                            self.x.insert_mongo_files(data)
                            #self.dbmongo.reviews.insert_one(data)
                            #self.db.child("rrae/empresas/file_process/dinero").push(data,self.user['idToken'])
                            self.process_record_word(pagina, titulominusculas)
                            yield ("BPC clave", diarioActual , clave), 1
                            yield ("BPC", diarioActual , "Total"), 1

                    # Contador match conglomerado - texto
                    if empresa:
                        print("hola empresa")
                    else:
                        yield ("No Procesado",diarioActual, "Total"), 1
                else:
                    yield ("No procesado", str(payload) , "Total"), 1
            else:
                # WAT, warcinfo, request, non-WAT metadata records
                yield ("No procesado", str(contentType), "Total"), 1
        else:
            # WAT, warcinfo, request, non-WAT metadata records
            yield ("No procesado", record, "Total"), 1

    def get_html_title(self, page, record):
        try:
            encoding = EncodingDetector.find_declared_encoding(
                page, is_html=True)
            soup = BeautifulSoup(page, "lxml", from_encoding=encoding)
            title = soup.title.string.strip()
            return title
        except:
            return ""

    def get_html_body(self, page, record):
        try:
            encoding = EncodingDetector.find_declared_encoding(
                page, is_html=True)
            soup = BeautifulSoup(page, "lxml", from_encoding=encoding)
            for script in soup(["script", "style", "<p>", "</p>", "<div","</div", "class=", "<strong>", "</strong>"]):
                script.extract()
            return soup.get_text(" ", strip=True)
        except:
            return ""

    def get_html_text(self,contenidoPagina, record):
        try:
            """f = open("prueba.txt","a")
            f.write(contenidoPagina)
            f.close()"""
            claveInicio = self.x.html_inicio(record)
            claveFin = self.x.html_fin(record)
            lugarInicio = contenidoPagina.find(claveInicio)
            lugarFin = contenidoPagina.find(claveFin, lugarInicio, len(contenidoPagina))
            contenidoPagina = contenidoPagina[lugarInicio:lugarFin]
            return contenidoPagina
        except:
            f = open("errores.txt","a")
            f.write("An exception occurred")
            f.close()
            return ""

if __name__ == "__main__":
    job = RraeCountJob()
    job.run()
