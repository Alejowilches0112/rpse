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
from mongodb import mongoRPSE

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
    
    x = mongoRPSE()
    def process_record(self, record):
        empresa = False
        empresas = self.x.findAllEmpresas()
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
                    for emp in empresas:
                        for clave in emp['clave']:
                            if(clave.decode('utf-8').lower() in titulominusculas):
                                data = {"diario": diarioActual, "empresa": emp['empresa'], "clave": clave, "titulo": titulominusculas, "html": pagina, 'AWS': [{},[]], 'AZURE': [{},[]] }
                                self.x.insert_mongo_files(data)
                                empresa = True
                                yield (emp['empresa'], diarioActual , "Total"), 1
                    # Contador match conglomerado - texto
                    if empresa:
                        print("hola empresa")
                    else:
                        yield ("No Procesado",diarioActual, "Total"), 1
                else:
                    yield ("No procesado", 'SIN DATOS' , "Total"), 1
            else:
                # WAT, warcinfo, request, non-WAT metadata records
                yield ("No procesado", 'SIN DATOS', "Total"), 1
        else:
            # WAT, warcinfo, request, non-WAT metadata records
            yield ("No procesado", 'SIN DATOS', "Total"), 1

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
            #f = open("prueba.txt","a")
            #f.write(contenidoPagina)
            #f.close()
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
