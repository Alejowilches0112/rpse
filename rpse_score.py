# coding=UTF-8

import unicodedata
import ujson as json
from pyspark.sql.types import StructType, StructField, StringType, LongType, FloatType

from sparkcc import CCSparkJob
from bs4 import BeautifulSoup
from bs4.dammit import EncodingDetector

from palabrasclave import *

import requests
import os
import boto3

from mongodb import mongoRPSE

from awsAnalytics import AWSTextAnalytics
from azureAnalytics import AnalyticsAzure

class RraeScoreJob(CCSparkJob):
    """ Procesamiento del indice RRAE: Sentimiento"""
    name = "RRAEScore"

    output_schema = StructType([
        StructField("key", StructType([
            StructField("diario", StringType(), True),
            StructField("service", StringType(), True),
            StructField("type", StringType(), True)]), True),
        StructField("cnt", LongType(), True)
    ])

    x = mongoRPSE()
    aws = AWSTextAnalytics()
    azure = AnalyticsAzure()

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
                    pagina = self.get_html_text_body(pagina, record)
                    titulominusculas = titulo.lower()
                    try:
                        # Configuración AWS
                        for emp in empresas:
                            for clave in emp['clave']:
                                if(clave.decode('utf-8').lower() in titulominusculas):
                                    #self.db.child("rrae/empresas/file_process/dinero").push(data,self.user['idToken'])
                                    empresa = True
                                    data = self.x.find_file_process(titulominusculas, emp['empresa'])
                                    obj = []
                                    for d in data:
                                        obj.append(d)
                                    yield (emp['empresa'], diarioActual , "Total"), 1
                                    if(len(pagina) > 0):
                                        # Realizar Analisis de Sentimiento y Busqueda de Frases Clave
                                        sentiemnt_aws = self.aws.procesarSentimientoAWS(pagina.encode('utf-8', errors='ignore'))
                                        keys_aws = self.aws.procesarKeyPhrasesAWS(pagina.encode('utf-8',  errors='ignore'))
                                        if(sentiemnt_aws['sentiment']):
                                            yield (emp['empresa'], diarioActual , "AWS "+sentiemnt_aws['sentiment']), 1
                                        else:
                                            yield (emp['empresa'], diarioActual , "AWS Error"), 1
                                        sentiemnt_azure = self.azure.sentiment_extraction(pagina.encode('utf-8', errors='ignore'))
                                        keys_azure = self.azure.key_phrase_extraction(pagina.encode('utf-8', errors='ignore'))
                                        objec = {'_id':obj[0]['_id'], 'AWS': [sentiemnt_aws,keys_aws], 'AZURE': [sentiemnt_azure,keys_azure]}
                                        
                                        self.x.update_mongo_score(objec)

                                        if(sentiemnt_azure['sentiment']):
                                            yield (emp['empresa'], diarioActual , "Azure "+sentiemnt_azure['sentiment']), 1
                                        else:
                                            yield (emp['empresa'], diarioActual , "Azure Error"), 1
                                    else:
                                        yield (emp['empresa'], diarioActual , "SIN HTML"), 1
                    except Exception as err:
                        f = open('error.txt', 'a')
                        f.write(str(err))
                        f.close()
                else:
                    return
        else:
            # WAT, warcinfo, request, non-WAT metadata records
            return

    def get_html_title(self, page, record):
        try:
            encoding = EncodingDetector.find_declared_encoding(
                page, is_html=True)
            soup = BeautifulSoup(page, "lxml", from_encoding=encoding)
            title = soup.title.string.strip()
            return title
        except:
            return ""

    def get_html_text_body(self, page, record):
        try:
            encoding = EncodingDetector.find_declared_encoding(
                page, is_html=True)
            soup = BeautifulSoup(page, "lxml", from_encoding=encoding)
            for script in soup(["script", "style"]):
                script.extract()
            return soup.get_text(" ", strip=True)
        except:
            return ""

    """def procesarSentimientoAzure(self, texto):
        # Configuración Azure
        subscription_key = "AZURE_SUBSCRIPTION_KEY"
        endpoint = "AZURE_ENDPOINT"
        sentiment_url = endpoint + "/text/analytics/v3.0-preview.1/sentiment"
        maxText = texto[:5120]

        azurePeticion = {"documents": [
            {"id": "1", "language": "es",
                "text": maxText},
        ]}

        headers = {"Ocp-Apim-Subscription-Key": subscription_key}
        respuesta = requests.post(
            sentiment_url, headers=headers, json=azurePeticion)
        sentiment = respuesta.json()
        return sentiment['documents'][0]['sentiment'], sentiment['documents'][0]['documentScores']['positive'], sentiment['documents'][0]['documentScores']['neutral'], sentiment['documents'][0]['documentScores']['negative']

    def procesarSentimientoAWS(self, texto):
        comprehend = boto3.client(
            service_name='comprehend', region_name='us-east-1')
        maxText = str(texto.encode("utf-8")[:5000], "utf-8", errors="ignore")
        sentiment = comprehend.detect_sentiment(
            Text=maxText, LanguageCode='es')
        return sentiment['Sentiment'], sentiment['SentimentScore']['Mixed'], sentiment['SentimentScore']['Positive'], sentiment['SentimentScore']['Neutral'], sentiment['SentimentScore']['Negative']
    """
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
            return ""

if __name__ == "__main__":
    job = RraeScoreJob()
    job.run()
