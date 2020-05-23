import boto3
import json

class AWSTextAnalytics():

    def procesarSentimientoAWS(self, texto):
        comprehend = boto3.client(
            service_name='comprehend', region_name='us-east-1')
        maxText = str(texto[:5000])
        sentiment = comprehend.detect_sentiment(
            Text=maxText, LanguageCode='es')
        return {'sentiment': sentiment['Sentiment'], "Mixto": sentiment['SentimentScore']['Mixed'], "Positivo": sentiment['SentimentScore']['Positive'], "Neutral": sentiment['SentimentScore']['Neutral'], "Negativo": sentiment['SentimentScore']['Negative']}
    
    def procesarKeyPhrasesAWS(self, texto):
        comprehend = boto3.client(
            service_name='comprehend', region_name='us-east-1')
        maxText = str(texto[:5000])
        response = comprehend.detect_key_phrases(Text=maxText, LanguageCode='es')
        keys = []
        for resp in response['KeyPhrases']:
            keys.append(resp['Text'])
        return keys

