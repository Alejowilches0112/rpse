from azure.ai.textanalytics import TextAnalyticsClient
from azure.core.credentials import AzureKeyCredential

class AnalyticsAzure:

    def authenticate_client(self):
        key = "KEY_TOKEN"
        url = "URL_SERVICE"
        ta_credential = AzureKeyCredential(key)
        text_analytics_client = TextAnalyticsClient(
                endpoint=url, credential=ta_credential)
        return text_analytics_client

    def key_phrase_extraction(self, texto):
        try:
            client = self.authenticate_client()
            documents = []
            documents.append({"id":1,"lenguage": "es","text": texto[:5000]})
            keys = []
            keyphrases = client.extract_key_phrases(documents = documents)[0]            
            # Keyphrases
            if not keyphrases.is_error:
                for phrase in keyphrases.key_phrases:
                    keys.append(phrase)
            else:
                print("error ", keyphrases.error)
            
            return keys

        except Exception as err:
            print("Encountered exception. {}".format(err))
        
    def sentiment_extraction(self, texto):
        #try:
        if(texto != ""):
            client = self.authenticate_client()
            documents = []
            documents.append({"id":1,"lenguage": "es","text": texto[:5000]})
            sentiment = client.analyze_sentiment(documents = documents)[0] 
            # Analisis de Sentimiento
            if not sentiment.is_error:
                return {"sentiment": sentiment.sentiment, "Positive": sentiment.confidence_scores.positive, "Neutral": sentiment.confidence_scores.neutral, "Negative":sentiment.confidence_scores.negative}
            else:
                return{"error ",sentiment.error}

            #db.inset_mongo_score(data)
        #except Exception as err:
        #    print("Encountered exception. {}".format(err))

