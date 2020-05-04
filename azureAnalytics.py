from azure.ai.textanalytics import TextAnalyticsClient
from azure.core.credentials import AzureKeyCredential
from mongodb import mongoRRAE
import json
class AnalyticsAzure:
    mongo = mongoRRAE()
    def __init__(self):
        print("Azure Analytics")
        
    def authenticate_client(self):
        key = "key_token"
        url = "url_service"
        ta_credential = AzureKeyCredential(key)
        text_analytics_client = TextAnalyticsClient(
                endpoint=url, credential=ta_credential)
        return text_analytics_client

    def key_phrase_extraction_example(self):
        try:
            client = self.authenticate_client()
            db = self.mongo
            files = db.find_file_process()
            documents = []
            i = 1
            data = {}
            for f in files:
                documents.append({"id":i,"lenguage": "es","text": f["html"]})
                i = i+1
                data = f
                data["keyphrases"] = []
                keyphrases = client.extract_key_phrases(documents = documents)[0]
                sentiment = client.analyze_sentiment(documents = documents)[0] 

                # Analisis de Sentimiento
                if not sentiment.is_error:
                    print(sentiment)
                    print(sentiment.confidence_scores)
                    data["score_azure"] = [{"sentiment": sentiment.sentiment}, {"Positive": sentiment.confidence_scores.positive}, {"Neutral": sentiment.confidence_scores.neutral},{"Negative":sentiment.confidence_scores.negative} ]
                else:
                    print(sentiment.id, sentiment.error)
                    data["score_azure"] = [{"error":sentiment.error, "id": sentiment.id}]

                # Keyphrases
                if not keyphrases.is_error:
                    #print("\tKey Phrases:")
                    for phrase in keyphrases.key_phrases:
                        data["keyphrases_azure"].append({"phrase": phrase})
                        #print("\t\t", phrase)
                else:
                    print(keyphrases.id, keyphrases.error)
                    data["keyphrases_azure"].append({"error": keyphrases.error, "id":keyphrases.id})

                db.inset_mongo_score(data)
                print(data)
                

        except Exception as err:
            print("Encountered exception. {}".format(err))

cliente = AnalyticsAzure()
cliente.key_phrase_extraction_example()
