from google.cloud import language_v1
from google.cloud.language import enums
from google.cloud.language import types

class AnalyticsGoogle:
    def auth_google_cloud(self):
        with open("../tesis-274400-3f8c7d5ca14d.json", "r") as f:
            cred = f.read()
            f.close()
        client = language_v1.LanguageServiceClient(credentials = cred)
        return client

    def service_analyze_text(self,data):
        try:
            client = self.auth_google_cloud()
            document = types.Document(
                content=data,
                type=enums.Document.Type.PLAIN_TEXT
            )
            sentiment = client.analyze_sentiment(document = document)
            print(sentiment)
        except Exception as err:
            print('An exception occurred',err)

x = AnalyticsGoogle()
data = x.service_analyze_text('Muy al servicio el de Claro')