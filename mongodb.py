from pymongo import MongoClient
class mongoRPSE:

    mongos = ""

    #insertar datos
    
    def insert_mongo_files(self,data):
        mongoc = MongoClient("localhost:27017")
        mongodb = mongoc.rpse
        mongodb.empresas_file_process.insert_one(data)

    def insert_mongo_score(self,data):
        mongoc = MongoClient("localhost:27017")
        mongodb = mongoc.rpse
        mongodb.empresas_file_score.insert_one(data)

    def inset_mongo_count(self,data):
        mongoc = MongoClient("localhost:27017")
        mongodb = mongoc.rpse
        mongodb.empresas_conteo.insert_one(data)

    def update_mongo_score(self,data):
        mongoc = MongoClient("localhost:27017")
        mongodb = mongoc.rpse
        mongodb.empresas_file_process.update_one({"_id":data["_id"]},{'$set': data})

    #Buscar empresa
    def find_diario_in_html(self, html):
        diarios = self.findAllDiario()
        data = "<meta name=\"url\" content=\"https://"
        if(data in str(html).lower()):
            for diario in diarios:
                print("filtro semana")
                d = data+str(diario["url"])
                if(d in str(html).lower()):
                    diarioActual = diario["nombre"].lower()
                    return diarioActual
        data = "<meta property=\"og:url\" content=\"https://"
        data1 = "<meta property=\"og:url\" content=\"http://"
        if(data in str(html).lower() or data1 in str(html).lower()):
            for diario in diarios:
                d = data+str(diario["url"])
                d1 = data1+str(diario["url"])
                if(d in str(html).lower() or d1 in str(html).lower()):
                    diarioActual = diario["nombre"].lower()
                    return diarioActual
        else:
            for diario in diarios:
                url = str(diario["url"])
                if("www." in url):
                    url = str(diario["url"])[4:len(url)]
                if(url in str(html).lower()):
                    diarioActual = diario["nombre"].lower()
                    return diarioActual
        return ""
    #listar Datos
    def find_file_process(self, titulo, empresa):
        mongoc = MongoClient("localhost:27017")
        db = mongoc.rpse
        files = db.empresas_file_process
        query = {"empresa": empresa, "titulo": titulo}
        data = files.find(query)
        return data
    
    def findAllDiario(self):
        mongoc = MongoClient("localhost:27017")
        db = mongoc.rpse
        diarios = db.diarios
        return diarios.find()
    
    def find_diario(self, diario):
        mongoc = MongoClient("localhost:27017")
        db = mongoc.rpse
        query = {"nombre": diario}
        diario = db.diarios.find(query)
        for d in diario:
            return d
    
    def findAllEmpresas(self):
        mongoc = MongoClient("localhost:27017")
        db = mongoc.rpse
        empresas = db.empresas
        return empresas.find()

    #Filtros para limpiar datos
    def html_inicio(self, diario):
        mongoc = MongoClient("localhost:27017")
        db = mongoc.rpse
        query = {"nombre": diario}
        diario = db.diarios.find(query)
        for d in diario:
            return str(d["inicio"])

    def html_fin(self, diario):
        mongoc = MongoClient("localhost:27017")
        db = mongoc.rpse
        query = {"nombre": diario}
        diario = db.diarios.find(query)
        for d in diario:
            return str(d["fin"])

    def prueba(self):
        self.mongos = "method prueba"
        mongoc = MongoClient("localhost:27017")
        db = mongoc.rpse
        #Insertar Diarios de Prueba
        diarios=[
        {"url": "www.eltiempo.com", "nombre": "eltiempo", "inicio":"<div class=\"articulo-contenido\" itemprop=\"articleBody\">", "fin": "<div class=\"articulo-enlaces\""},
        {"url": "www.elespectador.com", "nombre":"espectador", "inicio": '<div class="node-body content_nota field field--name-body field--type-text-with-summary field--label-hidden', "fin": "</div>"},
        {"url": "www.dinero.com", "nombre":"dinero", "inicio": "<div id=\"contentItem\">", "fin": "</div>"},
        {"url": "www.semana.com", "nombre":"semana", "inicio": "<!-- Alliance -->", "fin": "</div>"}, 
        {"url": "sostenibilidad.semana.com", "nombre":"sostenibilidad", "inicio": "<!-- Alliance -->", "fin": "</div>"}, 
        {"url": "www.larepublica.co", "nombre":"larepublica", "inicio": "<div class=\"lead\">", "fin": "<p>&nbsp;</p>"}, 
        {"url": "www.portafolio.co", "nombre":"portafolio", "inicio": "<div class=\"article-content\" itemprop=\"articleBody\"", "fin": "<div class=\"article-bottom-ads\""},
        {"url": "gerente.com/co", "nombre":"gerente", "inicio": "<div class=\"article-content\">", "fin": "</div>"}]
        for d in diarios:
            db.diarios.insert_one(d)
        #Insertar Informacion de empresas a buscar
        empresas = [
        {'empresa': 'ECOPETROL', 'clave': ['ecopetrol', 'reficar']},
        {'empresa': 'CANACOL ENERGY', 'clave': ['canacol', 'canacol energy']},
        {'empresa': 'CEPSA', 'clave': ['cepsa', 'cepsa colombia']},
        {'empresa': 'GENERAL', 'clave': ['fracking','gasoductos','petroleras']},
        {'empresa': 'BPC', 'clave': ['british petroleum','british petroleum']}]
        for d in empresas:
            db.empresas.insert_one(d)


