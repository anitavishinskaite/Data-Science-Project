import csv
import json
import pandas as pd
import sparql_dataframe
from pandas import read_csv, read_sql, Series
from sqlite3 import connect
from rdflib import Graph, Namespace, Literal, URIRef
from rdflib.namespace import RDF
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
from sparql_dataframe import get
from typing import List

class IdentifiableEntity(object):
    def __init__(self, id):
        self.id = id

 # Methods
    def getID(self):
        return self.id

class Annotation(IdentifiableEntity):
    def __init__(self, id, motivation, target, body):
        self.motivation = motivation
        self.target = target
        self.body = body

        #Inheritance
        super().__init__(id)

 # Methods
    def getBody(self):
        return self.body
    
    def getMotivation(self):
        return self.motivation
    
    def getTarget (self):
        return self.target
    
class Image(IdentifiableEntity):
    pass
    
class EntityWithMetadata(IdentifiableEntity):
    def __init__(self, id, label, creators , title = ""):
        self.label = label
        self.title = title
        self.creators = set()
        for creator in creators:
            self.creators.add(creator)

        #Inheritance
        super().__init__(id)

 # Methods
    def getLabel (self):
        return self.label
    
    def getTitle (self):
        return self.title
    
    def getCreators (self):
        result = []
        for creator in self.creators:
            result.append(creator)
        result.sort()
        return result
    
class Collection(EntityWithMetadata):
    def __init__(self, id, label, creators, items, title=""):
        self.items = []
        for item in items:
            self.items.append(item)

        #Inheritance
        super().__init__(id, label, creators, title)

  # Method
    def getItems(self):
        return self.items

class Manifest(EntityWithMetadata):
    def __init__(self, id, label, creators, items, title=""):
        self.items = []
        for item in items:
            self.items.append(item)

        #Inheritance
        super().__init__(id, label, creators, title)

  # Method
    def getItems(self):
        return self.items

class Canvas(EntityWithMetadata):
    pass       


########### Processors ###########

class Processor(object):
    def __init__(self, dbPathOrUrl=""):
        self.dbPathOrUrl = dbPathOrUrl

    def getDbPathOrUrl(self):
        return self.dbPathOrUrl

    def setDbPathOrUrl(self, pathOrUrl):
        self.dbPathOrUrl = pathOrUrl
        
# Relational database 
class AnnotationProcessor(Processor):
    def __init__(self, dbPathOrUrl=""):
        super().__init__(dbPathOrUrl)

    def uploadData(self, path):
        self.path = path

        try:
            annotations= pd.read_csv(path, 
                                    keep_default_na=False,
                                    dtype={
                                        "id": "string",
                                        "body": "string",
                                        "target": "string",
                                        "motivation": "string",
                                        })

            annotation_internal_id = []
            for idx, row in annotations.iterrows():
                annotation_internal_id.append("annotation-" + str(idx))
            annotations.insert(0, "internalId", Series(annotation_internal_id, dtype="string"))

            images = annotations[["id", "body"]].drop_duplicates().reset_index(drop=True)
            image_internal_id = ["image-" + str(idx) for idx in range(len(images))]
            images.insert(0, "internalId", image_internal_id)

            identifiable_entities = annotations[["id", "target"]].drop_duplicates().reset_index(drop=True)
            identifiable_entity_internal_id = ["id_entity-" + str(idx) for idx in range(len(identifiable_entities))]
            identifiable_entities.insert(0, "internalId", identifiable_entity_internal_id)

            with connect("relational.db") as con:
                annotations.to_sql("Annotation", con, if_exists="replace", index=False)
                images.to_sql("Image", con, if_exists="replace", index=False)
                identifiable_entities.to_sql("Identifiable Entity", con, if_exists="replace", index=False)
            con.commit()
            return True

        except Exception as e:
            print(f"Error while uploading data: {e}")
            return False

rel_path = "relational.db"
ann_dp = AnnotationProcessor()
ann_dp.setDbPathOrUrl(rel_path)
ann_dp.uploadData("data/annotations.csv")

class MetadataProcessor(Processor):
    def __init__(self, dbPathOrUrl=""):
        super().__init__(dbPathOrUrl)

    def uploadData(self, path):
        self.path = path

        try:
            entities_with_metadata = pd.read_csv(path, 
                                keep_default_na=False,
                                dtype={
                                    "id": "string",
                                    "title": "string",
                                    "creator": "string"
                                })
            
            entity_with_metadata_internal_id = []
            for idx, row in entities_with_metadata.iterrows():
                entity_with_metadata_internal_id.append("metadata-" + str(idx))
            entities_with_metadata.insert(0, "internalId", Series(entity_with_metadata_internal_id, dtype="string"))

            collections = entities_with_metadata[["id", "title", "creator"]].drop_duplicates().reset_index(drop=True)
            collection_internal_id = ["collection-" + str(idx) for idx in range(len(collections))]
            collections.insert(0, "internalId", collection_internal_id)

            manifests = entities_with_metadata[["id", "title", "creator"]].drop_duplicates().reset_index(drop=True)
            manifest_internal_id = ["manifest-" + str(idx) for idx in range(len(manifests))]
            manifests.insert(0, "internalId", manifest_internal_id)

            canvases = entities_with_metadata[["id", "title", "creator"]].drop_duplicates().reset_index(drop=True)
            canvas_internal_id = ["id_entity-" + str(idx) for idx in range(len(canvases))]
            canvases.insert(0, "internalId", canvas_internal_id)

            with connect("relational.db") as con:
                entities_with_metadata.to_sql("EntityWithMetadata", con, if_exists="replace", index=False)
                collections.to_sql("Collection", con, if_exists="replace", index=False)
                manifests.to_sql("Manifest", con, if_exists="replace", index=False)
                canvases.to_sql("Canvas", con, if_exists="replace", index=False)
            con.commit()
            return True

        except Exception as e:
            print(f"Error while uploading data: {e}")
            return False

met_dp = MetadataProcessor()
met_dp.setDbPathOrUrl(rel_path)
met_dp.uploadData("data/metadata.csv")

# RDF triplestore
class CollectionProcessor(Processor):
    def __init__(self, dbPathOrUrl=""):
        super().__init__(dbPathOrUrl)

    def uploadData(self, path):
        self.path = path
        
        try:
                base_url = "https://github.com/anitavishinskaite/data_science"

                graph = Graph()

                # classes of resources
                EntityWithMetadata = URIRef("https://dbpedia.org/page/Entity")
                Collection = URIRef("https://schema.org/Collection")
                Manifest = URIRef("https://dbpedia.org/page/Manifest")
                Canvas = URIRef("https://dbpedia.org/page/Canvas")

                # attributes related to classes
                id = URIRef("https://schema.org/identifier")
                label = URIRef("https://dbpedia.org/page/label")
                title = URIRef("https://schema.org/title")
                creators = URIRef("https://schema.org/creator")

                # relations among classes
                items = URIRef("https://schema.org/isPartOf")
            
                with open(path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                
                    collection_uri = URIRef(data['id'])
                    graph.add((collection_uri, RDF.type, Collection))
                    graph.add((collection_uri, id, Literal(data['id'])))
                    graph.add((collection_uri, label, Literal(data['label']['none'][0])))

                    for manifest in data['items']:
                        manifest_uri = URIRef(manifest['id'])
                        graph.add((manifest_uri, RDF.type, Manifest))
                        graph.add((manifest_uri, id, Literal(manifest['id'])))
                        graph.add((manifest_uri, label, Literal(manifest['label']['none'][0])))

                        graph.add((collection_uri, items, manifest_uri))

                        for canvas in manifest['items']:
                            canvas_uri = URIRef(canvas['id'])
                            graph.add((canvas_uri, RDF.type, Canvas))
                            graph.add((canvas_uri, id, Literal(canvas['id'])))
                            graph.add((canvas_uri, label, Literal(canvas['label']['none'][0])))

                            graph.add((manifest_uri, items, canvas_uri))

                store = SPARQLUpdateStore(graph)
                endpoint = 'http://127.0.0.1:9999/blazegraph/sparql'
                store.open((endpoint, endpoint))
                for triple in graph.triples((None, None, None)):
                    store.add(triple)
                store.close()

        except Exception as e:
            print(f"Error while uploading data: {e}")
            return False
            
grp_endpoint = "http://127.0.0.1:9999/blazegraph/sparql"
col_dp = CollectionProcessor()
col_dp.setDbPathOrUrl(grp_endpoint)
col_dp.uploadData("data/collection-1.json")
col_dp.uploadData("data/collection-2.json")


########### Query processors ###########

class QueryProcessor(Processor):
    def __init__(self, dbPathOrUrl=''):
        super().__init__(dbPathOrUrl)
    
    def getEntityById(self, entityId):
        result = self.db.query_entity(entityId)
        if result:
            df = pd.DataFrame(result)
            return df
        else:
            return None
        
# Query processor for relational database
class RelationalQueryProcessor(QueryProcessor):
    def __init__(self, dbPathOrUrl=''):
        super().__init__(dbPathOrUrl)

    def getAllAnnotations(self):
        with connect(self.dbPathOrUrl) as con:
            query = "SELECT * FROM Annotation"
            df_sql = pd.read_sql(query, con)
            return df_sql
    
    def getAllImages(self):
        with connect(self.dbPathOrUrl) as con:
            query = "SELECT * FROM Image"
            df_sql = pd.read_sql(query, con)
            return df_sql
        
    def getAnnotationsWithBody(self, bodyId):
        with connect(self.dbPathOrUrl) as con:
            query = f"SELECT * FROM Annotation WHERE body='{bodyId}'"
            df_sql = pd.read_sql(query, con)
            return df_sql
        
    def getAnnotationsWithBodyAndTarget(self, bodyId, targetId):
        with connect(self.dbPathOrUrl) as con:
            query = f"SELECT * FROM Annotation WHERE body='{bodyId}' AND target='{targetId}'"
            df_sql = pd.read_sql(query, con)
            return df_sql
        
    def getAnnotationsWithTarget(self, targetId):
        with connect(self.dbPathOrUrl) as con:
            query = f"SELECT * FROM Annotation WHERE target='{targetId}'"
            df_sql = pd.read_sql(query, con)
            return df_sql
        
    def getEntitiesWithCreator(self, creatorName):
        with connect(self.dbPathOrUrl) as con:
            query = f"SELECT * FROM EntityWithMetadata WHERE creator LIKE '%{creatorName}%'"
            df_sql = pd.read_sql(query, con)
            return df_sql

    def getEntitiesWithTitle(self, title):  
        with connect(self.dbPathOrUrl) as con:
            query = f"SELECT * FROM EntityWithMetadata WHERE title = '{title}'"
            df_sql = pd.read_sql(query, con)
            return df_sql

rel_qp = RelationalQueryProcessor()
rel_qp.setDbPathOrUrl(rel_path)

# Query processor for graph database
class TriplestoreQueryProcessor(QueryProcessor):
    def __init__(self, dbPathOrUrl=''):
        super().__init__(dbPathOrUrl)

    def getAllCanvases(self):
        query = """
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX dbp: <https://dbpedia.org/page/>
        PREFIX schema: <https://schema.org/>

        SELECT ?canvas_id ?canvas_label
        WHERE {
                ?canvas rdf:type dbp:Canvas .
                ?canvas schema:identifier ?canvas_id .
                ?canvas dbp:label ?canvas_label .
        }
        """
        df_sparql = get(self.dbPathOrUrl, query, True)
        return df_sparql
    
    def getAllCollections(self):
        query = """
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX dbp: <https://dbpedia.org/page/>
        PREFIX schema: <https://schema.org/>

        SELECT ?collection_id ?collection_label
        WHERE {
            ?collection rdf:type schema:Collection .
            ?collection schema:identifier ?collection_id .
            ?collection dbp:label ?collection_label .
        }
        """
        df_sparql = get(self.dbPathOrUrl, query, True)
        return df_sparql     
    
    def getAllManifests(self):
        query = """
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX dbp: <https://dbpedia.org/page/>
        PREFIX schema: <https://schema.org/>

        SELECT ?manifest_id ?manifest_label
        WHERE {
            ?manifest rdf:type dbp:Manifest .
            ?manifest schema:identifier ?manifest_id .
            ?manifest dbp:label ?manifest_label .
        }
        """
        df_sparql = get(self.dbPathOrUrl, query, True)
        return df_sparql     
    
    def getCanvasesInCollection(self, collectionId):
        query = """
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX dbp: <https://dbpedia.org/page/>
        PREFIX schema: <https://schema.org/>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    
        SELECT DISTINCT ?canvas_id ?canvas_label
        WHERE {{
        ?collection rdf:type schema:Collection ;
                    schema:identifier "{0}" ;
                    schema:isPartOf ?manifest .
        ?manifest rdf:type dbp:Manifest ;
                  schema:isPartOf ?canvas .
        ?canvas rdf:type dbp:Canvas ;
                schema:identifier ?canvas_id ;
                dbp:label ?canvas_label .
        }}
        """.format(collectionId)
     
        df_sparql = get(self.dbPathOrUrl, query, True)
        return df_sparql  
           
    def getCanvasesInManifest(self, manifestId):
        query = """
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX dbp: <https://dbpedia.org/page/>
        PREFIX schema: <https://schema.org/>
        
        SELECT DISTINCT ?canvas_id ?canvas_label
        WHERE {{
            ?manifest rdf:type dbp:Manifest ;
                    schema:identifier "{0}" ;
                    schema:isPartOf ?canvas .
            ?canvas rdf:type dbp:Canvas ;
                    schema:identifier ?canvas_id ;
                    dbp:label ?canvas_label .
        }}
        """.format(manifestId)
        df_sparql = get(self.dbPathOrUrl, query, True)
        return df_sparql    

    def getManifestsInCollection(self, collectionId):
        query = """
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX dbp: <https://dbpedia.org/page/>
        PREFIX schema: <https://schema.org/>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

        SELECT DISTINCT ?manifest_id ?manifest_label
        WHERE {{
            ?collection rdf:type schema:Collection ;
                        schema:identifier "{0}" ;
                        schema:hasPart ?manifest .
            ?manifest rdf:type dbp:Manifest ;
                    schema:identifier ?manifest_id ;
                    dbp:label ?manifest_label .
        }}
        """.format(collectionId)
        df_sparql = get(self.dbPathOrUrl, query, True)
        return df_sparql        
    
grp_qp = TriplestoreQueryProcessor()
grp_qp.setDbPathOrUrl(grp_endpoint)


class GenericQueryProcessor(object):
    def __init__(self):
        self.queryProcessors = []
    
    def cleanQueryProcessors(self):
        try:
            self.queryProcessors = []
            return True
        except:
            return False
        
    def addQueryProcessor(self, processor: QueryProcessor):
        try:
            self.queryProcessors.append(processor)
            return True
        except:
            return False  
        
generic = GenericQueryProcessor()
generic.addQueryProcessor(rel_qp)
generic.addQueryProcessor(grp_qp)

result_q1 = generic.getAllCanvas()
print(result_q1)

