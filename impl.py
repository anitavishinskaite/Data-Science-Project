import csv
import json
import pandas as pd
import sparql_dataframe
from pandas import read_csv, read_sql, Series, DataFrame
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
    def getId(self):
        return self.id
    
    def __str__(self):
        return f"id: {self.id}"

class Image(IdentifiableEntity):
    pass

class Annotation(IdentifiableEntity):
    def __init__(self, id, body, target, motivation):
        self.body = body
        self.target = target
        self.motivation = motivation

        #Inheritance
        super().__init__(id)

 # Methods
    def getBody(self)-> List[Image]:
        return self.body
    
    def getMotivation(self):
        return self.motivation
    
    def getTarget (self)-> List[IdentifiableEntity]:
        return self.target
    
    def __str__(self):
        annotation = []
        if self.body:
            annotation.append(f"body: {self.body}")
        if self.target:
            annotation.append(f"target: {self.target}")
        if self.motivation:
            annotation.append(f"motivation: {self.motivation}")
        output = f"id: {self.id}, {', '.join(annotation)}"
        return output
    
    
class EntityWithMetadata(IdentifiableEntity):
    def __init__(self, id, label="", creators = "", title = ""):
        self.title = title
        self.creators = creators
        self.label = label
        #Inheritance
        super().__init__(id)

 # Methods
    def getLabel (self):
        return self.label
    
    def getTitle (self):
        return self.title
    
    def getCreators (self):
        return self.creators
    
    def __str__(self):
        metadata = []
        if self.title:
            metadata.append(f"title: {self.title}")
        if self.creators:
            metadata.append(f"creator: {self.creators}")
        if self.label:
            metadata.append(f"label: {self.label}")
        output = f"id: {self.id}, {', '.join(metadata)}"
        return output

class Canvas(EntityWithMetadata):
    pass

class Manifest(EntityWithMetadata):
    def __init__(self, id, label, items="", creators="", title=""):
        self.items = items

        #Inheritance
        super().__init__(id, label, creators, title)

  # Method
    def getItems(self) -> List[Canvas]:
        return self.items

class Collection(EntityWithMetadata):
    def __init__(self, id, label, items="", creators="", title=""):
        self.items = items

        # Inheritance
        super().__init__(id, label, creators, title)

  # Method
    def getItems(self) -> List[Manifest]:
        return self.items

########### Processors ###########

class Processor(object):
    def __init__(self, dbPathOrUrl=""):
        self.dbPathOrUrl = dbPathOrUrl

    def getDbPathOrUrl(self):
        return self.dbPathOrUrl

    def setDbPathOrUrl(self, dbPathOrUrl:str):
        self.dbPathOrUrl = dbPathOrUrl
        return True
        
# Relational database 
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
            canvas_internal_id = ["canvas-" + str(idx) for idx in range(len(canvases))]
            canvases.insert(0, "internalId", canvas_internal_id)

            with connect(self.dbPathOrUrl) as con:
                entities_with_metadata.to_sql("EntityWithMetadata", con, if_exists="append", index=False)
                collections.to_sql("Collection", con, if_exists="append", index=False)
                manifests.to_sql("Manifest", con, if_exists="append", index=False)
                canvases.to_sql("Canvas", con, if_exists="append", index=False)
            con.commit()
            return True

        except Exception as e:
            print(f"Error while uploading data: {e}")
            
            return False
        
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

            image = annotations[["body"]]
            image_internal_id = ["image-" + str(idx) for idx in range(len(image))]
            image.insert(0, "internalId", image_internal_id)

            with connect(self.dbPathOrUrl) as con:
                annotations.to_sql("Annotation", con, if_exists="append", index=False)
                image.to_sql("Image", con, if_exists="append", index=False)
            con.commit()
            return True

        except Exception as e:
            print(f"Error while uploading data: {e}")
            return False
               
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
                Collection = URIRef("https://schema.org/Collection")
                Manifest = URIRef("https://dbpedia.org/page/Manifest")
                Canvas = URIRef("https://dbpedia.org/page/Canvas")

                # attributes related to classes
                id = URIRef("https://schema.org/identifier")
                label = URIRef("https://dbpedia.org/page/label")

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
                endpoint = self.dbPathOrUrl
                store.open((endpoint, endpoint))
                for triple in graph.triples((None, None, None)):
                    store.add(triple)
                store.close()
                return True

        except Exception as e:
            print(f"Error while uploading data: {e}")
            return False

########### Query processors ###########

class QueryProcessor(Processor):
    def __init__(self):
        super().__init__()

    def getEntityById(self, entityId):
        url_or_path = self.getDbPathOrUrl()
        df = DataFrame()
        if url_or_path.startswith('http'):
            df = pd.DataFrame(columns=['id', 'label'])
            endpoint = 'http://127.0.0.1:9999/blazegraph/sparql'
            query = """
                PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                PREFIX dbp: <https://dbpedia.org/page/>
                PREFIX schema: <https://schema.org/>

                SELECT DISTINCT ?id ?label
                WHERE {{
                    ?id schema:identifier "{0}" ;
                             dbp:label ?label .
                }}
                """.format(entityId)
            df = get(endpoint, query, True)
        else:
            with connect(url_or_path) as con:
                query = """
                        SELECT DISTINCT A.id, A.body, A.motivation,
                        EWM.id, EWM.title, EWM.creator
                        FROM EntityWithMetadata AS EWM
                        LEFT JOIN Annotation AS A
                            ON A.target = EWM.id
                        WHERE EWM.id = ?
                        """
                df = read_sql(query, con, params=[entityId])
                filtered_df = df.dropna(axis='columns')
                filtered_df = filtered_df.loc[:, filtered_df.any()]
                df = filtered_df 
        return df

# Query processor for relational database
class RelationalQueryProcessor(QueryProcessor):
    def __init__(self):
        super().__init__()

    def getAllAnnotations(self):
        with connect(self.dbPathOrUrl) as con:
            query = "SELECT DISTINCT * FROM Annotation"
            df_sql = pd.read_sql(query, con)
            return df_sql
    
    def getAllImages(self):
        with connect(self.dbPathOrUrl) as con:
            query = "SELECT DISTINCT * FROM Image"
            df_sql = pd.read_sql(query, con)
            return df_sql
        
    def getAnnotationsWithBody(self, bodyId: str):
        with connect(self.dbPathOrUrl) as con:
            query = f"SELECT DISTINCT * FROM Annotation WHERE body='{bodyId}'"
            df_sql = pd.read_sql(query, con)
            return df_sql
        
    def getAnnotationsWithBodyAndTarget(self, bodyId: str, targetId: str):
        with connect(self.dbPathOrUrl) as con:
            query = f"SELECT DISTINCT * FROM Annotation WHERE body='{bodyId}' OR target='{targetId}'"
            df_sql = pd.read_sql(query, con)
            return df_sql
        
    def getAnnotationsWithTarget(self, targetId: str):
        with connect(self.dbPathOrUrl) as con:
            query = f"SELECT DISTINCT * FROM Annotation WHERE target='{targetId}'"
            df_sql = pd.read_sql(query, con)
            return df_sql
        
    def getEntitiesWithCreator(self, creatorName: str):
        with connect(self.dbPathOrUrl) as con:
            query = f"SELECT DISTINCT * FROM EntityWithMetadata WHERE creator LIKE '%{creatorName}%'"
            df_sql = pd.read_sql(query, con)
            return df_sql

    def getEntitiesWithTitle(self, title: str):  
        with connect(self.dbPathOrUrl) as con:
            query = f"SELECT DISTINCT * FROM EntityWithMetadata WHERE title = '{title}'"
            df_sql = pd.read_sql(query, con)
            return df_sql

# Query processor for graph database
class TriplestoreQueryProcessor(QueryProcessor):
    def __init__(self):
        super().__init__()

    def getAllCanvases(self):
        query = """
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX dbp: <https://dbpedia.org/page/>
        PREFIX schema: <https://schema.org/>

        SELECT DISTINCT ?id ?label 
        WHERE {
            ?canvas rdf:type dbp:Canvas ;
                    schema:identifier ?id ;
                    dbp:label ?label .
        }
        """
        df_sparql = get(self.dbPathOrUrl, query, True)
        return df_sparql
    
    def getAllCollections(self):
        query = """
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX dbp: <https://dbpedia.org/page/>
        PREFIX schema: <https://schema.org/>

        SELECT DISTINCT ?id ?label 
        WHERE {
            ?collection rdf:type schema:Collection ;
                        schema:identifier ?id ;
                        dbp:label ?label .
        }
        """
        df_sparql = get(self.dbPathOrUrl, query, True)
        return df_sparql
    
    def getAllManifests(self):
        query = """
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX dbp: <https://dbpedia.org/page/>
        PREFIX schema: <https://schema.org/>

        SELECT DISTINCT ?id ?label 
        WHERE {
            ?manifest rdf:type dbp:Manifest ;
                        schema:identifier ?id ;
                        dbp:label ?label .
        }
        """
        df_sparql = get(self.dbPathOrUrl, query, True)
        return df_sparql     
    
    def getCanvasesInCollection(self, collectionId: str):
        query = """
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX dbp: <https://dbpedia.org/page/>
        PREFIX schema: <https://schema.org/>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    
        SELECT DISTINCT ?id ?label 
        WHERE {{
            ?collection rdf:type schema:Collection ;
                        schema:identifier "{0}" ;
                        schema:isPartOf ?manifest .
            ?manifest rdf:type dbp:Manifest ;
                        schema:isPartOf ?canvas .
            ?canvas rdf:type dbp:Canvas ;
                        schema:identifier ?id ;
                        dbp:label ?label .
        }}
        """.format(collectionId)
     
        df_sparql = get(self.dbPathOrUrl, query, True)
        return df_sparql  
           
    def getCanvasesInManifest(self, manifestId: str):
        query = """
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX dbp: <https://dbpedia.org/page/>
        PREFIX schema: <https://schema.org/>
        
        SELECT DISTINCT ?id ?label 
        WHERE {{
            ?collection rdf:type schema:Collection ;
                        schema:isPartOf ?manifest .  
            ?manifest rdf:type dbp:Manifest ;
                    schema:identifier "{0}" ;
                    schema:isPartOf ?canvas . 
            ?canvas rdf:type dbp:Canvas ;
                    schema:identifier ?id ;
                    dbp:label ?label .
        }}
        """.format(manifestId)
        df_sparql = get(self.dbPathOrUrl, query, True)
        return df_sparql    

    def getEntitiesWithLabel(self, label: str):
        query = """
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX dbp: <https://dbpedia.org/page/>
        PREFIX schema: <https://schema.org/>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

        SELECT DISTINCT ?id ?label
        WHERE {{        
            ?entity dbp:label "{0}" ;
                    dbp:label ?label ;
                    schema:identifier ?id .
        }}
        """.format(label.replace('"', '\\"'))
        df_sparql = get(self.dbPathOrUrl, query, True)
        return df_sparql        

    def getManifestsInCollection(self, collectionId: str):
        query = """
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX dbp: <https://dbpedia.org/page/>
        PREFIX schema: <https://schema.org/>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

        SELECT DISTINCT ?id ?label 
        WHERE {{
            ?collection rdf:type schema:Collection ;
                        schema:isPartOf ?manifest ;
                        schema:identifier "{0}" .
            ?manifest rdf:type dbp:Manifest ;
                        schema:identifier ?id ;
                        dbp:label ?label .
        }}
        """.format(collectionId)
        df_sparql = get(self.dbPathOrUrl, query, True)
        return df_sparql        

class GenericQueryProcessor(object):
    def __init__(self):
        self.queryProcessors = []
    
    def cleanQueryProcessors(self):
        self.queryProcessors = []
        return True
        
    def addQueryProcessor(self, processor: QueryProcessor):
        self.queryProcessors.append(processor)
        return True

    def getAllAnnotations(self) -> List[Annotation]:
        all_annotations_df = []
        all_annotations = []
        for queryProcessor in self.queryProcessors:
            if isinstance(queryProcessor, RelationalQueryProcessor):
                all_annotations_df.append(queryProcessor.getAllAnnotations())
        for annotations_df in all_annotations_df:
            for i, row in annotations_df.iterrows():
                id = row["id"]
                body = row["body"]
                target = row["target"]
                motivation = row["motivation"]
                annotation = Annotation(id, body, target, motivation)
                all_annotations.append(annotation)
        return all_annotations
    
    def getAllCanvas(self)-> List[Canvas]:
        all_canvases_df = []
        all_canvases = []
        for queryProcessor in self.queryProcessors:
            if isinstance(queryProcessor, TriplestoreQueryProcessor):
                all_canvases_df.append(queryProcessor.getAllCanvases())
        for canvas_df in all_canvases_df:
            for i, row in canvas_df.iterrows():
                id = row["id"]
                label = row["label"]
                canvas = Canvas(id, label)
                all_canvases.append(canvas)
        return  all_canvases

    def getAllCollections(self)-> List[Collection]:
        all_collections_df = []
        all_collections = []
        for queryProcessor in self.queryProcessors:
            if isinstance(queryProcessor, TriplestoreQueryProcessor):
                all_collections_df.append(queryProcessor.getAllCollections())
        for collection_df in all_collections_df:
            for i, row in collection_df.iterrows():
                id = row["id"]
                label = row["label"]
                collection = Collection(id, label)
                all_collections.append(collection)
        return  all_collections
    
    def getAllImages(self)-> List[Image]:
        all_images_df = []
        all_images = []
        for queryProcessor in self.queryProcessors:
            if isinstance(queryProcessor, RelationalQueryProcessor):
                all_images_df.append(queryProcessor.getAllImages())
        for image_df in all_images_df:
            for i, row in image_df.iterrows():
                id = row["body"]
                image = Image(id)
                all_images.append(image)
        return all_images

    def getAllManifests(self)-> List[Manifest]:
        all_manifests_df = []
        all_manifests = []
        for queryProcessor in self.queryProcessors:
            if isinstance(queryProcessor, TriplestoreQueryProcessor):
                all_manifests_df.append(queryProcessor.getAllManifests())
        for manifest_df in all_manifests_df:
            for i, row in manifest_df.iterrows():
                id = row["id"]
                label = row["label"]
                manifest = Manifest(id, label)
                all_manifests.append(manifest)
        return  all_manifests
    
    def getAnnotationsToCanvas(self, canvasId: str) -> List[Annotation]:
        annotations_to_canvas = []
        for queryProcessor in self.queryProcessors:
            if isinstance(queryProcessor, RelationalQueryProcessor):
                with connect(queryProcessor.getDbPathOrUrl()) as con:
                    query = f"SELECT DiSTINCT * FROM Annotation WHERE target ='{canvasId}'"
                    df_sq = pd.read_sql(query, con)
                    for i, row in df_sq.iterrows():
                        id = row["id"]
                        body = row["body"]
                        target = row["target"]
                        motivation = row["motivation"]
                        annotation = Annotation(id, body, target, motivation)
                        annotations_to_canvas.append(annotation)
        return annotations_to_canvas
    
    def getAnnotationsToCollection(self, collectionId: str) -> List[Annotation]:
        annotations_to_collection = []
        for queryProcessor in self.queryProcessors:
            if isinstance(queryProcessor, RelationalQueryProcessor):
                with connect(queryProcessor.getDbPathOrUrl()) as con:
                    query = f"SELECT DiSTINCT * FROM Annotation WHERE target ='{collectionId}'"
                    df_sq = pd.read_sql(query, con)
                    for i, row in df_sq.iterrows():
                        id = row["id"]
                        body = row["body"]
                        target = row["target"]
                        motivation = row["motivation"]
                        annotation = Annotation(id, body, target, motivation)
                        annotations_to_collection.append(annotation)
        return annotations_to_collection
    
    def getAnnotationsToManifest(self, manifestId: str) -> List[Annotation]:
        annotations_to_manifest = []
        for queryProcessor in self.queryProcessors:
            if isinstance(queryProcessor, RelationalQueryProcessor):
                with connect(queryProcessor.getDbPathOrUrl()) as con:
                    query = f"SELECT DiSTINCT * FROM Annotation WHERE target ='{manifestId}'"
                    df_sq = pd.read_sql(query, con)
                    for i, row in df_sq.iterrows():
                        id = row["id"]
                        body = row["body"]
                        target = row["target"]
                        motivation = row["motivation"]
                        annotation = Annotation(id, body, target, motivation)
                        annotations_to_manifest.append(annotation)
        return annotations_to_manifest
                        
    def getAnnotationsWithBody(self, bodyId: str) -> List[Annotation]:
        annotations_with_body_df = []
        annotations_with_body = []
        for queryProcessor in self.queryProcessors:
            if isinstance(queryProcessor, RelationalQueryProcessor):
                annotations_with_body_df.append(queryProcessor.getAnnotationsWithBody(bodyId))
        for annotation_df in annotations_with_body_df:
            for i, row in annotation_df.iterrows():
                id = row["id"]
                body = row["body"]
                target = row["target"]
                motivation = row["motivation"]
                annotation = Annotation(id, body, target, motivation)
                annotations_with_body.append(annotation)
        return annotations_with_body
    
    def getAnnotationsWithBodyAndTarget(self, bodyId: str, targetId : str) -> List[Annotation]:
        annotations_with_body_and_target_df = []
        annotations_with_body_and_target = []
        for queryProcessor in self.queryProcessors:
            if isinstance(queryProcessor, RelationalQueryProcessor):
                annotations_with_body_and_target_df.append(queryProcessor.getAnnotationsWithBodyAndTarget(bodyId, targetId))
        for annotation_df in annotations_with_body_and_target_df:
            for i, row in annotation_df.iterrows():
                id = row["id"]
                body = row["body"]
                target = row["target"]
                motivation = row["motivation"]
                annotation = Annotation(id, body, target, motivation)
                annotations_with_body_and_target.append(annotation)  
        return annotations_with_body_and_target
       
    def getAnnotationsWithTarget(self, targetId: str) -> List[Annotation]:
        annotations_with_target_df = []
        annotations_with_target = []
        for queryProcessor in self.queryProcessors:
            if isinstance(queryProcessor, RelationalQueryProcessor):
                annotations_with_target_df.append(queryProcessor.getAnnotationsWithTarget(targetId))
        for annotation_df in annotations_with_target_df:
            for i, row in annotation_df.iterrows():
                id = row["id"]
                body = row["body"]
                target = row["target"]
                motivation = row["motivation"]
                annotation = Annotation(id, body, target, motivation)
                annotations_with_target.append(annotation)  
        return annotations_with_target        
    
    def getCanvasesInCollection(self, collectionId: str) -> List[Canvas]:
        canvases_in_collection_df = []
        canvases_in_collection = []
        for queryProcessor in self.queryProcessors:
            if isinstance(queryProcessor, TriplestoreQueryProcessor):
                canvases_in_collection_df.append(queryProcessor.getCanvasesInCollection(collectionId))
        for canvas_in_collection_df in canvases_in_collection_df:
            for i, row in canvas_in_collection_df.iterrows():
                id = row["id"]
                label = row["label"]
                canvas = Canvas(id, label)
                canvases_in_collection.append(canvas)
        return canvases_in_collection
    
    def getCanvasesInManifest(self, manifestId: str) -> List[Canvas]:
        canvases_in_manifest_df = []
        canvases_in_manifest = []
        for queryProcessor in self.queryProcessors:
            if isinstance(queryProcessor, TriplestoreQueryProcessor):
                canvases_in_manifest_df.append(queryProcessor.getCanvasesInManifest(manifestId))
        for canvas_in_manifest_df in canvases_in_manifest_df:
            for i, row in canvas_in_manifest_df.iterrows():
                id = row["id"]
                label = row["label"]
                canvas = Canvas(id, label)
                canvases_in_manifest.append(canvas)
        return canvases_in_manifest
    
    def getEntitiesWithCreator(self, creatorName: str) -> List[EntityWithMetadata]:
        entities_with_creator_df = []
        entities_with_creator = []
        for queryProcessor in self.queryProcessors:
            if isinstance(queryProcessor, RelationalQueryProcessor):
                entities_with_creator_df.append(queryProcessor.getEntitiesWithCreator(creatorName))
        for entity_with_creator_df in entities_with_creator_df:
            for i, row in entity_with_creator_df.iterrows():
                id = row["id"]
                title = row["title"]
                creators = row["creator"]
                
                if id.endswith("collection"):
                    entity = Collection(id, label=None, title=title, creators=creators)
                elif id.endswith("manifest"):
                    entity = Manifest(id, label=None, title=title, creators=creators)
                else:
                    entity = EntityWithMetadata(id, title=title, creators=creators)

                entities_with_creator.append(entity)
        return entities_with_creator

    def getEntitiesWithLabel(self, label: str) -> List[EntityWithMetadata]:
        entities_with_label_df = []
        entities_with_label = []
        for queryProcessor in self.queryProcessors:
            if isinstance(queryProcessor, TriplestoreQueryProcessor):
                entities_with_label_df.append(queryProcessor.getEntitiesWithLabel(label))
        for entity_with_label_df in entities_with_label_df:
            for i, row in entity_with_label_df.iterrows():
                id = row["id"]
                label =  row["label"]

                if id.endswith("collection"):
                    entity = Collection(id, label=label)
                elif id.endswith("manifest"):
                    entity = Manifest(id, label=label)
                else:
                    entity = EntityWithMetadata(id, label=label)
                entities_with_label.append(entity)
        return entities_with_label

    def getEntitiesWithTitle(self, title: str) -> List[EntityWithMetadata]:
        entities_with_title_df = []
        entities_with_title = []
        for queryProcessor in self.queryProcessors:
            if isinstance(queryProcessor, RelationalQueryProcessor):
                entities_with_title_df.append(queryProcessor.getEntitiesWithTitle(title))
        for entity_with_title_df in entities_with_title_df:
            for i, row in entity_with_title_df.iterrows():
                id = row["id"]
                title = row["title"]
                creators = row["creator"]

                if id.endswith("collection"):
                    entity = Collection(id, label=None, title=title, creators=creators)
                elif id.endswith("manifest"):
                    entity = Manifest(id, label=None, title=title, creators=creators)
                else:
                    entity = EntityWithMetadata(id, title=title, creators=creators)
                entities_with_title.append(entity)
        return entities_with_title
        
    def getEntityById(self, entityId: str) -> IdentifiableEntity:
        combined_df = pd.DataFrame()  
        for processor in self.queryProcessors:
            entity = processor.getEntityById(entityId)
            if entity is not None:
                combined_df = pd.concat([combined_df, entity], ignore_index=True)
        
        if not combined_df.empty:
            merge_columns = []
            if 'body' in combined_df.columns and 'motivation' in combined_df.columns:
                merge_columns.extend(['body', 'motivation'])
            if 'title' in combined_df.columns and 'creator' in combined_df.columns:
                merge_columns.extend(['title', 'creator'])
            
            if len(merge_columns) > 0:
                merged_entity = combined_df.dropna(subset=merge_columns)
                if not merged_entity.empty:
                    merged_label = combined_df['label'].dropna().values[0]
                    merged_entity.at[0, 'label'] = merged_label
                    return merged_entity
        return None

    def getImagesAnnotatingCanvas(self, canvasId: str) -> List[Image]:
        images_annotating_canvas = []
        for queryProcessor in self.queryProcessors:
            if isinstance(queryProcessor, RelationalQueryProcessor):
                with connect(queryProcessor.getDbPathOrUrl()) as con:
                    query = f"SELECT DISTINCT * FROM Annotation WHERE target = '{canvasId}'"
                    df_sq = pd.read_sql(query, con)
                    for i, row in df_sq.iterrows():
                        id = row["body"]
                        image = Image(id)
                        images_annotating_canvas.append(image)
        return images_annotating_canvas

    def getManifestsInCollection(self, collectionId: str) -> List[Manifest]:
        manifests_in_collection_df = []
        manifests_in_collection = []
        for queryProcessor in self.queryProcessors:
            if isinstance(queryProcessor, TriplestoreQueryProcessor):
                manifests_in_collection_df.append(queryProcessor.getManifestsInCollection(collectionId))
        for manifest_in_collection_df in manifests_in_collection_df:
            for i, row in manifest_in_collection_df.iterrows():
                id = row["id"]
                label = row["label"]
                manifest = Manifest(id, label)
                manifests_in_collection.append(manifest)
        return manifests_in_collection

rel_path = "relational.db"
ann_dp = AnnotationProcessor()
ann_dp.setDbPathOrUrl(rel_path)
ann_dp.uploadData("data/annotations.csv")

met_dp = MetadataProcessor()
met_dp.setDbPathOrUrl(rel_path)
met_dp.uploadData("data/metadata.csv")

grp_endpoint = "http://127.0.0.1:9999/blazegraph/sparql"
col_dp = CollectionProcessor()
col_dp.setDbPathOrUrl(grp_endpoint)
col_dp.uploadData("data/collection-1.json")
col_dp.uploadData("data/collection-2.json")

rel_qp = RelationalQueryProcessor()
rel_qp.setDbPathOrUrl(rel_path)

grp_qp = TriplestoreQueryProcessor()
grp_qp.setDbPathOrUrl(grp_endpoint)

generic = GenericQueryProcessor()
generic.addQueryProcessor(rel_qp)
generic.addQueryProcessor(grp_qp)



