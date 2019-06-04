from py2neo import Graph

GRAPH_DB_CONN = Graph("bolt://127.0.0.1:7687", user="neo4j", password="fcktrump", secure=True)