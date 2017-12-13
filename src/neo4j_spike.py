"""Neo4j test sender"""

from neo4j.v1 import GraphDatabase

uri = "bolt://b3986.byod.hpi.de:7687"
driver = GraphDatabase.driver(uri)

def add_friends(tx, name, friend_name):
    tx.run("MERGE (a:Person {name: $name}) "
           "MERGE (a)-[:KNOWS]->(friend:Person {name: $friend_name})",
           name=name, friend_name=friend_name)

def print_friends(tx, name):
    for record in tx.run("MATCH (a:Person)-[:KNOWS]->(friend) WHERE a.name = $name "
                         "RETURN friend.name ORDER BY friend.name", name=name):
        print(record["friend.name"])

with driver.session() as session:
    session.write_transaction(add_friends, "Arthur", "Ich")
    session.write_transaction(add_friends, "Arthur", "teste")
    session.write_transaction(add_friends, "Arthur", "gern")
    session.read_transaction(print_friends, "Arthur")
