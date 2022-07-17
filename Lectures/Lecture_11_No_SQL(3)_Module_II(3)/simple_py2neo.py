from py2neo import Graph
from py2neo.ogm import GraphObject, Property, RelatedFrom, RelatedTo


graph = Graph("bolt://localhost:7687", auth=("neo4j", "sh01dan5"))


class Movie(GraphObject):
    __primarykey__ = "title"
    title = Property()
    tagline = Property()
    released = Property()
    actors = RelatedFrom("Person", "ACTED_IN")
    directors = RelatedFrom("Person", "DIRECTED")
    producers = RelatedFrom("Person", "PRODUCED")

class Person(GraphObject):
    __primarykey__ = "name"
    name = Property()
    born = Property()
    acted_in = RelatedTo(Movie)
    directed = RelatedTo(Movie)
    produced = RelatedTo(Movie)


def t1():
    x = graph.nodes.match("Person", name="Keanu Reeves").first()
    print(type(x))
    y = Person.wrap(x)
    print(y.born)
    print(y)

def t2():
    st = Person.match(graph).where("_.born = 1960")
    print("People born in 1960 and the movies they acted in:")
    for s in st:
        print(s.name, s.born)

        for m in s.acted_in:
            print("\t", m.title, m.released)
#t1()
t2()