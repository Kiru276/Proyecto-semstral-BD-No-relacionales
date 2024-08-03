from neo4j import GraphDatabase
from dotenv import load_dotenv
import os
import pprint

# Cargar variables de entorno desde .env
load_dotenv()

class Neo4jDB:
    def __init__(self):
        uri = os.getenv("NEO4J_URI")
        user = os.getenv("NEO4J_USER")
        password = os.getenv("NEO4J_PASSWORD")
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def consulta_1(self):
        query = """
        MATCH (n:Semilla)
        WITH n, toFloat(n.Superficie) AS Superficie
        RETURN n.Región AS Región, sum(Superficie) AS totalSuperficie
        ORDER BY totalSuperficie DESC
        """
        with self.driver.session() as session:
            result = session.run(query)
            return result.values()

    def consulta_2(self):
        query = """
        MATCH (n:Semilla)
        WHERE n.Temporada IN ['2009-2010', '2010-2011', '2011-2012']
        WITH n.Especie AS Especie, n.Temporada AS Temporada, count(*) AS count
        RETURN Temporada, collect({Especie: Especie, count: count}) AS especies
        ORDER BY Temporada
        """
        with self.driver.session() as session:
            result = session.run(query)
            return result.values()

    def consulta_3(self):
        query = """
        MATCH (n:Semilla {Sistema: 'NACIONAL'})
        WITH toFloat(replace(n.Superficie, ',', '.')) AS Superficie
        RETURN avg(Superficie) AS promedioSuperficie,
               min(Superficie) AS minSuperficie,
               max(Superficie) AS maxSuperficie,
               sum(Superficie) AS totalSuperficie
        """
        with self.driver.session() as session:
            result = session.run(query)
            return [{"consulta": "consulta_3", "promedioSuperficie": record["promedioSuperficie"], 
                     "minSuperficie": record["minSuperficie"], "maxSuperficie": record["maxSuperficie"], 
                     "totalSuperficie": record["totalSuperficie"]} for record in result]

def display_results(results):
    pp = pprint.PrettyPrinter(indent=4)
    for result in results:
        pp.pprint(result)

def main():
    db = Neo4jDB()
    
    try:
        while True:
            print("\nSeleccione una consulta para ejecutar:")
            print("1. Sumar Superficie cultivada total por Región")
            print("2. Obtener las distintas Especies y sus cantidades según su Temporada")
            print("3. Minimo, máximo, promedio y total de Superficie para Sistema 'NACIONAL'")
            print("4. Salir")
            
            choice = input("Ingrese el número de la consulta que desea ejecutar: ")
            
            if choice == '1':
                results = db.consulta_1()
                print("\nConsulta 1 - Sumar Superficie cultivada total por Región:")
                display_results(results)
            elif choice == '2':
                results = db.consulta_2()
                print("\nConsulta 2 - Obtener las distintas Especies y sus cantidades según su Temporada:")
                display_results(results)
            elif choice == '3':
                results = db.consulta_3()
                print("\nConsulta 3 - Minimo, máximo, promedio y total de Superficie para Sistema 'NACIONAL':")
                display_results(results)
            elif choice == '4':
                print("Saliendo del programa...")
                break
            else:
                print("Opción no válida, por favor intente nuevamente.")
    finally:
        db.close()

if __name__ == "__main__":
    main()
