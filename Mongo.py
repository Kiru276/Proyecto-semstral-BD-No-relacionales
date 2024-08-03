from pymongo import MongoClient
import pprint

def connect_to_db():
    client = MongoClient('mongodb://localhost:27017/')  
    db = client['Proyecto-BD-NoSQL']
    collection = db['TOC-Factory-semillas']
    return collection

def consulta_1(collection):
    pipeline = [
        { "$addFields": { "Superficie": { "$replaceAll": { "input": "$Superficie", "find": ",", "replacement": "." } } } },  
        { "$project": { "Región": 1, "Superficie": { "$toDouble": "$Superficie" } } },  
        { "$group": {
            "_id": "$Región",
            "totalSuperficie": { "$sum": "$Superficie" }
        }},
        { "$sort": { "totalSuperficie": -1 } }  
    ]
    results = list(collection.aggregate(pipeline))
    return results

def consulta_2(collection):
    pipeline = [
        { "$match": { "Temporada": { "$in": ["2009-2010", "2010-2011", "2011-2012"] } } },
        { "$group": { 
            "_id": { "Especie": "$Especie", "Temporada": "$Temporada" }, 
            "count": { "$sum": 1 }
        }},
        { "$group": { 
            "_id": "$_id.Temporada",
            "especies": { 
                "$push": { 
                    "Especie": "$_id.Especie", 
                    "count": "$count" 
                } 
            }
        }},
        { "$sort": { "_id": 1 } }  # Ordena por Temporada
    ]
    results = list(collection.aggregate(pipeline))
    return results

def consulta_3(collection):
    pipeline = [
        { "$addFields": { "Superficie": { "$replaceAll": { "input": "$Superficie", "find": ",", "replacement": "." } } } },  # Reemplazar comas por puntos
        { "$project": { "Sistema": 1, "Superficie": { "$toDouble": "$Superficie" } } },  # Convertir Superficie a número
        { "$match": { "Sistema": "NACIONAL" } },
        { "$group": {
            "_id": None,
            "promedioSuperficie": { "$avg": "$Superficie" },
            "minSuperficie": { "$min": "$Superficie" },
            "maxSuperficie": { "$max": "$Superficie" },
            "totalSuperficie": { "$sum": "$Superficie" }
        }}
    ]
    results = list(collection.aggregate(pipeline))
    return results


def display_results(results):
    pp = pprint.PrettyPrinter(indent=4)
    for result in results:
        pp.pprint(result)

def main():
    collection = connect_to_db()
    while True:
        print("\nSeleccione una consulta para ejecutar:")
        print("1. Sumar Superficie cultivada total por Región")
        print("2. Obtener las distintas Especies y sus cantidades según su Temporada")
        print("3. Minimo, máximo, promedio y total de Superficie para Sistema 'NACIONAL'")
        print("4. Salir")
        
        choice = input("Ingrese el número de la consulta que desea ejecutar: ")
        
        if choice == '1':
            results = consulta_1(collection)
            print("\nConsulta 1 - Sumar Superficie cultivada total por Región:")
            display_results(results)
        elif choice == '2':
            results = consulta_2(collection)
            print("\nConsulta 2 - Obtener las distintas Especies y sus cantidades según su Temporada:")
            display_results(results)
        elif choice == '3':
            results = consulta_3(collection)
            print("\nConsulta 3 - Minimo, máximo, promedio y total de Superficie para Sistema 'NACIONAL':")
            display_results(results)
        elif choice == '4':
            print("Saliendo del programa...")
            break
        else:
            print("Opción no válida, por favor intente nuevamente.")

if __name__ == "__main__":
    main()