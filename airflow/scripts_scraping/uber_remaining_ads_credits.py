import requests
import pandas as pd
import os
from dotenv import load_dotenv

def run_scraping():
    # Charger les variables d'environnement depuis le fichier .env
    load_dotenv()

    cookies = {
        'sid': os.getenv('SID'),
    }
    headers = {
        'x-csrf-token': os.getenv('X_CSRF_TOKEN'),
    }
    # Première requête pour obtenir les restaurants
    json_data = {
        'operationName': 'GetBrandStores',
        'variables': {},
        'query': 'fragment brandFragment on OnlineOrderingBrand {\n  uuid\n  name\n  url\n  isAdmin\n  __typename\n}\n\nfragment storeFragment on OnlineOrderingStore {\n  uuid\n  name\n  url\n  brandUUID\n  optedIn\n  __typename\n}\n\nfragment feeFragment on OnlineOrderingFee {\n  deliveryFee\n  thirdPartyDeliveryFee\n  pickupFee\n  dineInFee\n  __typename\n}\n\nquery GetBrandStores {\n  getOnlineOrderingBrandStores {\n    brands {\n      ...brandFragment\n      __typename\n    }\n    stores {\n      ...storeFragment\n      __typename\n    }\n    __typename\n  }\n  getSelectedStore {\n    fees {\n      ...feeFragment\n      __typename\n    }\n    __typename\n  }\n}\n',
    }

    response = requests.post('https://merchants.ubereats.com/manager/graphql', cookies=cookies, headers=headers, json=json_data)

    liste_resto = []
    if response.status_code == 200:
        for store in response.json()['data']['getOnlineOrderingBrandStores']['stores']:
            restau = {
                'UUID': store['uuid'],
                'Restaurant': store['name']
            }
            liste_resto.append(restau)
    else:
        print(f"Erreur lors de la récupération des restaurants: Statut: {response.status_code}")
        return pd.DataFrame()  # Retourne un DataFrame vide si la requête échoue

    # Deuxième requête pour chaque restaurant
    json_data = {
        'operationName': 'GetSpendParams',
        'variables': {
            'request': {
                'budgetUnit': 'DAILY',
            },
        },
        'query': 'query GetSpendParams($request: GetSpendParamsRequest__Input) { getSpendParams(request: $request) { availableCredits { amountE5 } availableCreditsExpirationDateTime } }'
    }

    df_final = pd.DataFrame()

    for restaurant in liste_resto:
        uuid = restaurant['UUID']
        nom = restaurant['Restaurant']
        cookies['selectedRestaurant'] = uuid

        try:
            response = requests.post('https://merchants.ubereats.com/manager/graphql', cookies=cookies, headers=headers, json=json_data)

            if response.status_code == 200:
                data = response.json().get('data').get('getSpendParams')
                ads = {
                    'Restaurant': nom,
                    'Uuid': uuid,
                    'Credits restants low': data.get('availableCredits').get('amountE5').get('low')/100000,
                    'Date expiration': data.get('availableCreditsExpirationDateTime')
                }

                df_temp = pd.DataFrame([ads])
                df_final = pd.concat([df_final, df_temp], ignore_index=True)
                print(f"Restaurant {uuid} traité avec succès.")
            else:
                print(f"Échec du scraping pour le restaurant {uuid}. Statut: {response.status_code}")

        except Exception as e:
            print(f"Erreur lors du traitement du restaurant {uuid}: {str(e)}")

    # Convertir la colonne de date en datetime
    if not df_final.empty:
        df_final['Date expiration'] = pd.to_datetime(df_final['Date expiration'], errors='coerce')
        df_final['Date expiration'] = df_final['Date expiration'].dt.strftime('%d/%m/%Y')

    return df_final

# Test de la fonction (vous pouvez supprimer cette partie si vous voulez uniquement l'importer)
if __name__ == "__main__":
    df = run_scraping()
    print(df)
