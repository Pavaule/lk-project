import requests
import pandas as pd
import os
from dotenv import load_dotenv

# Charger les variables d'environnement depuis le fichier .env
load_dotenv()

cookies = {
        'sid': os.getenv('SID'),
    }
headers = {
        'x-csrf-token': os.getenv('X_CSRF_TOKEN'),
    }

json_data = {
    'operationName': 'GetBrandStores',
    'variables': {},
    'query': 'fragment brandFragment on OnlineOrderingBrand {\n  uuid\n  name\n  url\n  isAdmin\n  __typename\n}\n\nfragment storeFragment on OnlineOrderingStore {\n  uuid\n  name\n  url\n  brandUUID\n  optedIn\n  __typename\n}\n\nfragment feeFragment on OnlineOrderingFee {\n  deliveryFee\n  thirdPartyDeliveryFee\n  pickupFee\n  dineInFee\n  __typename\n}\n\nquery GetBrandStores {\n  getOnlineOrderingBrandStores {\n    brands {\n      ...brandFragment\n      __typename\n    }\n    stores {\n      ...storeFragment\n      __typename\n    }\n    __typename\n  }\n  getSelectedStore {\n    fees {\n      ...feeFragment\n      __typename\n    }\n    __typename\n  }\n}\n',
}

params = {
    'localeCode': 'fr-FR',
}

restaurants = requests.post('https://merchants.ubereats.com/manager/graphql', cookies=cookies, headers=headers, json=json_data)

liste_resto = []
for store in restaurants.json()['data']['getOnlineOrderingBrandStores']['stores']:
    restau = {}
    restau['UUID'] = store['uuid']
    restau['Restaurant'] = store['name']
    liste_resto.append(restau)
liste_resto

df = pd.DataFrame()

for restaurant in liste_resto:
    uuid = restaurant['UUID']
    nom = 'paul'

    cookies['selectedRestaurant'] = uuid
    json_data = {
        'restaurantUUID': uuid,
        'startDay': '2024-10-17',
        'granularity': 'DAILY',
    }

    try:
        response3 = requests.post(
            'https://merchants.ubereats.com/manager/api/fetchAdsMetrics',
            params=params,
            cookies=cookies,
            headers=headers,
            json=json_data,
        )

        if response3.status_code == 200:
            for date_str, metrics in response3.json().get('data').get('granularMetrics').items():
                element = {}
                element['restaurant'] = nom
                element['uuid'] = uuid
                element['date'] = date_str
                element['impressions low'] = metrics.get('impressions').get('low')
                element['impressions high'] = metrics.get('impressions').get('high')
                element['impression unsigned'] = metrics.get('impressions').get('unsigned')
                element['click low'] = metrics.get('clicks').get('low')
                element['click high'] = metrics.get('clicks').get('high')
                element['click unsigned'] = metrics.get('clicks').get('unsigned')
                element['total commande'] = metrics.get('totalOrders')
                element['commande après click'] = metrics.get('ordersAfterClick')
                element['bookingValueTotal_low'] = metrics.get('bookingValueTotal').get('amountE5').get('low') / 100000
                element['bookingValueTotal_high'] = metrics.get('bookingValueTotal').get('amountE5').get('high') / 100000
                element['bookingValueAfterClick_low'] = metrics.get('bookingValueAfterClick').get('amountE5').get('low') / 100000
                element['bookingValueAfterClick_high'] = metrics.get('bookingValueAfterClick').get('amountE5').get('high') / 100000
                element['adSpend_low'] = metrics.get('adSpend').get('amountE5').get('low') / 100000
                element['adSpend_high'] = metrics.get('adSpend').get('amountE5').get('high') / 100000
                element['roasAfterClick'] = metrics.get('roasAfterClick')
                element['roasAfterImpression'] = metrics.get('roasAfterImpression')
                element['avgCostPerClick_low'] = metrics.get('avgCostPerClick').get('amountE5').get('low') / 100000
                element['avgCostPerClick_high'] = metrics.get('avgCostPerClick').get('amountE5').get('high') / 100000
                element['clickThroughRate'] = metrics.get('clickThroughRate')
                element['budgetUtilization'] = metrics.get('budgetUtilization')
                element['totalOrdersOnOrderDate_low'] = metrics.get('totalOrdersOnOrderDate').get('low')
                element['totalOrdersOnOrderDate_high'] = metrics.get('totalOrdersOnOrderDate').get('high')
                element['ordersAfterClickOnOrderDate_low'] = metrics.get('ordersAfterClickOnOrderDate').get('low')
                element['ordersAfterClickOnOrderDate_high'] = metrics.get('ordersAfterClickOnOrderDate').get('high')
                element['bookingValueTotalOnOrderDate_low'] = metrics.get('bookingValueTotalOnOrderDate').get('amountE5').get('low') / 100000
                element['bookingValueTotalOnOrderDate_high'] = metrics.get('bookingValueTotalOnOrderDate').get('amountE5').get('high') / 100000
                element['bookingValueAfterClickOnOrderDate_low'] = metrics.get('bookingValueAfterClickOnOrderDate').get('amountE5').get('low') / 100000
                element['bookingValueAfterClickOnOrderDate_high'] = metrics.get('bookingValueAfterClickOnOrderDate').get('amountE5').get('high') / 100000
                df_temp_element = pd.DataFrame([element])
                df = pd.concat([df, df_temp_element], ignore_index=True)

            print(f"Restaurant {uuid} traité avec succès.")
        else:
            print(f"Échec du scraping pour le restaurant {uuid}. Statut: {response3.status_code}")

    except Exception as e:
        print(f"Erreur lors du traitement du restaurant {uuid}: {str(e)}")
    print(f"Nombre total de lignes dans df_final_camp : {len(df)}")
