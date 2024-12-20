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

restaurants = requests.post('https://merchants.ubereats.com/manager/graphql', cookies=cookies, headers=headers, json=json_data)


liste_resto = []
for store in restaurants.json()['data']['getOnlineOrderingBrandStores']['stores']:
    restau = {}
    restau['UUID'] = store['uuid']
    restau['Restaurant'] = store['name']
    liste_resto.append(restau)
liste_resto


params = {
    'localeCode': 'fr-FR',
}

df_final = pd.DataFrame()
df_stats = pd.DataFrame()

for restaurant in liste_resto:
    uuid = restaurant['UUID']
    nom = 'Paul'

    cookies['selectedRestaurant'] = uuid
    json_data = {
        'restaurantUUID': uuid,
    }

    try:
        response = requests.post(
            'https://merchants.ubereats.com/manager/api/getAllPromotionStatistics',
            params=params,
            cookies=cookies,
            headers=headers,
            json=json_data,
        )

         # Vérifier si la requête a réussi
        if response.status_code == 200:
            performances = response

            df_temp = []
            for donnees in performances.json().get('data'):
                df_promo = {}
                df_promo['UuidRestaurant'] = uuid
                df_promo['NomRestaurant'] = nom
                df_promo['TypePromo'] = donnees.get('promoType')
                df_promo['UuidPromo'] = donnees.get('uuid')
                df_promo['StatutPromo'] = donnees.get('state')
                df_promo['DateDebut'] = donnees.get('date').get('min')
                df_promo['DateFin'] = donnees.get('date').get('max')
                df_promo['NouveauClient'] = donnees.get('newCustomerCount')
                df_promo['Audience'] = donnees.get('audience')
                df_promo['Ventes'] = donnees.get('sales')
                df_promo['BudgetUtilise'] = donnees.get('budgetUsed')
                df_promo['Redemptions'] = donnees.get('totalRedemptions')
                df_promo['Stats'] = donnees.get('dailyPromotionStatistics')
                df_promo['DateCalculStats'] = donnees.get('statisticsComputedAt')
                df_promo['FinancementPromo'] = donnees.get('funding')
                df_promo['UuidCampagne'] = donnees.get('campaignUUID')
                df_promo['marketingExperienceType'] = donnees.get('marketingExperienceType')
                df_promo['TypeOptionPromo'] = donnees.get('promotionOption').get('type')
                TypeOptionPromo = donnees.get('promotionOption').get('type')

                if TypeOptionPromo == 'percentOffSubtotalPromotionOption':
                    df_promo['PercentOptionPromo'] = donnees.get('promotionOption').get(TypeOptionPromo).get('percent')
                    df_promo['LowValeurMaxOptionPromo'] = donnees.get('promotionOption').get(TypeOptionPromo).get('maxValue').get('amountE5').get('low')
                    df_promo['HighValeurMaxOptionPromo'] = donnees.get('promotionOption').get(TypeOptionPromo).get('maxValue').get('amountE5').get('high')
                    df_promo['ContrainteTempsOptionPromo'] = donnees.get('promotionOption').get(TypeOptionPromo).get('timeConstraint')
                    df_promo['minimumBasketSizeOptionPromo'] = donnees.get('promotionOption').get(TypeOptionPromo).get('minimumBasketSize')
                    df_promo['items'] = None
                    df_promo['amount'] = None
                    df_promo['Uuid article'] = None
                    df_promo['quantity'] = None

                elif TypeOptionPromo == 'discountDeliveryPromotionOption':
                    df_promo['PercentOptionPromo'] = donnees.get('promotionOption').get(TypeOptionPromo).get('percent')
                    df_promo['LowValeurMaxOptionPromo'] = donnees.get('promotionOption').get(TypeOptionPromo).get('discountValue').get('amountE5').get('low')
                    df_promo['HighValeurMaxOptionPromo'] = donnees.get('promotionOption').get(TypeOptionPromo).get('discountValue').get('amountE5').get('high')
                    df_promo['ContrainteTempsOptionPromo'] = None
                    df_promo['minimumBasketSizeOptionPromo'] = donnees.get('promotionOption').get(TypeOptionPromo).get('minimumBasketSize')
                    df_promo['items'] = None
                    df_promo['amount'] = None
                    df_promo['Uuid article'] = None
                    df_promo['quantity'] = None

                elif TypeOptionPromo == 'discountedItemPromotionOption':
                    df_promo['PercentOptionPromo'] = None
                    df_promo['LowValeurMaxOptionPromo'] = None
                    df_promo['HighValeurMaxOptionPromo'] = None
                    df_promo['ContrainteTempsOptionPromo'] = None
                    df_promo['minimumBasketSizeOptionPromo'] = None
                    df_promo['items'] = donnees.get('promotionOption').get(TypeOptionPromo).get('items')
                    df_promo['amount'] = donnees.get('promotionOption').get(TypeOptionPromo).get('amount')
                    df_promo['Uuid article'] = []
                    df_promo['quantity'] = []
                    item_discounts = donnees.get('promotionOption').get(TypeOptionPromo).get('itemDiscounts')
                    if item_discounts and isinstance(item_discounts, list):
                        if len(item_discounts) == 1 and 'items' in item_discounts[0]:
                        # Cas où il y a une seule liste d'items
                            for item in item_discounts[0].get('items', []):
                                uuid_item = item.get('skuUUID')
                                df_promo['Uuid article'].append(uuid_item)
                                quantity_item = item.get('quantity')
                                df_promo['quantity'].append(quantity_item)
                        else:
                            # Cas où chaque discount a un seul item
                            for discount in item_discounts:
                                for item in discount.get('items', []):
                                    uuid_item = item.get('skuUUID')
                                    df_promo['Uuid article'].append(uuid_item)
                                    quantity_item = item.get('quantity')
                                    df_promo['quantity'].append(quantity_item)

                elif TypeOptionPromo == 'flatOffSubtotalPromotionOption':
                    df_promo['PercentOptionPromo'] = None
                    df_promo['LowValeurMaxOptionPromo'] = donnees.get('promotionOption').get(TypeOptionPromo).get('promotionValue').get('amountE5').get('low')
                    df_promo['HighValeurMaxOptionPromo'] = donnees.get('promotionOption').get(TypeOptionPromo).get('promotionValue').get('amountE5').get('high')
                    df_promo['ContrainteTempsOptionPromo'] = donnees.get('promotionOption').get(TypeOptionPromo).get('timeConstraint')
                    df_promo['lowMinimumBasketSizeOptionPromo'] = donnees.get('promotionOption').get(TypeOptionPromo).get('minimumBasketSize').get('amountE5').get('low')
                    df_promo['highMinimumBasketSizeOptionPromo'] = donnees.get('promotionOption').get(TypeOptionPromo).get('minimumBasketSize').get('amountE5').get('high')
                    df_promo['items'] = None
                    df_promo['amount'] = None
                    df_promo['Uuid article'] = None
                    df_promo['quantity'] = None

                df_temp.append(df_promo)

            df_temp_final = pd.DataFrame(df_temp)

            stats_combined = []
            for element in performances.json().get('data'):
                UuidPromo = element.get('uuid')
                for date in element.get('dailyPromotionStatistics'):
                    info = {}
                    info['Date'] = date.get('date')
                    info['Ventes'] = date.get('sales')
                    info['Uuid promo'] = UuidPromo
                    info['Uuid restaurant'] = uuid
                    info['Nom restaurant'] = nom
                    stats_combined.append(info)
            df_temp_stats = pd.DataFrame(stats_combined)

                # Agréger les résultats
            df_final = pd.concat([df_final, df_temp_final], ignore_index=True)
            df_stats = pd.concat([df_stats, df_temp_stats], ignore_index=True)

            print(f"Restaurant {uuid} traité avec succès.")
        else:
            print(f"Échec du scraping pour le restaurant {uuid}. Statut: {response.status_code}")

    except Exception as e:
        print(f"Erreur lors du traitement du restaurant {uuid}: {str(e)}")
    print(f"Nombre total de lignes dans df_final : {len(df_final)}")
    print(f"Nombre total de lignes dans df_stats : {len(df_stats)}")
