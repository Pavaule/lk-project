import requests
import pandas as pd
import os
from dotenv import load_dotenv

# Charger les variables d'environnement depuis le fichier .env
load_dotenv()

cookies = {
    'udi-id': os.getenv('UDI_ID'),
    '_cc': os.getenv('CC'),
    '_cid_cc': os.getenv('CID_CC'),
    'marketing_vistor_id': os.getenv('MARKETING_VISITOR_ID'),
    'uber_sites_geolocalization': os.getenv('UBER_SITES_GEOLOCALIZATION'),
    'segmentCookie': os.getenv('SEGMENT_COOKIE'),
    'utag_main_segment': os.getenv('UTAG_MAIN_SEGMENT'),
    'utag_main_optimizely_segment': os.getenv('UTAG_MAIN_OPTIMIZELY_SEGMENT'),
    'dId': os.getenv('DID'),
    'uev2.gdprAdsConsented': os.getenv('UEV2_GDPR_ADS_CONSENTED'),
    'uev2.gg': os.getenv('UEV2_GG'),
    'CONSENTMGR': os.getenv('CONSENT_MGR'),
    '_scid': os.getenv('SCID'),
    '_fbp': os.getenv('FBP'),
    '_tt_enable_cookie': os.getenv('TT_ENABLE_COOKIE'),
    '_ttp': os.getenv('TTP'),
    '_yjsu_yjad': os.getenv('YJSU_YJAD'),
    'uev2.diningMode': os.getenv('UEV2_DINING_MODE'),
    '_gcl_au': os.getenv('GCL_AU'),
    '_clck': os.getenv('CLCK'),
    '_scid_r': os.getenv('SCID_R'),
    '_ga': os.getenv('GA'),
    'uev2.loc': os.getenv('UEV2_LOC'),
    'utag_main__sn': os.getenv('UTAG_MAIN_SN'),
    '_uetvid': os.getenv('UETVID'),
    '_ga_P1RM71MPFP': os.getenv('GA_P1RM71MPFP'),
    'jwt-session-uem': os.getenv('JWT_SESSION_UEM'),
    'sid': os.getenv('SID'),
    '_ua': os.getenv('UA'),
    'selectedRestaurant': os.getenv('SELECTED_RESTAURANT'),
    'udi-fingerprint': os.getenv('UDI_FINGERPRINT'),
    'user_city_ids': os.getenv('USER_CITY_IDS'),
    'jwt-session': os.getenv('JWT_SESSION'),
    'mp_adec770be288b16d9008c964acfba5c2_mixpanel': os.getenv('MP_ADEC770BE288B16D9008C964ACFBA5C2_MIXPANEL'),
}

headers = {
    'accept': os.getenv('ACCEPT'),
    'accept-language': os.getenv('ACCEPT_LANGUAGE'),
    'content-type': os.getenv('CONTENT_TYPE'),
    'origin': os.getenv('ORIGIN'),
    'priority': os.getenv('PRIORITY'),
    'referer': os.getenv('REFERER'),
    'sec-ch-ua': os.getenv('SEC_CH_UA'),
    'sec-ch-ua-mobile': os.getenv('SEC_CH_UA_MOBILE'),
    'sec-ch-ua-platform': os.getenv('SEC_CH_UA_PLATFORM'),
    'sec-fetch-dest': os.getenv('SEC_FETCH_DEST'),
    'sec-fetch-mode': os.getenv('SEC_FETCH_MODE'),
    'sec-fetch-site': os.getenv('SEC_FETCH_SITE'),
    'user-agent': os.getenv('USER_AGENT'),
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


json_data = {
    'operationName': 'GetSpendParams',
    'variables': {
        'request': {
            'budgetUnit': 'DAILY',
        },
    },
    'query': 'query GetSpendParams($request: GetSpendParamsRequest__Input) {\n  getSpendParams(request: $request) {\n    availableCredits {\n      currencyCode\n      amountE5\n      __typename\n    }\n    availableCreditsExpirationDateTime\n    defaultBudget {\n      lowWeekly {\n        amountE5\n        currencyCode\n        __typename\n      }\n      midWeekly {\n        amountE5\n        currencyCode\n        __typename\n      }\n      highWeekly {\n        amountE5\n        currencyCode\n        __typename\n      }\n      maxWeekly {\n        amountE5\n        currencyCode\n        __typename\n      }\n      minWeekly {\n        amountE5\n        currencyCode\n        __typename\n      }\n      min {\n        amountE5\n        currencyCode\n        __typename\n      }\n      mid {\n        amountE5\n        currencyCode\n        __typename\n      }\n      max {\n        amountE5\n        currencyCode\n        __typename\n      }\n      low {\n        amountE5\n        currencyCode\n        __typename\n      }\n      high {\n        amountE5\n        currencyCode\n        __typename\n      }\n      extraHigh {\n        amountE5\n        currencyCode\n        __typename\n      }\n      budgetUnit\n      __typename\n    }\n    defaultBid {\n      minBid {\n        amountE5\n        currencyCode\n        __typename\n      }\n      maxBid {\n        amountE5\n        currencyCode\n        __typename\n      }\n      __typename\n    }\n    __typename\n  }\n}\n',
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
                'Credits restants high': data.get('availableCredits').get('amountE5').get('high')/100000,
                'Date expiration': data.get('availableCreditsExpirationDateTime')
            }

            df_temp = pd.DataFrame([ads])
            df_final = pd.concat([df_final, df_temp], ignore_index=True)
            print(f"Restaurant {uuid} traité avec succès.")
        else:
            print(f"Échec du scraping pour le restaurant {uuid}. Statut: {response.status_code}")

    except Exception as e:
        print(f"Erreur lors du traitement du restaurant {uuid}: {str(e)}")
    print(f"Nombre total de lignes dans df_final : {len(df_final)}")

# Convertir la colonne de date en datetime
df_final['Date expiration'] = pd.to_datetime(df_final['Date expiration'], format='%Y-%m-%dT%H:%M:%SZ')

# Modifier le format d'affichage des dates
df_final['Date expiration'] = df_final['Date expiration'].dt.strftime('%d/%m/%Y')
