import requests
import pandas as pd

cookies = {
    'udi-id': UDI_ID,
    '_cc': CC,
    '_cid_cc': CID_CC,
    'marketing_vistor_id': MARKETING_VISITOR_ID,
    'uber_sites_geolocalization': UBER_SITES_GEOLOCALIZATION,
    'segmentCookie': SEGMENT_COOKIE,
    'utag_main_segment': UTAG_MAIN_SEGMENT,
    'utag_main_optimizely_segment': UTAG_MAIN_OPTIMIZELY_SEGMENT,
    'dId': DID,
    'uev2.gdprAdsConsented': UEV2_GDPR_ADS_CONSENTED,
    'uev2.gg': UEV2_GG,
    'CONSENTMGR': CONSENT_MGR,
    '_scid': SCID,
    '_fbp': FBP,
    '_tt_enable_cookie': TT_ENABLE_COOKIE,
    '_ttp': TTP,
    '_yjsu_yjad': YJSU_YJAD,
    'uev2.diningMode': UEV2_DINING_MODE,
    '_gcl_au': GCL_AU,
    '_clck': CLCK,
    '_scid_r': SCID_R,
    '_ga': GA,
    'uev2.loc': UEV2_LOC,
    'utag_main__sn': UTAG_MAIN_SN,
    '_uetvid': UETVID,
    '_ga_P1RM71MPFP': GA_P1RM71MPFP,
    'jwt-session-uem': JWT_SESSION_UEM,
    'sid': SID,
    '_ua': UA,
    'selectedRestaurant': SELECTED_RESTAURANT,
    'udi-fingerprint': UDI_FINGERPRINT,
    'user_city_ids': USER_CITY_IDS,
    'jwt-session': JWT_SESSION,
    'mp_adec770be288b16d9008c964acfba5c2_mixpanel': MP_ADEC770BE288B16D9008C964ACFBA5C2_MIXPANEL,
}

headers = {
    'accept': ACCEPT,
    'accept-language': ACCEPT_LANGUAGE,
    'content-type': CONTENT_TYPE,
    'origin': ORIGIN,
    'priority': PRIORITY,
    'referer': REFERER,
    'sec-ch-ua': SEC_CH_UA,
    'sec-ch-ua-mobile': SEC_CH_UA_MOBILE,
    'sec-ch-ua-platform': SEC_CH_UA_PLATFORM,
    'sec-fetch-dest': SEC_FETCH_DEST,
    'sec-fetch-mode': SEC_FETCH_MODE,
    'sec-fetch-site': SEC_FETCH_SITE,
    'user-agent': USER_AGENT,
    'x-csrf-token': X_CSRF_TOKEN,
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

df_final_camp = pd.DataFrame()

for restaurant in liste_resto:
    uuid = restaurant['UUID']
    nom = restaurant['Restaurant']

    cookies['selectedRestaurant'] = uuid
    json_data = {
        'restaurantUUID': uuid,
    }

    try:
        response2 = requests.post(
            'https://merchants.ubereats.com/manager/api/getCampaigns',
            params=params,
            cookies=cookies,
            headers=headers,
            json=json_data,
        )

        if response2.status_code == 200:
            data2 = response2.json().get('data')[0].get('marketingActivities')[0]  # Utilisez la réponse comme 'performances'

            campagne = {
                'restaurant': nom,
                'uuid': uuid,
                'status': data2.get('status'),
                'uuid_campagne': data2.get('campaign_uuid'),
                'budget_high': data2.get('marketingActivityDefinition').get('sponsoredListing').get('budget').get('amountE5').get('high')/100000,
                'budget_low': data2.get('marketingActivityDefinition').get('sponsoredListing').get('budget').get('amountE5').get('low')/100000,
                'spent_low': data2.get('marketingActivityDefinition').get('sponsoredListing').get('spent').get('amountE5').get('low')/100000,
                'spent_high': data2.get('marketingActivityDefinition').get('sponsoredListing').get('spent').get('amountE5').get('high')/100000,
                'isAutoBid': data2.get('marketingActivityDefinition').get('sponsoredListing').get('bid').get('isAutoBid'),
                'manualBidAmount': data2.get('marketingActivityDefinition').get('sponsoredListing').get('bid').get('manualBidAmount'),
                'roasGoal': data2.get('marketingActivityDefinition').get('sponsoredListing').get('bid').get('roasGoal'),
                'debut_campagne': data2.get('marketingActivityDefinition').get('sponsoredListing').get('schedule').get('campaignStart'),
                'fin_campagne': data2.get('marketingActivityDefinition').get('sponsoredListing').get('schedule').get('campaignEnd'),
                'name':  data2.get('marketingActivityDefinition').get('sponsoredListing').get('name'),
                'budgetUnit': data2.get('marketingActivityDefinition').get('sponsoredListing').get('budgetUnit')
            }

            df_temp_camp = pd.DataFrame([campagne])
            df_final_camp = pd.concat([df_final_camp, df_temp_camp], ignore_index=True)
            print(f"Restaurant {uuid} traité avec succès.")
        else:
            print(f"Échec du scraping pour le restaurant {uuid}. Statut: {response.status_code}")

    except Exception as e:
        print(f"Erreur lors du traitement du restaurant {uuid}: {str(e)}")
    print(f"Nombre total de lignes dans df_final_camp : {len(df_final_camp)}")

# Convertir la colonne de date en datetime
df_final_camp['debut_campagne'] = pd.to_datetime(df_final_camp['debut_campagne'], format='%Y-%m-%dT%H:%M:%SZ')
df_final_camp['fin_campagne'] = pd.to_datetime(df_final_camp['fin_campagne'], format='%Y-%m-%dT%H:%M:%SZ')

# Modifier le format d'affichage des dates
df_final_camp['debut_campagne'] = df_final_camp['debut_campagne'].dt.strftime('%d/%m/%Y')
df_final_camp['fin_campagne'] = df_final_camp['fin_campagne'].dt.strftime('%d/%m/%Y')
