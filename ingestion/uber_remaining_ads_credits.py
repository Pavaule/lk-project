import requests
import pandas as pd

cookies = {
    'udi-id': 'kwXouyFvFrJNRXv8uayOrdhqJnwUE2PU8jyU4FVtXORTH3vMSIugyKt7ByMXqzvo5zHCpltlykvF+9oXiPEY9gnoS9iDzwKeo12vZVN3sCfiSUKfGgqmMurDZsPQd3tMMH1xmxI1XcZATh+Vos+qNLVNxpgjucgEA4i9f5SS7ZSBS4x8NUffvI9A4vv6ahyFHEXxTYZyhyVbl1KGHMlnBg==6Vgud5x3mnM4lwaDaoaZhQ==WfAcPKsbA4B1wN6/e5PaFpwByUWCEOz0WCHPrzW2+Ik=',
    '_cc': 'AQuD3DHkAaVIzc4gtKNHXT7q',
    '_cid_cc': 'AQuD3DHkAaVIzc4gtKNHXT7q',
    'marketing_vistor_id': 'facee8d3-6729-4a64-b7c8-a1597dad3f68',
    'uber_sites_geolocalization': '{%22best%22:{%22localeCode%22:%22fr-FR%22%2C%22countryCode%22:%22FR%22%2C%22territoryId%22:51%2C%22territorySlug%22:%22lyon%22%2C%22territoryName%22:%22Lyon%22}%2C%22url%22:{%22localeCode%22:%22fr-FR%22%2C%22countryCode%22:%22FR%22}%2C%22user%22:{%22countryCode%22:%22FR%22%2C%22territoryId%22:51%2C%22territoryGeoJson%22:[[{%22lat%22:47.1554108%2C%22lng%22:3.6228018}%2C{%22lat%22:47.1554108%2C%22lng%22:6.3585501}%2C{%22lat%22:44.6969643%2C%22lng%22:6.3585501}%2C{%22lat%22:44.6969643%2C%22lng%22:3.6228018}]]%2C%22territoryGeoPoint%22:{%22latitude%22:45.764%2C%22longitude%22:4.8357}%2C%22territorySlug%22:%22lyon%22%2C%22territoryName%22:%22Lyon%22%2C%22localeCode%22:%22fr-FR%22}}',
    'segmentCookie': 'b',
    'utag_main_segment': 'a',
    'utag_main_optimizely_segment': 'a',
    'dId': '0cb9705c-710c-45fb-aace-eabee593c9e2',
    'uev2.gdprAdsConsented': 'true',
    'uev2.gg': 'true',
    'CONSENTMGR': 'c1:1%7Cc2:1%7Cc3:1%7Cc4:1%7Cc5:1%7Cc6:1%7Cc7:1%7Cc8:1%7Cc9:1%7Cc10:1%7Cc11:1%7Cc12:1%7Cc13:1%7Cc14:1%7Cc15:1%7Cts:1713259230208%7Cconsent:true',
    '_scid': '4404be9b-22eb-48d6-9be1-f14ed447a56e',
    '_fbp': 'fb.1.1713259230933.1079323249',
    '_tt_enable_cookie': '1',
    '_ttp': 'nxeo9rO7DZKIwBWgOMjjxxuJzQy',
    '_yjsu_yjad': '1713259231.213846e4-eddd-4bbc-b06a-567142350f1c',
    'uev2.diningMode': 'DELIVERY',
    '_gcl_au': '1.1.301575419.1721048191',
    '_clck': '18j3pnf%7C2%7Cfpm%7C0%7C1677',
    '_scid_r': 'udlEBL6bIuvZ1jfh8U7UR6VuAI2ltntfvDbhjA',
    '_ga': 'GA1.2.1700777215.1713259231',
    'uev2.loc': '%7B%22address%22%3A%7B%22address1%22%3A%2235%20Rue%20Saint-Fran%C3%A7ois%20R%C3%A9gis%22%2C%22address2%22%3A%22Le%20Puy-en-Velay%22%2C%22aptOrSuite%22%3A%22%22%2C%22eaterFormattedAddress%22%3A%2235%20Rue%20Saint-Fran%C3%A7ois%20R%C3%A9gis%2C%2043000%20Le%20Puy-en-Velay%2C%20France%22%2C%22subtitle%22%3A%22Le%20Puy-en-Velay%22%2C%22title%22%3A%2235%20Rue%20Saint-Fran%C3%A7ois%20R%C3%A9gis%22%2C%22uuid%22%3A%22%22%7D%2C%22latitude%22%3A45.0439255%2C%22longitude%22%3A3.886289599999999%2C%22reference%22%3A%22ChIJO50FY1b69UcR1sKtVfc0QFg%22%2C%22referenceType%22%3A%22google_places%22%2C%22type%22%3A%22google_places%22%2C%22addressComponents%22%3A%7B%22city%22%3A%22%22%2C%22countryCode%22%3A%22FR%22%2C%22firstLevelSubdivisionCode%22%3A%22Auvergne-Rh%C3%B4ne-Alpes%22%2C%22postalCode%22%3A%2243000%22%7D%2C%22categories%22%3A%5B%22address_point%22%5D%2C%22originType%22%3A%22user_autocomplete%22%7D',
    'utag_main__sn': '31',
    '_uetvid': '91f46780fbd211eebf6d199e4aa82597',
    '_ga_P1RM71MPFP': 'GS1.1.1727725471.34.0.1727725471.60.0.0',
    'jwt-session-uem': 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE3Mjc5NTY3NzAsImV4cCI6MTcyODU2MTU3MH0.c5AwOxNr4bkBlepc9a9qEFhN6rb3Q-Ly8f0MiV6TayY',
    'sid': 'QA.CAESEGMvmeD090wZuiQUz63RqHkYopauuQYiATEqJDVjMWY4YTUyLWNkZGMtNDZjNC04MGQ0LTc1OGM4MzNiNWM1ZDI8sxbiQBpMTzfAULUrVkkZuqGBCE5lyedjPpEvEKKT1-8nZ_ivA5c-orKnol-sYzL3a6p08SsaGoJ5-cITOgExQg0udWJlcmVhdHMuY29t.9r76owhZ-JaEIkAXhgGsMojKG36T5QoLKRjuuY0mFm8',
    '_ua': '{"session_id":"6ff9fb1d-4a15-4b20-9f78-b32aceaa78b9","session_time_ms":1728423045093}',
    'selectedRestaurant': 'e37f5ac6-c118-5a8d-9ef0-999112d2ec79',
    'udi-fingerprint': 'qn6ZvZUcPyEh5f6rmniFIZNl2PzpQuxZVTpCr+YMUSSE6R3ltVxZBUVBe6qLFFEro1GVYxjga6YvixOw3Z8Vaw==3R1DNQ5itY44KWKu9gidHopiJitlTxt9AYqGsIsSJ0o=',
    'user_city_ids': '51',
    'jwt-session': 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJkYXRhIjp7InNsYXRlLWV4cGlyZXMtYXQiOjE3Mjg0MjQ4NTIxNzV9LCJpYXQiOjE3Mjg0MTM4NDcsImV4cCI6MTcyODUwMDI0N30.UTqxLgUmEir_eWQ3gFwEkQ3Gf5hb0h4X3eLRxUETf4Q',
    'mp_adec770be288b16d9008c964acfba5c2_mixpanel': '%7B%22distinct_id%22%3A%20%225c1f8a52-cddc-46c4-80d4-758c833b5c5d%22%2C%22%24device_id%22%3A%20%2218eaa11ecf92ac-030a85e51eb0cd-26001a51-1fa400-18eaa11ecfab6a%22%2C%22%24initial_referrer%22%3A%20%22https%3A%2F%2Fauth.uber.com%2F%22%2C%22%24initial_referring_domain%22%3A%20%22auth.uber.com%22%2C%22%24user_id%22%3A%20%225c1f8a52-cddc-46c4-80d4-758c833b5c5d%22%7D',
}

headers = {
    'accept': '*/*',
    'accept-language': 'fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7',
    'content-type': 'application/json',
    # 'cookie': 'udi-id=kwXouyFvFrJNRXv8uayOrdhqJnwUE2PU8jyU4FVtXORTH3vMSIugyKt7ByMXqzvo5zHCpltlykvF+9oXiPEY9gnoS9iDzwKeo12vZVN3sCfiSUKfGgqmMurDZsPQd3tMMH1xmxI1XcZATh+Vos+qNLVNxpgjucgEA4i9f5SS7ZSBS4x8NUffvI9A4vv6ahyFHEXxTYZyhyVbl1KGHMlnBg==6Vgud5x3mnM4lwaDaoaZhQ==WfAcPKsbA4B1wN6/e5PaFpwByUWCEOz0WCHPrzW2+Ik=; _cc=AQuD3DHkAaVIzc4gtKNHXT7q; _cid_cc=AQuD3DHkAaVIzc4gtKNHXT7q; marketing_vistor_id=facee8d3-6729-4a64-b7c8-a1597dad3f68; uber_sites_geolocalization={%22best%22:{%22localeCode%22:%22fr-FR%22%2C%22countryCode%22:%22FR%22%2C%22territoryId%22:51%2C%22territorySlug%22:%22lyon%22%2C%22territoryName%22:%22Lyon%22}%2C%22url%22:{%22localeCode%22:%22fr-FR%22%2C%22countryCode%22:%22FR%22}%2C%22user%22:{%22countryCode%22:%22FR%22%2C%22territoryId%22:51%2C%22territoryGeoJson%22:[[{%22lat%22:47.1554108%2C%22lng%22:3.6228018}%2C{%22lat%22:47.1554108%2C%22lng%22:6.3585501}%2C{%22lat%22:44.6969643%2C%22lng%22:6.3585501}%2C{%22lat%22:44.6969643%2C%22lng%22:3.6228018}]]%2C%22territoryGeoPoint%22:{%22latitude%22:45.764%2C%22longitude%22:4.8357}%2C%22territorySlug%22:%22lyon%22%2C%22territoryName%22:%22Lyon%22%2C%22localeCode%22:%22fr-FR%22}}; segmentCookie=b; utag_main_segment=a; utag_main_optimizely_segment=a; dId=0cb9705c-710c-45fb-aace-eabee593c9e2; uev2.gdprAdsConsented=true; uev2.gg=true; CONSENTMGR=c1:1%7Cc2:1%7Cc3:1%7Cc4:1%7Cc5:1%7Cc6:1%7Cc7:1%7Cc8:1%7Cc9:1%7Cc10:1%7Cc11:1%7Cc12:1%7Cc13:1%7Cc14:1%7Cc15:1%7Cts:1713259230208%7Cconsent:true; _scid=4404be9b-22eb-48d6-9be1-f14ed447a56e; _fbp=fb.1.1713259230933.1079323249; _tt_enable_cookie=1; _ttp=nxeo9rO7DZKIwBWgOMjjxxuJzQy; _yjsu_yjad=1713259231.213846e4-eddd-4bbc-b06a-567142350f1c; uev2.diningMode=DELIVERY; _gcl_au=1.1.301575419.1721048191; _clck=18j3pnf%7C2%7Cfpm%7C0%7C1677; _scid_r=udlEBL6bIuvZ1jfh8U7UR6VuAI2ltntfvDbhjA; _ga=GA1.2.1700777215.1713259231; uev2.loc=%7B%22address%22%3A%7B%22address1%22%3A%2235%20Rue%20Saint-Fran%C3%A7ois%20R%C3%A9gis%22%2C%22address2%22%3A%22Le%20Puy-en-Velay%22%2C%22aptOrSuite%22%3A%22%22%2C%22eaterFormattedAddress%22%3A%2235%20Rue%20Saint-Fran%C3%A7ois%20R%C3%A9gis%2C%2043000%20Le%20Puy-en-Velay%2C%20France%22%2C%22subtitle%22%3A%22Le%20Puy-en-Velay%22%2C%22title%22%3A%2235%20Rue%20Saint-Fran%C3%A7ois%20R%C3%A9gis%22%2C%22uuid%22%3A%22%22%7D%2C%22latitude%22%3A45.0439255%2C%22longitude%22%3A3.886289599999999%2C%22reference%22%3A%22ChIJO50FY1b69UcR1sKtVfc0QFg%22%2C%22referenceType%22%3A%22google_places%22%2C%22type%22%3A%22google_places%22%2C%22addressComponents%22%3A%7B%22city%22%3A%22%22%2C%22countryCode%22%3A%22FR%22%2C%22firstLevelSubdivisionCode%22%3A%22Auvergne-Rh%C3%B4ne-Alpes%22%2C%22postalCode%22%3A%2243000%22%7D%2C%22categories%22%3A%5B%22address_point%22%5D%2C%22originType%22%3A%22user_autocomplete%22%7D; utag_main__sn=31; _uetvid=91f46780fbd211eebf6d199e4aa82597; _ga_P1RM71MPFP=GS1.1.1727725471.34.0.1727725471.60.0.0; jwt-session-uem=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE3Mjc5NTY3NzAsImV4cCI6MTcyODU2MTU3MH0.c5AwOxNr4bkBlepc9a9qEFhN6rb3Q-Ly8f0MiV6TayY; sid=QA.CAESEGMvmeD090wZuiQUz63RqHkYopauuQYiATEqJDVjMWY4YTUyLWNkZGMtNDZjNC04MGQ0LTc1OGM4MzNiNWM1ZDI8sxbiQBpMTzfAULUrVkkZuqGBCE5lyedjPpEvEKKT1-8nZ_ivA5c-orKnol-sYzL3a6p08SsaGoJ5-cITOgExQg0udWJlcmVhdHMuY29t.9r76owhZ-JaEIkAXhgGsMojKG36T5QoLKRjuuY0mFm8; _ua={"session_id":"6ff9fb1d-4a15-4b20-9f78-b32aceaa78b9","session_time_ms":1728423045093}; selectedRestaurant=e37f5ac6-c118-5a8d-9ef0-999112d2ec79; udi-fingerprint=qn6ZvZUcPyEh5f6rmniFIZNl2PzpQuxZVTpCr+YMUSSE6R3ltVxZBUVBe6qLFFEro1GVYxjga6YvixOw3Z8Vaw==3R1DNQ5itY44KWKu9gidHopiJitlTxt9AYqGsIsSJ0o=; user_city_ids=51; jwt-session=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJkYXRhIjp7InNsYXRlLWV4cGlyZXMtYXQiOjE3Mjg0MjQ4NTIxNzV9LCJpYXQiOjE3Mjg0MTM4NDcsImV4cCI6MTcyODUwMDI0N30.UTqxLgUmEir_eWQ3gFwEkQ3Gf5hb0h4X3eLRxUETf4Q; mp_adec770be288b16d9008c964acfba5c2_mixpanel=%7B%22distinct_id%22%3A%20%225c1f8a52-cddc-46c4-80d4-758c833b5c5d%22%2C%22%24device_id%22%3A%20%2218eaa11ecf92ac-030a85e51eb0cd-26001a51-1fa400-18eaa11ecfab6a%22%2C%22%24initial_referrer%22%3A%20%22https%3A%2F%2Fauth.uber.com%2F%22%2C%22%24initial_referring_domain%22%3A%20%22auth.uber.com%22%2C%22%24user_id%22%3A%20%225c1f8a52-cddc-46c4-80d4-758c833b5c5d%22%7D',
    'origin': 'https://merchants.ubereats.com',
    'priority': 'u=1, i',
    'referer': 'https://merchants.ubereats.com/manager/stores',
    'sec-ch-ua': '"Google Chrome";v="129", "Not=A?Brand";v="8", "Chromium";v="129"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36',
    'x-csrf-token': 'x',
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
