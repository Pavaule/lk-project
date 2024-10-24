import requests
from datetime import datetime, timedelta
import time
import json
import pandas as pd
import os

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
    # 'cookie': 'udi-id=kwXouyFvFrJNRXv8uayOrdhqJnwUE2PU8jyU4FVtXORTH3vMSIugyKt7ByMXqzvo5zHCpltlykvF+9oXiPEY9gnoS9iDzwKeo12vZVN3sCfiSUKfGgqmMurDZsPQd3tMMH1xmxI1XcZATh+Vos+qNLVNxpgjucgEA4i9f5SS7ZSBS4x8NUffvI9A4vv6ahyFHEXxTYZyhyVbl1KGHMlnBg==6Vgud5x3mnM4lwaDaoaZhQ==WfAcPKsbA4B1wN6/e5PaFpwByUWCEOz0WCHPrzW2+Ik=; _cc=AQuD3DHkAaVIzc4gtKNHXT7q; _cid_cc=AQuD3DHkAaVIzc4gtKNHXT7q; marketing_vistor_id=facee8d3-6729-4a64-b7c8-a1597dad3f68; uber_sites_geolocalization={%22best%22:{%22localeCode%22:%22fr-FR%22%2C%22countryCode%22:%22FR%22%2C%22territoryId%22:51%2C%22territorySlug%22:%22lyon%22%2C%22territoryName%22:%22Lyon%22}%2C%22url%22:{%22localeCode%22:%22fr-FR%22%2C%22countryCode%22:%22FR%22}%2C%22user%22:{%22countryCode%22:%22FR%22%2C%22territoryId%22:51%2C%22territoryGeoJson%22:[[{%22lat%22:47.1554108%2C%22lng%22:3.6228018}%2C{%22lat%22:47.1554108%2C%22lng%22:6.3585501}%2C{%22lat%22:44.6969643%2C%22lng%22:6.3585501}%2C{%22lat%22:44.6969643%2C%22lng%22:3.6228018}]]%2C%22territoryGeoPoint%22:{%22latitude%22:45.764%2C%22longitude%22:4.8357}%2C%22territorySlug%22:%22lyon%22%2C%22territoryName%22:%22Lyon%22%2C%22localeCode%22:%22fr-FR%22}}; segmentCookie=b; utag_main_segment=a; utag_main_optimizely_segment=a; dId=0cb9705c-710c-45fb-aace-eabee593c9e2; uev2.gdprAdsConsented=true; uev2.gg=true; CONSENTMGR=c1:1%7Cc2:1%7Cc3:1%7Cc4:1%7Cc5:1%7Cc6:1%7Cc7:1%7Cc8:1%7Cc9:1%7Cc10:1%7Cc11:1%7Cc12:1%7Cc13:1%7Cc14:1%7Cc15:1%7Cts:1713259230208%7Cconsent:true; _scid=4404be9b-22eb-48d6-9be1-f14ed447a56e; _fbp=fb.1.1713259230933.1079323249; _tt_enable_cookie=1; _ttp=nxeo9rO7DZKIwBWgOMjjxxuJzQy; _yjsu_yjad=1713259231.213846e4-eddd-4bbc-b06a-567142350f1c; uev2.diningMode=DELIVERY; _gcl_au=1.1.301575419.1721048191; _clck=18j3pnf%7C2%7Cfpm%7C0%7C1677; _scid_r=udlEBL6bIuvZ1jfh8U7UR6VuAI2ltntfvDbhjA; _ga=GA1.2.1700777215.1713259231; uev2.loc=%7B%22address%22%3A%7B%22address1%22%3A%2235%20Rue%20Saint-Fran%C3%A7ois%20R%C3%A9gis%22%2C%22address2%22%3A%22Le%20Puy-en-Velay%22%2C%22aptOrSuite%22%3A%22%22%2C%22eaterFormattedAddress%22%3A%2235%20Rue%20Saint-Fran%C3%A7ois%20R%C3%A9gis%2C%2043000%20Le%20Puy-en-Velay%2C%20France%22%2C%22subtitle%22%3A%22Le%20Puy-en-Velay%22%2C%22title%22%3A%2235%20Rue%20Saint-Fran%C3%A7ois%20R%C3%A9gis%22%2C%22uuid%22%3A%22%22%7D%2C%22latitude%22%3A45.0439255%2C%22longitude%22%3A3.886289599999999%2C%22reference%22%3A%22ChIJO50FY1b69UcR1sKtVfc0QFg%22%2C%22referenceType%22%3A%22google_places%22%2C%22type%22%3A%22google_places%22%2C%22addressComponents%22%3A%7B%22city%22%3A%22%22%2C%22countryCode%22%3A%22FR%22%2C%22firstLevelSubdivisionCode%22%3A%22Auvergne-Rh%C3%B4ne-Alpes%22%2C%22postalCode%22%3A%2243000%22%7D%2C%22categories%22%3A%5B%22address_point%22%5D%2C%22originType%22%3A%22user_autocomplete%22%7D; utag_main__sn=31; _uetvid=91f46780fbd211eebf6d199e4aa82597; _ga_P1RM71MPFP=GS1.1.1727725471.34.0.1727725471.60.0.0; jwt-session-uem=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE3Mjc5NTY3NzAsImV4cCI6MTcyODU2MTU3MH0.c5AwOxNr4bkBlepc9a9qEFhN6rb3Q-Ly8f0MiV6TayY; sid=QA.CAESEGMvmeD090wZuiQUz63RqHkYopauuQYiATEqJDVjMWY4YTUyLWNkZGMtNDZjNC04MGQ0LTc1OGM4MzNiNWM1ZDI8sxbiQBpMTzfAULUrVkkZuqGBCE5lyedjPpEvEKKT1-8nZ_ivA5c-orKnol-sYzL3a6p08SsaGoJ5-cITOgExQg0udWJlcmVhdHMuY29t.9r76owhZ-JaEIkAXhgGsMojKG36T5QoLKRjuuY0mFm8; _ua={"session_id":"6ff9fb1d-4a15-4b20-9f78-b32aceaa78b9","session_time_ms":1728423045093}; selectedRestaurant=e37f5ac6-c118-5a8d-9ef0-999112d2ec79; udi-fingerprint=qn6ZvZUcPyEh5f6rmniFIZNl2PzpQuxZVTpCr+YMUSSE6R3ltVxZBUVBe6qLFFEro1GVYxjga6YvixOw3Z8Vaw==3R1DNQ5itY44KWKu9gidHopiJitlTxt9AYqGsIsSJ0o=; user_city_ids=51; jwt-session=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJkYXRhIjp7InNsYXRlLWV4cGlyZXMtYXQiOjE3Mjg0MjQ4NTIxNzV9LCJpYXQiOjE3Mjg0MTM4NDcsImV4cCI6MTcyODUwMDI0N30.UTqxLgUmEir_eWQ3gFwEkQ3Gf5hb0h4X3eLRxUETf4Q; mp_adec770be288b16d9008c964acfba5c2_mixpanel=%7B%22distinct_id%22%3A%20%225c1f8a52-cddc-46c4-80d4-758c833b5c5d%22%2C%22%24device_id%22%3A%20%2218eaa11ecf92ac-030a85e51eb0cd-26001a51-1fa400-18eaa11ecfab6a%22%2C%22%24initial_referrer%22%3A%20%22https%3A%2F%2Fauth.uber.com%2F%22%2C%22%24initial_referring_domain%22%3A%20%22auth.uber.com%22%2C%22%24user_id%22%3A%20%225c1f8a52-cddc-46c4-80d4-758c833b5c5d%22%7D',
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



# Fonction pour obtenir les dates de début et de fin de la semaine
def get_week_dates(date):
    start = date - timedelta(days=date.weekday())
    end = start + timedelta(days=6)
    return start.strftime('%Y-%m-%d'), end.strftime('%Y-%m-%d')


# Dates de début et de fin
start_date = datetime(2023, 11, 1)
end_date = datetime.now()

# Liste pour stocker toutes les données
all_data = []

# Boucle principale pour récupérer et traiter les données
for restaurant in liste_resto:
    uuid = restaurant['UUID']
    print(f"Fetching data for restaurant UUID: {uuid}")
    current_date = start_date

    while current_date <= end_date:
        # Calcul des dates de début et de fin de la semaine
        week_start = (current_date - timedelta(days=current_date.weekday())).strftime('%Y-%m-%d')
        week_end = (current_date - timedelta(days=current_date.weekday()) + timedelta(days=6)).strftime('%Y-%m-%d')
        print(f"  Fetching week: {week_start} to {week_end}")

        json_data = {
            'operationName': 'CustomerMenuConversion',
            'variables': {
                'widgetInput': {
                    'dateRange': {
                        'start': week_start,
                        'end': week_end,
                    },
                    'locationConstraints': {
                        'countries': [],
                        'cities': [],
                        'locationUUIDs': [uuid],
                    },
                    'displayCurrencyCode': 'EUR',
                },
            },
            'query': 'query CustomerMenuConversion($widgetInput: WidgetInput!) {\n  customerMenuConversion(widgetInput: $widgetInput) {\n    ...widgetFields\n    __typename\n  }\n}\n\nfragment widgetFields on WidgetBasicAndCSV {\n  widget {\n    header {\n      ...headerFields\n      __typename\n    }\n    insights {\n      ...insightFields\n      __typename\n    }\n    visualization {\n      ...visualizationFields\n      __typename\n    }\n    dateRangeAdjusted\n    aggregationMeta {\n      start\n      end\n      timeUnit\n      __typename\n    }\n    __typename\n  }\n  __typename\n}\n\nfragment headerFields on WidgetHeader {\n  primaryHeader\n  subtitle\n  tooltipMarkdown\n  __typename\n}\n\nfragment insightFields on Insight {\n  header\n  body\n  secondaryCTA {\n    label\n    href\n    __typename\n  }\n  tertiaryCTA {\n    label\n    href\n    __typename\n  }\n  __typename\n}\n\nfragment visualizationFields on Visualization {\n  __typename\n  ... on BigNumbers {\n    ...visualizationBigNumbersFields\n    __typename\n  }\n  ... on ConversionFunnel {\n    ...visualizationConversionFunnelFields\n    __typename\n  }\n  ... on HeatMap {\n    ...visualizationHeatMapFields\n    __typename\n  }\n  ... on PeriodComparisonSeries {\n    ...visualizationPeriodComparisonSeriesFields\n    __typename\n  }\n  ... on AreaSeries {\n    ...visualizationAreaSeriesFields\n    __typename\n  }\n  ... on BarSeries {\n    ...visualizationBarSeriesFields\n    __typename\n  }\n  ... on BarSeriesWithoutDateTime {\n    ...visualizationBarSeriesWithoutDateTimeFields\n    __typename\n  }\n}\n\nfragment visualizationBigNumbersFields on BigNumbers {\n  bigNumbers {\n    label\n    value\n    __typename\n  }\n  __typename\n}\n\nfragment visualizationConversionFunnelFields on ConversionFunnel {\n  steps {\n    value\n    legend {\n      ...legendItemFields\n      __typename\n    }\n    __typename\n  }\n  hero {\n    ...legendItemFields\n    __typename\n  }\n  __typename\n}\n\nfragment legendItemFields on LegendItem {\n  header\n  subheader\n  delta {\n    ...chartDeltaFields\n    __typename\n  }\n  description\n  tooltip\n  __typename\n}\n\nfragment chartDeltaFields on ChartDelta {\n  delta\n  sentiment\n  label\n  __typename\n}\n\nfragment heapMapBucket on HeatMapBucket {\n  intensity\n  label\n  __typename\n}\n\nfragment heatMapRowCell on HeatMapCell {\n  value\n  intensity\n  cellborder\n  tooltip {\n    header\n    subheader\n    rows {\n      key\n      label\n      value\n      delta {\n        delta\n        sentiment\n        label\n        __typename\n      }\n      __typename\n    }\n    __typename\n  }\n  __typename\n}\n\nfragment heatMapRow on HeatMapRow {\n  label\n  cells {\n    ...heatMapRowCell\n    __typename\n  }\n  __typename\n}\n\nfragment visualizationHeatMapFields on HeatMap {\n  buckets {\n    ...heapMapBucket\n    __typename\n  }\n  columnLabels\n  rows {\n    ...heatMapRow\n    __typename\n  }\n  sentiment\n  __typename\n}\n\nfragment visualizationPeriodComparisonSeriesFields on PeriodComparisonSeries {\n  xAxisFormat\n  unit\n  periods {\n    cost {\n      ...legendItemFields\n      __typename\n    }\n    start\n    end\n    legend {\n      ...legendItemFields\n      __typename\n    }\n    points {\n      dateTime\n      y\n      __typename\n    }\n    __typename\n  }\n  __typename\n}\n\nfragment visualizationAreaSeriesFields on AreaSeries {\n  xAxisFormat\n  unit\n  areas {\n    start\n    end\n    legend {\n      ...legendItemFields\n      __typename\n    }\n    points {\n      dateTime\n      y\n      __typename\n    }\n    __typename\n  }\n  annotations {\n    ...timeSeriesAnnotationYAxisLineFields\n    __typename\n  }\n  tooltips {\n    ...tooltipByDateTimeFields\n    __typename\n  }\n  __typename\n}\n\nfragment timeSeriesAnnotationYAxisLineFields on TimeSeriesAnnotationYAxisLine {\n  value\n  label\n  iconHref\n  tooltipMarkdown\n  __typename\n}\n\nfragment tooltipByDateTimeFieldsTooltipRows on ChartTooltipRow {\n  key\n  delta {\n    ...chartDeltaFields\n    __typename\n  }\n  label\n  value\n  value2\n  __typename\n}\n\nfragment tooltipByDateTimeFieldsTooltip on ChartTooltip {\n  header\n  header2\n  unit\n  subheader\n  rows {\n    ...tooltipByDateTimeFieldsTooltipRows\n    __typename\n  }\n  __typename\n}\n\nfragment tooltipByDateTimeFields on ChartTooltipByDateTime {\n  dateTime\n  tooltip {\n    ...tooltipByDateTimeFieldsTooltip\n    __typename\n  }\n  __typename\n}\n\nfragment visualizationBarSeriesFields on BarSeries {\n  xAxisFormat\n  unit\n  sentiment\n  series {\n    key\n    start\n    end\n    legend {\n      ...legendItemFields\n      __typename\n    }\n    points {\n      dateTime\n      y\n      __typename\n    }\n    __typename\n  }\n  tooltips {\n    ...tooltipByDateTimeFields\n    __typename\n  }\n  hero {\n    ...legendItemFields\n    __typename\n  }\n  __typename\n}\n\nfragment visualizationBarSeriesWithoutDateTimeFields on BarSeriesWithoutDateTime {\n  xAxisFormat\n  unit\n  sentiment\n  series {\n    start\n    end\n    key\n    legend {\n      ...legendItemFields\n      __typename\n    }\n    points {\n      xLabel {\n        numbers\n        header\n        __typename\n      }\n      y\n      __typename\n    }\n    __typename\n  }\n  tooltips {\n    ...tooltipByDateTimeFields\n    __typename\n  }\n  hero {\n    ...legendItemFields\n    __typename\n  }\n  __typename\n}\n',
        }

        try:
            response = requests.post('https://merchants.ubereats.com/manager/graphql', cookies=cookies, headers=headers, json=json_data)
            response.raise_for_status()
            response_data = response.json()

            if 'errors' in response_data:
                print(f"API returned an error for week {week_start} to {week_end}:")
                print(json.dumps(response_data['errors'], indent=2))
                current_date += timedelta(days=7)
                continue

            if response_data.get('data') is None:
                print(f"No data returned for week {week_start} to {week_end}")
                current_date += timedelta(days=7)
                continue

            processed_data = {
                'restaurant_uuid': uuid,
                'week_start': week_start,
                'week_end': week_end
            }

            chemin = response_data.get('data', {}).get('customerMenuConversion', {}).get('widget', {}).get('visualization', {}).get('steps', [])
            for element in chemin:
                legend = element.get('legend', {})
                subheader = legend.get('subheader')
                value = element.get('value')
                if value == 'NaN':
                    value = None  # ou 0, selon ce qui est le plus approprié pour votre cas
                if subheader == 'storefront_impression_total':
                    processed_data['view'] = value
                elif subheader == 'added_to_cart_total':
                    processed_data['add_to_command'] = value
                elif subheader == 'placed_order_total':
                    processed_data['command_done'] = value
                else:
                    print(f'There is a new type of data: {subheader}')

            all_data.append(processed_data)

        except requests.exceptions.RequestException as e:
            print(f"Une erreur s'est produite lors de la requête: {e}")
            print(f"Skipping week {week_start} to {week_end} due to error")

        current_date += timedelta(days=7)


# Création du DataFrame
df = pd.DataFrame(all_data)

# Réorganisation des colonnes
column_order = ['restaurant_uuid', 'week_start', 'week_end', 'view', 'add_to_command', 'command_done']
df = df[column_order]
