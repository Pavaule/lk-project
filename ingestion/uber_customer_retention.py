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


# Fonction pour obtenir les dates de début et de fin du mois
def get_month_dates(date):
    start = date.replace(day=1)
    next_month = start + timedelta(days=32)
    end = next_month.replace(day=1) - timedelta(days=1)
    return start.strftime('%Y-%m-%d'), end.strftime('%Y-%m-%d')


# Dates de début et de fin
start_date = datetime(2023, 1, 1)
end_date = datetime.now()

# Liste pour stocker toutes les données
all_data = []

# Boucle principale pour récupérer et traiter les données
for restaurant in liste_resto:
    uuid = restaurant['UUID']
    print(f"Fetching data for restaurant UUID: {uuid}")
    current_date = start_date

    while current_date <= end_date:
        # Calcul des dates de début et de fin du mois
        month_start, month_end = get_month_dates(current_date)
        print(f"  Fetching month: {month_start} to {month_end}")

        json_data = {
            'operationName': 'CustomerRetentionOverTime',
            'variables': {
                'widgetInput': {
                    'dateRange': {
                        'start': month_start,
                        'end': month_end,
                    },
                    'locationConstraints': {
                        'countries': [],
                        'cities': [],
                        'locationUUIDs': [uuid]
                    },
                    'displayCurrencyCode': 'EUR',
                },
            },
            'query': 'query CustomerRetentionOverTime($widgetInput: WidgetInput!) {\n  customerRetentionOverTime(widgetInput: $widgetInput) {\n    ...widgetFields\n    __typename\n  }\n}\n\nfragment widgetFields on WidgetBasicAndCSV {\n  widget {\n    header {\n      ...headerFields\n      __typename\n    }\n    insights {\n      ...insightFields\n      __typename\n    }\n    visualization {\n      ...visualizationFields\n      __typename\n    }\n    dateRangeAdjusted\n    aggregationMeta {\n      start\n      end\n      timeUnit\n      __typename\n    }\n    __typename\n  }\n  __typename\n}\n\nfragment headerFields on WidgetHeader {\n  primaryHeader\n  subtitle\n  tooltipMarkdown\n  __typename\n}\n\nfragment insightFields on Insight {\n  header\n  body\n  secondaryCTA {\n    label\n    href\n    __typename\n  }\n  tertiaryCTA {\n    label\n    href\n    __typename\n  }\n  __typename\n}\n\nfragment visualizationFields on Visualization {\n  __typename\n  ... on BigNumbers {\n    ...visualizationBigNumbersFields\n    __typename\n  }\n  ... on ConversionFunnel {\n    ...visualizationConversionFunnelFields\n    __typename\n  }\n  ... on HeatMap {\n    ...visualizationHeatMapFields\n    __typename\n  }\n  ... on PeriodComparisonSeries {\n    ...visualizationPeriodComparisonSeriesFields\n    __typename\n  }\n  ... on AreaSeries {\n    ...visualizationAreaSeriesFields\n    __typename\n  }\n  ... on BarSeries {\n    ...visualizationBarSeriesFields\n    __typename\n  }\n  ... on BarSeriesWithoutDateTime {\n    ...visualizationBarSeriesWithoutDateTimeFields\n    __typename\n  }\n}\n\nfragment visualizationBigNumbersFields on BigNumbers {\n  bigNumbers {\n    label\n    value\n    __typename\n  }\n  __typename\n}\n\nfragment visualizationConversionFunnelFields on ConversionFunnel {\n  steps {\n    value\n    legend {\n      ...legendItemFields\n      __typename\n    }\n    __typename\n  }\n  hero {\n    ...legendItemFields\n    __typename\n  }\n  __typename\n}\n\nfragment legendItemFields on LegendItem {\n  header\n  subheader\n  delta {\n    ...chartDeltaFields\n    __typename\n  }\n  description\n  tooltip\n  __typename\n}\n\nfragment chartDeltaFields on ChartDelta {\n  delta\n  sentiment\n  label\n  __typename\n}\n\nfragment heapMapBucket on HeatMapBucket {\n  intensity\n  label\n  __typename\n}\n\nfragment heatMapRowCell on HeatMapCell {\n  value\n  intensity\n  cellborder\n  tooltip {\n    header\n    subheader\n    rows {\n      key\n      label\n      value\n      delta {\n        delta\n        sentiment\n        label\n        __typename\n      }\n      __typename\n    }\n    __typename\n  }\n  __typename\n}\n\nfragment heatMapRow on HeatMapRow {\n  label\n  cells {\n    ...heatMapRowCell\n    __typename\n  }\n  __typename\n}\n\nfragment visualizationHeatMapFields on HeatMap {\n  buckets {\n    ...heapMapBucket\n    __typename\n  }\n  columnLabels\n  rows {\n    ...heatMapRow\n    __typename\n  }\n  sentiment\n  __typename\n}\n\nfragment visualizationPeriodComparisonSeriesFields on PeriodComparisonSeries {\n  xAxisFormat\n  unit\n  periods {\n    cost {\n      ...legendItemFields\n      __typename\n    }\n    start\n    end\n    legend {\n      ...legendItemFields\n      __typename\n    }\n    points {\n      dateTime\n      y\n      __typename\n    }\n    __typename\n  }\n  __typename\n}\n\nfragment visualizationAreaSeriesFields on AreaSeries {\n  xAxisFormat\n  unit\n  areas {\n    start\n    end\n    legend {\n      ...legendItemFields\n      __typename\n    }\n    points {\n      dateTime\n      y\n      __typename\n    }\n    __typename\n  }\n  annotations {\n    ...timeSeriesAnnotationYAxisLineFields\n    __typename\n  }\n  tooltips {\n    ...tooltipByDateTimeFields\n    __typename\n  }\n  __typename\n}\n\nfragment timeSeriesAnnotationYAxisLineFields on TimeSeriesAnnotationYAxisLine {\n  value\n  label\n  iconHref\n  tooltipMarkdown\n  __typename\n}\n\nfragment tooltipByDateTimeFieldsTooltipRows on ChartTooltipRow {\n  key\n  delta {\n    ...chartDeltaFields\n    __typename\n  }\n  label\n  value\n  value2\n  __typename\n}\n\nfragment tooltipByDateTimeFieldsTooltip on ChartTooltip {\n  header\n  header2\n  unit\n  subheader\n  rows {\n    ...tooltipByDateTimeFieldsTooltipRows\n    __typename\n  }\n  __typename\n}\n\nfragment tooltipByDateTimeFields on ChartTooltipByDateTime {\n  dateTime\n  tooltip {\n    ...tooltipByDateTimeFieldsTooltip\n    __typename\n  }\n  __typename\n}\n\nfragment visualizationBarSeriesFields on BarSeries {\n  xAxisFormat\n  unit\n  sentiment\n  series {\n    key\n    start\n    end\n    legend {\n      ...legendItemFields\n      __typename\n    }\n    points {\n      dateTime\n      y\n      __typename\n    }\n    __typename\n  }\n  tooltips {\n    ...tooltipByDateTimeFields\n    __typename\n  }\n  hero {\n    ...legendItemFields\n    __typename\n  }\n  __typename\n}\n\nfragment visualizationBarSeriesWithoutDateTimeFields on BarSeriesWithoutDateTime {\n  xAxisFormat\n  unit\n  sentiment\n  series {\n    start\n    end\n    key\n    legend {\n      ...legendItemFields\n      __typename\n    }\n    points {\n      xLabel {\n        numbers\n        header\n        __typename\n      }\n      y\n      __typename\n    }\n    __typename\n  }\n  tooltips {\n    ...tooltipByDateTimeFields\n    __typename\n  }\n  hero {\n    ...legendItemFields\n    __typename\n  }\n  __typename\n}\n',
        }
        try:
            response = requests.post('https://merchants.ubereats.com/manager/graphql', cookies=cookies, headers=headers, json=json_data)
            response.raise_for_status()
            response_data = response.json()

            if 'errors' in response_data:
                print(f"API returned an error for month {month_start} to {month_end}:")
                print(json.dumps(response_data['errors'], indent=2))
                current_date += timedelta(days=32)
                current_date = current_date.replace(day=1)
                continue

            if response_data.get('data') is None:
                print(f"No data returned for month {month_start} to {month_end}")
                current_date += timedelta(days=32)
                current_date = current_date.replace(day=1)
                continue

            # Vérification de la structure de la réponse
            if 'customerRetentionOverTime' not in response_data['data']:
                print(f"Unexpected response structure for month {month_start} to {month_end}")
                print(json.dumps(response_data, indent=2))
                current_date += timedelta(days=32)
                current_date = current_date.replace(day=1)
                continue

            tooltips = response_data['data']['customerRetentionOverTime']['widget']['visualization']['tooltips']
            for day in tooltips:
                processed_data = {
                    'restaurant_uuid': uuid,
                    'date': day['dateTime']
                }
                for row in day['tooltip']['rows']:
                    key = row['key']
                    value = row['value']
                    if value == 'NaN':
                        value = None
                    processed_data[key] = value
                all_data.append(processed_data)

        except requests.exceptions.RequestException as e:
            print(f"Une erreur s'est produite lors de la requête: {e}")
            print(f"Response status code: {e.response.status_code}")
            print(f"Response headers: {e.response.headers}")
            print(f"Response content: {e.response.text}")
            print(f"Skipping month {month_start} to {month_end} due to error")


        current_date += timedelta(days=32)
        current_date = current_date.replace(day=1)

        # Ajouter un délai entre les requêtes pour éviter de surcharger l'API
        time.sleep(5)

# Création du DataFrame
df = pd.DataFrame(all_data)

# Réorganisation des colonnes (ajustez selon les clés réelles dans vos données)
column_order = ['restaurant_uuid', 'date', 'NEW_CUSTOMERS', 'REPEAT_CUSTOMERS_RANK_2', 'REPEAT_CUSTOMERS_RANK_3', 'REPEAT_CUSTOMERS_RANK_4_PLUS', 'total']
df = df.reindex(columns=column_order)
