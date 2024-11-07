import requests
from datetime import datetime, timedelta
import time
import json
import pandas as pd
import os

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
