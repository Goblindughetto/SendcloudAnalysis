
#Export Sendcloud analytics:
	#• 10 villes ou on envoie le plus (Destination City)
	#• 5 villes ou on envoie le plus (en dehors de la belgique) (Destination City)
	#• Temps entre création de l'étiquette et livré : (Faire une moyenne de: (Date Arrived - Created At)/ nombre de commandes)
#Temps entre expédié et livré: (Date Arrived -Date Shipped)


import datetime
import geopandas as gpd
import timedelta
from geopy.geocoders import Nominatim
import requests
import base64
import time
import pandas as pd
import numpy as np
import matplotlib as mpl

public_key = "4e4ef2372fb84fec9cc18708c3166d22"
private_key = "9131be48d59f4b2f8f6134d0ff7d1b5e"

# Construisez l'en-tête d'authentification
credentials = f"{public_key}:{private_key}"
encoded_credentials = base64.b64encode(credentials.encode("utf-8")).decode("utf-8")
headers = {
    "Accept": "application/json",
    "Authorization": f"Basic {encoded_credentials}",
    "Content-Type": "application/json"
}

# Définissez les données de la requête pour générer le rapport
report_request_data = {
    "fields": ["parcel_id", "direction", "carrier_code","destination_city",
               "destination_postal_code", "destination_country_code",
               "order_number", "tracking_number", "created_at", "updated_at", "announced_at",
               "shipped_at", "first_delivery_at", "arrived_at", "shipping_method",
               "shipping_method_name","global_status_slug", "carrier_status",
               "integration_id", "integration_type", "weight"],
    "filters": {
    }
}

# Effectuez la requête POST pour générer le rapport
response = requests.post('https://panel.sendcloud.sc/api/v2/reporting/parcels', headers=headers, json=report_request_data)



if response.status_code == 202:
    report_id = response.json().get("id")
    print("Rapport généré avec succès. Report ID :", report_id)
    
    # Construction l'URL de téléchargement
    download_url = f'https://panel.sendcloud.sc/api/v2/reporting/parcels/{report_id}'
    print(download_url)
    while True:
        # requête GET pour obtenir les informations du rapport
        report_response = requests.get(download_url, headers=headers)
        report_data = report_response.json()
        
        # Vérifier si le rapport est prêt à être téléchargé
        if report_data['status_message'] == 'The report is ready':
            download_csv_url = report_data['url']
            
            # Télécharger le fichier CSV en utilisant la bibliothèque requests
            response = requests.get(download_csv_url)
            
            if response.status_code == 200:
                # Enregistrer le contenu du fichier téléchargé
                with open('report.csv', 'wb') as file:
                    file.write(response.content)
                print("Rapport téléchargé avec succès.")
                break  
            else:
                print("Échec du téléchargement du rapport. Code de statut :", response.status_code)
        
        else:
            print("Le rapport n'est pas encore prêt.")
            time.sleep(5)  
        
else:
    print("La requête n'a pas réussi. Code de statut :", response.status_code)
print(download_csv_url)

df = pd.read_csv(download_csv_url)



# Filtrer les lignes avec "#" dans la colonne "Order Number"
# Supprimer les lignes qui ne contiennent pas de '#' et ne sont pas vides dans la colonne 'Order Number'
filtered_df = df[df['Order Number'].str.contains('#', na=False)]
filtered_df['Order Number'] = filtered_df['Order Number'].str.extract(r'#(\d+)').astype('int64')

#filtrer les données pour ne garder que bpost comme valeur de Carrier code
filtered_df = filtered_df[filtered_df['Carrier Code'] == 'bpost']

#filtrer les données pour ne garder que bpost comme valeur de cCountry code 'BE', 'FR', 'LU', 'DE', 'NL'

selected_countries = ['BE', 'FR', 'LU', 'DE', 'NL']
filtered_df = filtered_df[filtered_df['Destination Country Code'].isin(selected_countries)]

# Supposons que vous voulez afficher les différentes valeurs uniques dans la colonne 'Carrier Code'
unique_carrier_codes = filtered_df['Tracking Number'].unique()
print(unique_carrier_codes)

# Vérifier s'il y a des valeurs nulles dans la colonne 'Order Number'
has_null_values =filtered_df['Tracking Number'].isnull().any()
if has_null_values:
    print("Il y a des valeurs nulles dans la colonne 'Order Number'")
else:
    print("Il n'y a pas de valeurs nulles dans la colonne 'Order Number'")


#Conversion des types de données en dates , int et str
filtered_df['Direction'] = filtered_df['Direction'].astype(str)
filtered_df['Carrier Code'] = filtered_df['Carrier Code'].astype(str)
filtered_df['Order Number'] = filtered_df['Order Number'].astype(int)
filtered_df['Destination City'] = filtered_df['Destination City'].astype(str)
filtered_df['Destination Postal Code'] = filtered_df['Destination Postal Code'].astype(str)
filtered_df['Destination Country Code'] = filtered_df['Destination Country Code'].astype(str)
filtered_df['Shipping Method Name'] = filtered_df['Shipping Method Name'].astype(str)



filtered_df['Created At'] = pd.to_datetime(filtered_df['Created At'])
filtered_df['Updated At'] = pd.to_datetime(filtered_df['Updated At'])
filtered_df['Date Announced'] = pd.to_datetime(filtered_df['Date Announced'])
filtered_df['Date Shipped'] = pd.to_datetime(filtered_df['Date Shipped'], errors='coerce')
filtered_df['Date Arrived'] = pd.to_datetime(filtered_df['Date Arrived'], errors='coerce')
filtered_df['Date First Delivery'] = pd.to_datetime(filtered_df['Date First Delivery'], errors='coerce')


filtered_df.reset_index(drop=True, inplace=True)
print(filtered_df.dtypes)
print(filtered_df.describe)


filtered_df = filtered_df.dropna(subset=['Date Shipped', 'Date Arrived'])

# Calculer le temps entre la date d'arrivée et la date de création en jours ouvrables
def calculate_created_to_arrived_days(row):
    start_date = row['Created At'].date()
    end_date = row['Date Arrived'].date()
    business_days = np.busday_count(start_date, end_date)
    return business_days

filtered_df['Created to Arrived Time (Business Days)'] = filtered_df.apply(calculate_created_to_arrived_days, axis=1)

# Calculer la moyenne du temps entre la date d'arrivée et la date de création en jours ouvrables
average_created_to_arrived_time = filtered_df['Created to Arrived Time (Business Days)'].mean()
print("Moyenne du temps entre la date d'arrivée et la date de création en jours ouvrables :", average_created_to_arrived_time)





# Calculer les 10 villes où vous envoyez le plus
top_10_cities = filtered_df['Destination City'].value_counts().head(10)

# Afficher les 10 villes où vous envoyez le plus
print("Les 10 villes où vous envoyez le plus :", top_10_cities)

# Calculer la moyenne du temps entre la date d'arrivée et la date de création en jours ouvrables pour ces 10 villes
average_created_to_arrived_time_top_cities = filtered_df[filtered_df['Destination City'].isin(top_10_cities.index)]['Created to Arrived Time (Business Days)'].mean()

print("Moyenne du temps entre la date d'arrivée et la date de création en jours ouvrables pour les 10 villes où vous envoyez le plus :", average_created_to_arrived_time_top_cities)
