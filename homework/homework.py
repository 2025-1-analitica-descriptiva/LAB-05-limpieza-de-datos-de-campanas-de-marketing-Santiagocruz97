"""
Escriba el codigo que ejecute la accion solicitada.
"""

# pylint: disable=import-outside-toplevel

import os
import zipfile
import pandas as pd

def clean_campaign_data():
    input_dir = "files/input/"
    output_dir = "files/output/"
    os.makedirs(output_dir, exist_ok=True)

    # Lista de archivos .zip en la carpeta de entrada
    input_files = [os.path.join(input_dir, f) for f in os.listdir(input_dir) if f.endswith(".zip")]

    all_data = []
    for file in input_files:
        with zipfile.ZipFile(file) as z:
            for name in z.namelist():
                if name.endswith(".csv"):
                    with z.open(name) as f:
                        df = pd.read_csv(f)
                        all_data.append(df)

    # Concatenar todos los DataFrames
    df_full = pd.concat(all_data, ignore_index=True)

    # === client.csv ===
    df_client = df_full[[
        'client_id', 'age', 'job', 'marital',
        'education', 'credit_default', 'mortgage'
    ]].copy()

    df_client['job'] = df_client['job'].str.replace('.', '', regex=False).str.replace('-', '_', regex=False)
    df_client['education'] = df_client['education'].str.replace('.', '_', regex=False)
    df_client['education'] = df_client['education'].replace('unknown', pd.NA)
    df_client['credit_default'] = df_client['credit_default'].apply(lambda x: 1 if x == 'yes' else 0)
    df_client['mortgage'] = df_client['mortgage'].apply(lambda x: 1 if x == 'yes' else 0)

    df_client.to_csv(os.path.join(output_dir, 'client.csv'), index=False)

    # === campaign.csv ===
    df_campaign = df_full[[
        'client_id', 'number_contacts', 'contact_duration',
        'previous_campaign_contacts', 'previous_outcome',
        'campaign_outcome', 'day', 'month'
    ]].copy()

    df_campaign['previous_outcome'] = df_campaign['previous_outcome'].apply(lambda x: 1 if x == 'success' else 0)
    df_campaign['campaign_outcome'] = df_campaign['campaign_outcome'].apply(lambda x: 1 if x == 'yes' else 0)

    month_map = {
        'jan': '01', 'feb': '02', 'mar': '03', 'apr': '04',
        'may': '05', 'jun': '06', 'jul': '07', 'aug': '08',
        'sep': '09', 'oct': '10', 'nov': '11', 'dec': '12'
    }
    df_campaign['month_num'] = df_campaign['month'].str.lower().map(month_map)
    df_campaign['day'] = df_campaign['day'].astype(str).str.zfill(2)
    df_campaign['last_contact_date'] = '2022-' + df_campaign['month_num'] + '-' + df_campaign['day']
    df_campaign.drop(columns=['month', 'month_num', 'day'], inplace=True)

    df_campaign.to_csv(os.path.join(output_dir, 'campaign.csv'), index=False)

    # === economics.csv ===
    df_economics = df_full[[
        'client_id', 'cons_price_idx', 'euribor_three_months'
    ]].copy()

    df_economics.to_csv(os.path.join(output_dir, 'economics.csv'), index=False)


    """
    En esta tarea se le pide que limpie los datos de una campaña de
    marketing realizada por un banco, la cual tiene como fin la
    recolección de datos de clientes para ofrecerls un préstamo.

    La información recolectada se encuentra en la carpeta
    files/input/ en varios archivos csv.zip comprimidos para ahorrar
    espacio en disco.

    Usted debe procesar directamente los archivos comprimidos (sin
    descomprimirlos). Se desea partir la data en tres archivos csv
    (sin comprimir): client.csv, campaign.csv y economics.csv.
    Cada archivo debe tener las columnas indicadas.

    Los tres archivos generados se almacenarán en la carpeta files/output/.

    client.csv:
    - client_id
    - age
    - job: se debe cambiar el "." por "" y el "-" por "_"
    - marital
    - education: se debe cambiar "." por "_" y "unknown" por pd.NA
    - credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
    - mortage: convertir a "yes" a 1 y cualquier otro valor a 0

    campaign.csv:
    - client_id
    - number_contacts
    - contact_duration
    - previous_campaing_contacts
    - previous_outcome: cmabiar "success" por 1, y cualquier otro valor a 0
    - campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
    - last_contact_day: crear un valor con el formato "YYYY-MM-DD",
        combinando los campos "day" y "month" con el año 2022.

    economics.csv:
    - client_id
    - const_price_idx
    - eurobor_three_months



    """
if __name__ == "__main__":
    clean_campaign_data()
