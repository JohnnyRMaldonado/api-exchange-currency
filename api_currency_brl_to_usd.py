import pandas as pd
import requests
from google.cloud import bigquery
from google.cloud import secretmanager


def access_secret_version(project_id, secret_id, version_id="latest"):
    # Crear el cliente de Secret Manager
    client = secretmanager.SecretManagerServiceClient()

    # Configurar el nombre completo del secreto y la versión (por defecto 'latest')
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"

    # Acceder a la versión del secreto
    response = client.access_secret_version(request={"name": name})

    # El valor del secreto está en 'payload.data'
    secret_value = response.payload.data.decode("UTF-8")

    return secret_value

def run_pipeline():
    # Configure your variables
    project_id = "cobalt-matrix-441017-e8"
    secret_key = "secret_currency_api"

    # get the api key
    api_key = access_secret_version(project_id, secret_key)

    # Configurar la URL para obtener los datos de la API
    url = f"https://api.freecurrencyapi.com/v1/latest?apikey={api_key}&currencies=USD&base_currency=BRL"

    # Ruta de archivo de cloud storage
    file_products = "gs://bucket-brl-to-us/products.csv"
    file_orders = "gs://bucket-brl-to-us/orders.csv"

    #df_products =  pd.read_csv(file_products)
    df_orders = pd.read_csv(file_orders)
    df_products = pd.read_csv(file_products)

    # Get the latest currency conversion data (BRL to USD)
    response = requests.get(url)
    currency_data = response.json()

    # Extract the conversion rate for BRL to USD
    brl_to_usd = currency_data['data']['USD']

    #merge the files
    df_merged = pd.merge(df_orders, df_products, left_on="product_id", right_on= "id", how="inner")

    # Calculate the total price in BRL and USD
    df_merged['total_price_br'] = df_merged['quantity'] * df_merged['price']
    df_merged['total_price_us'] = (df_merged['total_price_br'] * brl_to_usd).round(2)

    #select the columns
    df_consolidated = df_merged[["created_date", "id_x", "name", "quantity", "total_price_br", "total_price_us"]]

    #rename the columns
    df_consolidated = df_consolidated.rename(columns={
    "created_date": "order_created_date",
    "id_x": "order_id", 
    "name": "product_name", 
    })

    # print(df_consolidated)

    #configuración de Bquery
    dataset_id = "Johnny_ETLs"
    table_id = "brl_to_us"

    # Configuración del trabajo de carga
    cliente = bigquery.Client(project=project_id)
    table_ref = cliente.dataset(dataset_id).table(table_id)
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
    job_config.autodetect = True


    # Cargar el DataFrame a la tabla en BigQuery
    job = cliente.load_table_from_dataframe(df_consolidated, table_ref, job_config=job_config)
        
    # job.result() # Esperar a que el trabajo se complete
    job.result()

    # Imprimir confirmación de éxito
    print(f"Datos cargados exitosamente en la tabla {table_id}")

    
def main(request):
    try:
        run_pipeline()
        return "corrio bien"
    except Exception as e:
        error_message=str(e)
        print(f"Error is: {error_message}")
        return "Error"
    
    
