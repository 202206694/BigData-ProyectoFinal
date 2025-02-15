import boto3
from botocore.exceptions import ClientError, BotoCoreError

aws_region = "eu-south-2"
database_name = "trade_data_imat3a13"
glue_role = " "

abreviaturas = ["aave", "ada", "btc", "doge", "dot", "eth", "shb", "sol", "xlm", "xrp"]

glue_client = boto3.client("glue", region_name=aws_region)

def create_database():
    try:
        glue_client.create_database(
            DatabaseInput={"Name": database_name, "Description": "Base de datos para trade data"}
        )
        print(f"Base de datos '{database_name}' creada.")
    except glue_client.exceptions.AlreadyExistsException:
        print(f"La base de datos '{database_name}' ya existe.")
    except ClientError as e:
        print(f"Error de cliente AWS: {e}")
    except BotoCoreError as e:
        print(f"Error en la comunicación con AWS Glue: {e}")

def create_crawler():
    crawler_name = "crawler-sprint2"
    s3_target_paths = [f"s3://grupo13-{abreviatura}/" for abreviatura in abreviaturas]
    try:
        glue_client.create_crawler(
            Name=crawler_name,
            Role=glue_role,
            DatabaseName=database_name,
            Targets={'S3Targets': [{'Path': path} for path in s3_target_paths]},
            TablePrefix="trade_data_"
        )
        print(f"Crawler '{crawler_name}' creado.")
    except glue_client.exceptions.AlreadyExistsException:
        print(f"El crawler '{crawler_name}' ya existe.")
    except ClientError as e:
        print(f"Error de cliente AWS: {e}")
    except BotoCoreError as e:
        print(f"Error en la comunicación con AWS Glue: {e}")

def start_crawler():
    crawler_name = "crawler-sprint2"
    try:
        glue_client.start_crawler(Name=crawler_name)
        print(f"Crawler '{crawler_name}' iniciado correctamente.")
    except ClientError as e:
        print(f"Error de cliente AWS al iniciar el crawler: {e}")
    except BotoCoreError as e:
        print(f"Error en la comunicación con AWS Glue al iniciar el crawler: {e}")

def main():
    create_database()
    create_crawler()
    start_crawler()

if __name__ == "__main__":
    main()