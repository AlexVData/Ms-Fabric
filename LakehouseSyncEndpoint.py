import json
import time
import logging
from datetime import datetime
import pytz
import sempy.fabric as fabric
from sempy.fabric.exceptions import FabricHTTPException, WorkspaceNotFoundException

# Logging warnings and errors
logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(levelname)s - %(message)s')

class LakehouseSyncManager:
    def __init__(self, workspace_id, lakehouse_id):
        """
        Inicializa el sincronizador de Lakehouse.

        :param workspace_id: ID del espacio de trabajo.
        :param lakehouse_id: ID del Lakehouse.
        """
        self.workspace_id = workspace_id
        self.lakehouse_id = lakehouse_id
        self.client = fabric.FabricRestClient()

    @staticmethod
    def convert_to_spain(utc_time_str):
        """
        Convierte tiempo UTC al formato de la zona horaria de España.

        :param utc_time_str: Marca de tiempo UTC en formato ISO.
        :return: Fecha y hora convertidas al formato de Madrid.
        """
        if utc_time_str is None:
            return 'N/A'

        # Zona horaria de España
        madrid_tz = pytz.timezone('Europe/Madrid')

        # Truncar el string para milisegundos extra
        utc_time_str = utc_time_str.split('.')[0] + '+00:00'

        # Convertir a datetime y ajustar zona horaria
        utc_time = datetime.fromisoformat(utc_time_str)
        return utc_time.astimezone(madrid_tz).strftime('%Y-%m-%d %H:%M:%S %Z')

    def fetch_sql_endpoint_id(self):
        """
        Obtiene el ID del SQL Endpoint asociado al Lakehouse.

        :return: SQL Endpoint ID.
        """
        response = self.client.get(f"/v1/workspaces/{self.workspace_id}/lakehouses/{self.lakehouse_id}")
        response.raise_for_status()
        lakehouse_info = response.json()
        return lakehouse_info['properties']['sqlEndpointProperties']['id']

    def initiate_sync(self, sql_endpoint_id):
        """
        Inicia la sincronización del SQL Endpoint.

        :param sql_endpoint_id: ID del SQL Endpoint.
        :return: ID del lote y estado inicial.
        """
        uri = f"/v1.0/myorg/lhdatamarts/{sql_endpoint_id}"
        payload = {"commands": [{"$type": "MetadataRefreshCommand"}]}
        response = self.client.post(uri, json=payload)
        response.raise_for_status()
        data = response.json()
        return data["batchId"], data["progressState"]

    def poll_sync_status(self, sql_endpoint_id, batch_id):
        """
        Realiza polling para monitorear el progreso de la sincronización.

        :param sql_endpoint_id: ID del SQL Endpoint.
        :param batch_id: ID del lote de sincronización.
        :return: Detalles finales del estado de la sincronización.
        """
        status_uri = f"/v1.0/myorg/lhdatamarts/{sql_endpoint_id}/batches/{batch_id}"
        progress_state = 'inProgress'

        while progress_state == 'inProgress':
            time.sleep(1)
            response = self.client.get(status_uri)
            response.raise_for_status()
            status_data = response.json()
            progress_state = status_data["progressState"]

        return status_data

    def sync_lakehouse(self):
        """
        Sincroniza el SQL Endpoint con el Lakehouse.
        """
        try:
            # Obtener el ID del SQL Endpoint
            sql_endpoint_id = self.fetch_sql_endpoint_id()

            # Iniciar la sincronización
            batch_id, progress_state = self.initiate_sync(sql_endpoint_id)

            # Monitorear el estado de la sincronización
            status_data = self.poll_sync_status(sql_endpoint_id, batch_id)

            # Procesar el resultado
            if status_data["progressState"] == 'success':
                tables_sync_status = status_data['operationInformation'][0]['progressDetail']['tablesSyncStatus']
                table_details = [
                    {
                        'tableName': table['tableName'],
                        'lastSuccessfulUpdate': self.convert_to_spain(table.get('lastSuccessfulUpdate', 'N/A')),
                        'tableSyncState': table['tableSyncState'],
                        'sqlSyncState': table['sqlSyncState']
                    }
                    for table in tables_sync_status
                ]
                for detail in table_details:
                    print(f"Table: {detail['tableName']}   Last Update: {detail['lastSuccessfulUpdate']}  "
                          f"Table Sync State: {detail['tableSyncState']}  SQL Sync State: {detail['sqlSyncState']}")

            elif status_data["progressState"] == 'failure':
                logging.error(f"Sync failed: {status_data}")
            else:
                logging.warning(f"Unexpected state: {status_data['progressState']}")

        except FabricHTTPException as fe:
            logging.error(f"Fabric HTTP Exception: {fe}")
        except WorkspaceNotFoundException as we:
            logging.error(f"Workspace not found: {we}")
        except Exception as e:
            logging.error(f"An unexpected error occurred: {e}")

# Funcion Main
def run_sync():
    # IDs de ejemplo

    ## Para version 1.2
    workspace_id = 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX'
    lakehouse_id = 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX'

    ## Para version 1.3
    #workspace_id = spark.conf.get("trident.workspace.id") 
    #lakehouse_id = spark.conf.get("trident.lakehouse.id") 
    

    # Crear y ejecutar el sincronizador
    sync_manager = LakehouseSyncManager(workspace_id, lakehouse_id)
    sync_manager.sync_lakehouse()



run_sync()