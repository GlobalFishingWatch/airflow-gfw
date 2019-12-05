from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults


GCLOUD_CONN_ID='google_cloud_storage_default'

class GoogleCloudStoragePrefixSensor(BaseSensorOperator):
    """
    Checks for the existence of a file in Google Cloud Storage.
    """
    template_fields = ('bucket', 'prefix')
    ui_color = '#f0eee4'

    @apply_defaults
    def __init__(
            self,
            bucket,
            prefix,
            google_cloud_conn_id=GCLOUD_CONN_ID,
            delegate_to=None,
            *args,
            **kwargs):

        super(GoogleCloudStoragePrefixSensor, self).__init__(*args, **kwargs)
        self.bucket = bucket
        self.prefix = prefix
        self.google_cloud_conn_id = google_cloud_conn_id
        self.delegate_to = delegate_to

    def poke(self, context):
        self.log.info('Sensor checks existence of: <gs://%s/%s>', self.bucket, self.prefix)
        hook = GoogleCloudStorageHook(
            google_cloud_storage_conn_id=self.google_cloud_conn_id,
            delegate_to=self.delegate_to)
        return len(hook.list(self.bucket, prefix=self.prefix)) > 0

