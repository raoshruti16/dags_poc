import pandas as pd
import glob
from datetime import datetime
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class CustomCleaningOperator(BaseOperator):
    @apply_defaults
    def __init__(self, datasets_path, **kwargs):
        super().__init__(**kwargs)
        self.datasets_path = datasets_path

    def execute(self, context):
        ti = context['ti']
        
        combined_df = pd.DataFrame()

        for file in glob.glob(self.datasets_path):
            df = pd.read_csv(file)
            combined_df = pd.concat([combined_df, df], ignore_index=True)

        df = combined_df.dropna()

        ti.xcom_push(key='cleaned_data', value=df.to_json())