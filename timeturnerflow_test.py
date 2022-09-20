from metaflow import FlowSpec, step, IncludeFile

import numpy as np
import math
import pandas as pd
import re
from datetime import date, datetime, timedelta
import json

class TimeTurnerFlow(FlowSpec):
    """
    A flow to generate some statistics about the movie genres.

    The flow performs the following steps:
    1) Ingests a CSV into a Pandas Dataframe.
    2) Fan-out over genre using Metaflow foreach.
    3) Compute quartiles for each genre.
    4) Save a dictionary of genre specific statistics.

    """

#     @conda(libraries={"pandas": "1.4.2"})
    @step
    def start(self):
        import pandas as pd
        import boto3
        from io import StringIO

        BUCKET_NAME = 'hbo-ingest-datascience-content-dev'
        bucket_key = f'psi_first_views/test.csv'
        csv_buffer=StringIO()
        
        
        data = [1,2,3,4,5]
        self.df = pd.DataFrame(data, columns=['a'])
  
        self.df.to_csv(csv_buffer, index=True)
        s3_client = boto3.client("s3", region_name="us-east-1")
        response=s3_client.put_object(Body=csv_buffer.getvalue(),
            Bucket=BUCKET_NAME,
            Key=bucket_key)
        print("response from writing csv file to S3:", response)
        self.next(self.end)

    @step
    def end(self):
        """
        End the flow.

        """
        pass


if __name__ == "__main__":
    TimeTurnerFlow()
