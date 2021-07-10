import uuid
from pyspark.sql.functions import udf

class pysparkFunctions:

    def sample_df(df, sample_percent):
        return df.sample(sample_percent,seed=123)

class pythonFunctions:
    @udf
    def generate_uuid():
        return str(uuid.uuid4().hex)