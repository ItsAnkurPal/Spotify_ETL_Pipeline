I engineered a comprehensive ETL (Extract, Transform, Load)
solution for Spotify's top 100 songs playlist utilizing AWS
services. The extraction process was initiated by a scheduled
AWS Lambda function, triggered daily, fetching data through
Spotify's API and seamlessly storing it in an S3 bucket. Once the
raw data was securely deposited, an S3 trigger activated a
second Lambda function for data transformation. This
transformative Lambda function executed a series of operations
to cleanse and enhance the dataset before reloading the refined
data back into the S3 bucket. The entire workflow was
orchestrated with precision, ensuring that the extraction
occurred daily and the subsequent transformations were
seamlessly triggered by the S3 bucket events. For analytics,
Athena was employed, utilizing a catalog for efficient
management and retrieval of valuable insights.
