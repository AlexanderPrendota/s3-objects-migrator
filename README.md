# s3 objects migrator


## How to start

1. First of all please add the env variables :wrench:

| ENV                | Description          |
| -------------------|----------------------|
| AWS_SECRET_TO      | AWS Credential       |
| AWS_SECRET_FROM    | AWS Credential       |
| AWS_KEY_TO         | AWS Credential       |
| AWS_KEY_FROM       | AWS Credential       |
| REGION_FROM        | Region               |
| REGION_TO          | Region               |
| BUCKET_FROM        | Amazon S3 bucket name|
| BUCKET_TO          | Amazon S3 bucket name|

2. Build :hammer:

```bash
./gradlew build
```

3. Run :rocket:

```bash
./gradlew execute
```