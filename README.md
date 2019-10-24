# S3 Objects migrator

Migrate objects with metadata between two S3 storage.

## How to start

Please provide AWS Credential and region in env variables.

1. Add the env variables :wrench:

| ENV                | Description          |
| -------------------|----------------------|
| BUCKET_FROM        | Amazon S3 bucket name|
| BUCKET_TO          | Amazon S3 bucket name|
| PREFIX_FROM        | Prefix to file       |
| PREFIX_TO          | Prefix to file       |

The default prefix is empty. If you wanna change the file location just set prefix's names.
The script will replace PREFIX_FROM from BACKET_FROT to PREFIX_TO from BUCKET_TO.
How to prefix works: https://pl.kotl.in/fInJE2a-2.


2. Build :hammer:

```bash
./gradlew build
```

3. Run :rocket:

```bash
./gradlew execute
```