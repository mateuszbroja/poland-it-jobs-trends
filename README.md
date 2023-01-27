# Poland IT Jobs Trends
## Architecture

### Ingestion

Data sources:
- justjoin.it
- nofluffjobs.pl

Data Source --> Google Storage --> Google BigQuery

Google BigQuery
Landing:
bronze_justjoinit
bronze_nofluffjobs

- choose columns
- transform columns
- add columns
- rename columns for union
silver_justjoinit
silver_nofluffjobs

- rename columns for union
silver_unioned_jobs

- append to table
gold_jobs


Front-end in JS

All technologies used:
- Python (library: pandas)
- Google Cloud Platform:
    - Cloud Run
    - BigQuery
    - Looker
    - Scheduler
- Docker
- Terraform

TODO:
- Change order - najpierw wszystko do Storage, potem do tabeli, itd.
- backfilling
- dodatkowa tabela ze statystykami
- logging
- IaC
- Docker
- tests
- CI/CD
