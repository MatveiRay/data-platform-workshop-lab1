# Terraform (BigQuery datasets)

```bash
cd terraform
terraform init
terraform plan -var="project_id=<YOUR_GCP_PROJECT_ID>"
terraform apply -var="project_id=<YOUR_GCP_PROJECT_ID>"
```

Далее создайте таблицы из `../sql/bigquery_ddl.sql` (или через dbt).
