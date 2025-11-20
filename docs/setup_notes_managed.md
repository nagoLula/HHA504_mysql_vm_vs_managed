# Managed MySQL Setup Notes

## GCP Cloud SQL

- Create a Cloud SQL instance in your chosen region.
- Add a database (e.g., `claims_db`).
- Create a user with a secure password.
- Authorize your workstation’s IP for external connections.
- Connect using SQL clients or Python scripts — no SSH access.

## Azure Database for MySQL

- Create a resource group in your region.
- Provision a Flexible Server with admin credentials.
- Add a database (e.g., `claims_db`).
- Configure public access or restrict to specific IP ranges.
- Connect using SQL clients or Python scripts — no SSH access
