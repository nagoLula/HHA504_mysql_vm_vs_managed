# HHA504_mysql_vm_vs_managed

## Homework: MySQL on VM vs Managed Service (SQLAlchemy + pandas)

### Cloud & Region

- **Cloud Provider(s)**: Azure (VM + Managed MySQL)
- **Region**: East US (eastus)

### Cloud Providers Considered

- **Azure**: Azure Database for MySQL – Flexible Server
- **GCP**: Cloud SQL for MySQL
- **Oracle Cloud (OCI)**: MySQL Database Service (MDS)

*Final implementation uses Azure for both VM and Managed MySQL.*

---

### Overview

This project compares **MySQL on a self-managed VM** vs **Managed MySQL Service** using:

- **SQLAlchemy** for database connectivity
- **pandas** for data manipulation
- A small **radiology_procedures dataset** for demonstration

---

### Repo Structure

├───docs/comparison.md/setup_notes_managed.md/setup_notes_vm.md/

├───screenshots/managed/vm/

└───scripts/managed_demo.py/vm_demo.py
