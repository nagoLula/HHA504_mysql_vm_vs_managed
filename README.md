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

├── docs/ │ ├── comparison.md │ ├── setup_notes_managed.md │ └── setup_notes_vm.md │ ├── screenshots/ │ ├── managed/ │ └── vm/ │ ├── scripts/ │ ├── managed_demo.py │ └── vm_demo.py │ └── videos/ ├── managed_demo.mp4 └── vm_demo.mp4

Code

---

### Videos

- **Managed Demo Video** → shows connection to Azure Database for MySQL (Flexible Server) using SQLAlchemy + pandas.  
- **VM Demo Video** → shows connection to MySQL running on a self‑managed Azure VM.  

Videos are stored in the `videos/` folder and referenced in documentation for clarity.  
You can embed them in markdown like this:

```markdown
## Managed Demo
https://github.com/user-attachments/assets/f0c43311-b1a6-4191-bf25-6c833ea30c53

## VM Demo

https://github.com/user-attachments/assets/18c9cb82-9102-4527-99a1-593da4ef816c

https://github.com/user-attachments/assets/1197d9d6-1d6d-4dfd-9793-c2af3e04e3bd

https://github.com/user-attachments/assets/83b5123b-daaf-4b3a-a499-4ce7695d64f7

