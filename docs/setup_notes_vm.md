# Setup Notes: VM-Based MySQL

This file documents reproducible steps for provisioning and connecting to a **VM-hosted MySQL server**.  
Unlike managed services, you control the OS, MySQL installation, and configuration directly.

---

## VM Provisioning (GCP Example)

- Create a Compute Engine VM in your chosen zone (e.g., `us-west3-c`).
- Use Ubuntu 22.04 LTS as the base image.
- Assign a machine type (e.g., `e2-medium`) and disk size (e.g., 20GB).
- Tag the VM with `mysql-server` for firewall rules.

---

## Firewall Configuration

- Create a firewall rule to allow inbound traffic on port `3306`.
- Attach the rule to VMs tagged with `mysql-server`.
- This enables external clients (SQLTools, Python scripts) to connect.

---

## SSH Access

- Connect to the VM using `gcloud compute ssh`.
- Once inside, update packages and install MySQL server.

---

## MySQL Configuration

- Edit `/etc/mysql/mysql.conf.d/mysqld.cnf`.
- Change `bind-address` from `127.0.0.1` to `0.0.0.0` to allow external connections.
- Restart MySQL service after changes.

---

## Validation

- Connect externally using the VM’s public IP, port `3306`, and MySQL credentials.
- Test with SQLTools or Python (`vm_demo.py`).
- Verify database creation, table creation, and inserts.

---

## Notes

- VM approach requires manual patching, backups, and scaling.
- Full control over OS and MySQL version.
- Useful for learning, troubleshooting, and custom configurations.
- For production, restrict firewall rules to specific IP ranges and enable SSL.
