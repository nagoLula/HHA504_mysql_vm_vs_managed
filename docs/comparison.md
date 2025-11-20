# Comparison Notes: Azure vs GCP Managed MySQL

## Experience Summary
- Setting up **both Azure Database for MySQL and GCP Cloud SQL** was a challenging process.
- The deployments **failed numerous times**, often due to authentication and connectivity errors.
- Each platform required multiple steps for authentication, networking, and database configuration, which made troubleshooting more complex.

## GCP Cloud SQL
- Instance creation and user setup required careful IAM role management.
- Authorizing external IPs was necessary for client connections.
- No SSH access — all interactions had to be through SQL clients or scripts.
- Overall, functional but felt more rigid and less intuitive for me.

## Azure Database for MySQL
- Resource group and server creation were straightforward with the CLI.
- Public access configuration was easier to manage.
- User format was clear once understood.
- Felt more flexible and manageable in terms of workflow and troubleshooting.

## Troubleshooting Reflection
- I experienced **numerous failures** during setup across both platforms.
- I suspect that part of the difficulty came from working on **different computers (including work machines)**, where cached credentials, account contexts, and permission settings did not carry over cleanly.
- Switching between networks (work, home, VPN) likely caused **IP authorization mismatches**, blocking connections even when commands were correct.
- These environmental factors compounded the complexity of IAM and firewall rules, making the process harder to stabilize.

## Personal Reflection
- It was **very difficult** for me to create and configure both platforms, and I experienced repeated failures.
- Despite the challenges, I find **Azure more interesting** and **more manageable** for my style of work.
- Azure’s structure aligns better with how I troubleshoot and document workflows.
- In the future, I would rather continue with **Azure Database for MySQL** as my preferred managed solution.

  ## Overall
- I would try again, a fresh start, the same project, to make sure I understand fully, and just to practice.
