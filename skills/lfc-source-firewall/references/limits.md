# Firewall rule limits per target

Firewall rule limits matter only for a source shared by workspaces in many regions:

| Target | Limit | Serverless list for one region | Every region (all clouds) |
|---|---|---|---|
| Azure VM, NSG | 400 CIDRs per rule, many rules | fits | fits (about 760 CIDRs) |
| Azure Postgres Flexible Server | 500 rules | fits (up to about 40) | doesn't fit |
| Azure MySQL Flexible Server | 500 rules | fits | doesn't fit |
| Azure SQL Database | 128 rules | fits | doesn't fit |
| AWS RDS / Aurora security group | 60 inbound rules (default quota) | fits (1 to 5) | doesn't fit |
| GCP Cloud SQL authorized networks | no published limit | fits (3 to 4) | test before relying on it |
| GCP VM, VPC firewall | 5,000 per rule | fits | fits |
