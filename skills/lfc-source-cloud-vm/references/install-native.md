# Native Database Package Installation

## MySQL (Ubuntu/Debian)

```bash
ssh -i ~/.ssh/lfc_test_rsa azureuser@${AZURE_PUBLIC_IP}

sudo apt-get update
sudo apt-get install -y mysql-server

sudo systemctl enable mysql
sudo systemctl start mysql

# Configure bind address to allow remote connections
sudo sed -i 's/bind-address.*/bind-address = 0.0.0.0/' /etc/mysql/mysql.conf.d/mysqld.cnf
sudo systemctl restart mysql

# Set root password and create LFC user
sudo mysql -u root <<'EOF'
ALTER USER 'root'@'localhost' IDENTIFIED BY 'set-root-password-here';
CREATE USER IF NOT EXISTS 'root'@'%' IDENTIFIED BY 'set-root-password-here';
GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' WITH GRANT OPTION;
FLUSH PRIVILEGES;
EOF

# Verify
export MYSQL_PWD='set-root-password-here'
mysql -u root -e "SELECT VERSION();"
unset MYSQL_PWD
```

## PostgreSQL (Ubuntu/Debian)

```bash
ssh -i ~/.ssh/lfc_test_rsa azureuser@${AZURE_PUBLIC_IP}

sudo apt-get update
sudo apt-get install -y postgresql postgresql-contrib

sudo systemctl enable postgresql
sudo systemctl start postgresql

# Allow remote connections: only the addresses the firewall opens, over TLS (Ubuntu's package turns ssl on with its own
# certificate; the UC connection trusts it). allowed-cidrs.txt: the laptop's /32 and the workspace's serverless egress,
# one CIDR per line (lfc-source-firewall: serverless-egress.sh <cloud> <region> | grep /). tee -a appends: without -a
# it would replace postgresql.conf.
echo "listen_addresses = '*'" | sudo tee -a /etc/postgresql/*/main/postgresql.conf
while read -r cidr; do echo "hostssl all all $cidr scram-sha-256"; done < allowed-cidrs.txt | sudo tee -a /etc/postgresql/*/main/pg_hba.conf
sudo systemctl restart postgresql

# Set postgres password and create LFC user
sudo -u postgres psql <<'EOF'
ALTER USER postgres WITH PASSWORD 'set-postgres-password-here';
CREATE USER lfc_user WITH PASSWORD 'set-lfc-password-here';
ALTER ROLE lfc_user WITH CREATEDB CREATEROLE;
EOF

# Verify
export PGPASSWORD='set-postgres-password-here'
psql -h localhost -U postgres -c "SELECT VERSION();"
unset PGPASSWORD
```

## SQL Server (Ubuntu - Linux version)

```bash
ssh -i ~/.ssh/lfc_test_rsa azureuser@${AZURE_PUBLIC_IP}

# Add Microsoft SQL Server repository
sudo add-apt-repository "$(wget -qO- https://packages.microsoft.com/config/ubuntu/22.04/mssql-server-2022.list)"

# Install SQL Server
sudo apt-get update
sudo ACCEPT_EULA=Y apt-get install -y mssql-server

# Configure
sudo /opt/mssql/bin/mssql-conf set sqlagent.enabled true
sudo /opt/mssql/bin/mssql-conf set telemetry.customerfeedback false

sudo systemctl restart mssql-server

# Verify
export SQLCMDPASSWORD='set-sa-password-here'
sqlcmd -S localhost -U sa -Q "SELECT @@VERSION"
unset SQLCMDPASSWORD
```

## Post-install networking

### MySQL / PostgreSQL

The services bind to `0.0.0.0` by default (as configured above). Verify:

```bash
sudo netstat -tuln | grep LISTEN
```

### SQL Server

For remote access, enable TCP/IP via SQL Server Configuration Manager or verify it's listening:

```bash
sudo netstat -tuln | grep 1433
```
