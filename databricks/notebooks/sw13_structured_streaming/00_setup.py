# Databricks notebook source
# DBTITLE 1,Setup: Secret Scope und Credentials
# MAGIC %md
# MAGIC # Setup: Secret Scope and Credentials
# MAGIC
# MAGIC This notebook creates a secret scope and stores credentials as secrets.
# MAGIC
# MAGIC ## What is a Secret Scope?
# MAGIC
# MAGIC A **Secret Scope** is a secure storage mechanism in Databricks for managing sensitive information like passwords, API keys, tokens, and connection strings. Secrets stored in a scope are encrypted and can be accessed programmatically without exposing the actual values in your code or notebooks.
# MAGIC
# MAGIC **Key benefits:**
# MAGIC * Centralized credential management
# MAGIC * Encrypted storage
# MAGIC * Access control and auditing
# MAGIC * Secrets are never displayed in plain text in notebook outputs
# MAGIC
# MAGIC **Documentation:** [Databricks Secret Management](https://docs.databricks.com/security/secrets/index.html)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Steps:**
# MAGIC 1. Create widgets for credential input
# MAGIC 2. Create secret scope
# MAGIC 3. Save secrets from widget values

# COMMAND ----------

# DBTITLE 1,Widgets erstellen
# Create text widgets for all required parameters
dbutils.widgets.text("service_uri", "", "Service URI")
dbutils.widgets.text("host", "", "Host")
dbutils.widgets.text("port", "", "Port")
dbutils.widgets.text("user", "", "User")
dbutils.widgets.text("password", "", "Password")

print("✓ Widgets created. Please fill in the values above.")

# COMMAND ----------

# DBTITLE 1,Widget-Werte auslesen
# Read values from the widgets
service_uri = dbutils.widgets.get("service_uri")
host = dbutils.widgets.get("host")
port = dbutils.widgets.get("port")
user = dbutils.widgets.get("user")
password = dbutils.widgets.get("password")

# Validation
if not all([service_uri, host, port, user, password]):
    raise ValueError("All fields must be filled out!")

print("✓ All values successfully read")
print(f"  Service URI: {service_uri}")
print(f"  Host: {host}")
print(f"  Port: {port}")
print(f"  User: {user}")
print(f"  Password: {'*' * len(password)}")

# COMMAND ----------

# DBTITLE 1,Secret Scope erstellen
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import ResourceAlreadyExists

w = WorkspaceClient()
scope_name = "secret_scope"

# Create scope (idempotent)
existing = [s.name for s in w.secrets.list_scopes()]
if scope_name in existing:
    print(f"✓ Secret scope '{scope_name}' already exists")
else:
    w.secrets.create_scope(scope=scope_name)
    print(f"✓ Secret scope '{scope_name}' created")

# Prepare secrets dictionary from widget values
secrets = {
    "service_uri": service_uri,
    "host": host,
    "port": port,
    "user": user,
    "password": password
}

# Save all secrets
for key, value in secrets.items():
    w.secrets.put_secret(scope=scope_name, key=key, string_value=value)
    print(f"✓ Secret '{key}' saved")

print(f"\n✓ All secrets successfully saved in scope '{scope_name}'!")

# COMMAND ----------

# DBTITLE 1,Verify Secrets
# Verify saved secrets (only show keys, not values)
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
scope_name = "secret_scope"

# List all secrets in the scope
secret_list = w.secrets.list_secrets(scope=scope_name)

print(f"Secrets in scope '{scope_name}':")
for secret in secret_list:
    print(f"  - {secret.key}")

print("\n✓ Setup completed!")
print("\nThe secrets can now be used in other notebooks with:")
print(f"  dbutils.secrets.get('{scope_name}', 'service_uri')")
print(f"  dbutils.secrets.get('{scope_name}', 'host')")
print(f"  dbutils.secrets.get('{scope_name}', 'port')")
print(f"  dbutils.secrets.get('{scope_name}', 'user')")
print(f"  dbutils.secrets.get('{scope_name}', 'password')")

# COMMAND ----------

# DBTITLE 1,Test Secret Access
# Test: Retrieve secrets to verify they are accessible
scope_name = "secret_scope"

print("Testing secret retrieval...\n")

try:
    # Retrieve all secrets
    test_service_uri = dbutils.secrets.get(scope_name, "service_uri")
    test_host = dbutils.secrets.get(scope_name, "host")
    test_port = dbutils.secrets.get(scope_name, "port")
    test_user = dbutils.secrets.get(scope_name, "user")
    test_password = dbutils.secrets.get(scope_name, "password")
    
    # Function to redact secrets (show first 3 chars, rest as asterisks)
    def redact(value, show_chars=3):
        if len(value) <= show_chars:
            return '*' * len(value)
        return value[:show_chars] + '*' * (len(value) - show_chars)
    
    # Display redacted values
    print("Retrieved secrets (redacted):")
    print(f"  service_uri: {redact(test_service_uri)}")
    print(f"  host: {redact(test_host)}")
    print(f"  port: {redact(test_port)}")
    print(f"  user: {redact(test_user)}")
    print(f"  password: {redact(test_password)}")
    
    # Verify secrets are not empty
    print("\nVerification status:")
    secrets_status = {
        "service_uri": "✓ Retrieved" if test_service_uri else "✗ Empty",
        "host": "✓ Retrieved" if test_host else "✗ Empty",
        "port": "✓ Retrieved" if test_port else "✗ Empty",
        "user": "✓ Retrieved" if test_user else "✗ Empty",
        "password": "✓ Retrieved" if test_password else "✗ Empty"
    }
    
    for key, status in secrets_status.items():
        print(f"  {key}: {status}")
    
    print("\n✓ All secrets are accessible and working correctly!")
    
except Exception as e:
    print(f"✗ Error accessing secrets: {e}")

# COMMAND ----------

