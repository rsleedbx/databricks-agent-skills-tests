# Cloud SQL Lifecycle Management

## Stop and start (for cost savings)

Cloud SQL instances can be stopped to pause compute charges (storage charges continue).

### Stop

```bash
gcloud sql instances patch "${INSTANCE_NAME}" \
  --activation-policy=NEVER \
  --project="${PROJECT_ID}"
```

Verification:

```bash
gcloud sql instances describe "${INSTANCE_NAME}" \
  --project="${PROJECT_ID}" \
  --format="value(state)"  # Should show STOPPED
```

### Start

```bash
gcloud sql instances patch "${INSTANCE_NAME}" \
  --activation-policy=ALWAYS \
  --project="${PROJECT_ID}"
```

**Limitations:**
- Stopped instances can remain stopped for up to 90 days before automatic restart by GCP
- Public IP is retained
- Replication slots on PostgreSQL may time out after extended stop (verify after restart)

## Delete the instance

```bash
gcloud sql instances delete "${INSTANCE_NAME}" \
  --quiet \
  --project="${PROJECT_ID}"
```

**Post-deletion:**
- Instance name cannot be reused for ~7 days (GCP reserves it)
- Automated backups are deleted unless you manually export them
- UC connection in Databricks remains but will fail on test

### Delete UC connection

```bash
databricks connections delete "jane-doe-mysql-uscentral1-${EPOCH}" --profile <PROFILE>
```

## Labels and cleanup tracking

Add labels to instances for cost allocation and cleanup:

```bash
gcloud sql instances patch "${INSTANCE_NAME}" \
  --update-labels="env=dev,owner=jane-doe,remove_after=2025-02-15" \
  --project="${PROJECT_ID}"
```

Query all instances with a label:

```bash
gcloud sql instances list \
  --filter="labels.remove_after<2025-02-15" \
  --project="${PROJECT_ID}" \
  --format="table(name, state, createTime)"
```

**Note:** GCP does not provide automatic deletion based on labels. Manual audits or Cloud Scheduler + Cloud Function scripts are required for cleanup.

## Monitoring and quotas

Check current instance count against quota:

```bash
# List instances
gcloud sql instances list --project "${PROJECT_ID}" \
  --format="table(name, databaseVersion, state)"

# Count instances
gcloud sql instances list --project "${PROJECT_ID}" \
  --format="value(name)" | wc -l
```

Default quota is 100 instances per project.
