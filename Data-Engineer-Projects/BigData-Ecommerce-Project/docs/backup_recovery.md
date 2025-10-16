# Data Backup and Recovery Procedures

## Overview

This document outlines comprehensive backup and recovery procedures for the e-commerce streaming pipeline, ensuring data durability and business continuity.

## 🗂️ Backup Strategy

### 1. Data Classification

#### Critical Data (RTO: < 1 hour, RPO: < 15 minutes)
- **Real-time events** in Pub/Sub (processed within window)
- **Pipeline state** and checkpoints
- **Configuration** and deployment metadata

#### Important Data (RTO: < 4 hours, RPO: < 1 hour)
- **Aggregated results** in BigQuery
- **Data quality metrics** and validation results
- **Monitoring data** and logs

#### Reference Data (RTO: < 24 hours, RPO: < 6 hours)
- **Historical logs** and audit trails
- **Performance metrics** and trends
- **Cost data** and billing information

### 2. Backup Schedules

#### Real-time Backups
- **Pub/Sub messages**: Retained for 7 days (default)
- **Pipeline checkpoints**: Continuous via Dataflow
- **BigQuery streaming buffer**: Automatic

#### Daily Backups
- **BigQuery tables**: Automated snapshots at 02:00 UTC
- **Bigtable tables**: Automated backups at 03:00 UTC
- **Configuration**: Git repository backup

#### Weekly Backups
- **Full system configuration** backup
- **Performance baseline** capture
- **Security configuration** verification

## 💾 Backup Procedures

### 1. BigQuery Backup

#### Automated Daily Backups
```bash
#!/bin/bash
# scripts/backup_bigquery.sh

PROJECT_ID=$1
DATASET_NAME="ecommerce"

# Create timestamped backup dataset
BACKUP_DATASET="${DATASET_NAME}_backup_$(date +%Y%m%d_%H%M%S)"

# Create backup dataset
bq mk --dataset --project_id $PROJECT_ID $BACKUP_DATASET

# Copy all tables with data
for table in product_views_summary sales_summary inventory_summary; do
    bq cp --project_id $PROJECT_ID "$PROJECT_ID:$DATASET_NAME.$table" "$PROJECT_ID:$BACKUP_DATASET.$table"
done

echo "BigQuery backup completed: $BACKUP_DATASET"
```

#### Manual Backup for Critical Events
```bash
# Create point-in-time backup before major changes
python scripts/backup_bigquery.py --project $PROJECT_ID --create-snapshot --retention-days 30
```

### 2. Bigtable Backup

#### Automated Backups
```bash
#!/bin/bash
# scripts/backup_bigtable.sh

PROJECT_ID=$1
INSTANCE_NAME=$2
TABLE_NAME="product_stats"

# Create backup
BACKUP_NAME="${TABLE_NAME}_backup_$(date +%Y%m%d_%H%M%S)"

gcloud bigtable backups create $BACKUP_NAME \
    --project $PROJECT_ID \
    --instance $INSTANCE_NAME \
    --table $TABLE_NAME \
    --retention-period 30d

echo "Bigtable backup created: $BACKUP_NAME"
```

### 3. Configuration Backup

#### Automated Git Backup
```bash
#!/bin/bash
# scripts/backup_configuration.sh

# Commit and push all changes
git add .
git commit -m "Automated backup: $(date)"
git push origin main

# Create backup branch for major releases
BRANCH_NAME="backup/$(date +%Y%m%d_%H%M%S)"
git checkout -b $BRANCH_NAME
git push origin $BRANCH_NAME
```

### 4. Pipeline State Backup

#### Dataflow Job Export
```bash
#!/bin/bash
# scripts/backup_pipeline_state.sh

PROJECT_ID=$1
JOB_ID=$2

# Export job configuration and state
gcloud dataflow jobs describe $JOB_ID \
    --project $PROJECT_ID \
    --region $REGION \
    --format json > "pipeline_state_$(date +%Y%m%d_%H%M%S).json"

# Export custom metrics and counters
gcloud monitoring metrics list \
    --project $PROJECT_ID \
    --filter 'resource.labels.job_id="'$JOB_ID'"' \
    --format json > "pipeline_metrics_$(date +%Y%m%d_%H%M%S).json"
```

## 🔄 Recovery Procedures

### 1. Disaster Recovery Levels

#### Level 1: Component Failure (RTO: < 15 minutes)
**Triggers:**
- Single Dataflow job failure
- BigQuery table corruption
- Bigtable instance issues

**Recovery Steps:**
1. **Identify failed component**
   ```bash
   # Check pipeline status
   python scripts/validate_environment.py

   # Check specific component
   gcloud dataflow jobs list --project $PROJECT_ID --filter "state=Failed"
   ```

2. **Automated recovery** (if enabled)
   ```bash
   # Restart failed job
   python run.py flex --restart-failed
   ```

3. **Manual recovery**
   ```bash
   # Scale up resources
   python scripts/blue_green_deploy.py --project $PROJECT_ID --rollback

   # Restore from recent backup if needed
   python scripts/restore_bigquery.py --project $PROJECT_ID --from-backup $BACKUP_ID
   ```

#### Level 2: Regional Outage (RTO: < 1 hour)
**Triggers:**
- GCP region unavailable
- Network connectivity issues
- Multi-component failures

**Recovery Steps:**
1. **Failover to secondary region**
   ```bash
   # Switch to backup region
   python scripts/regional_failover.py --project $PROJECT_ID --failover-to europe-west1
   ```

2. **Restore from cross-region backups**
   ```bash
   # Restore BigQuery
   python scripts/restore_bigquery.py --project $PROJECT_ID --cross-region

   # Restore Bigtable
   python scripts/restore_bigtable.py --project $PROJECT_ID --cross-region
   ```

#### Level 3: Complete System Loss (RTO: < 4 hours)
**Triggers:**
- Project deletion or corruption
- Widespread GCP service issues
- Security incident requiring full rebuild

**Recovery Steps:**
1. **Emergency response activation**
   ```bash
   # Notify stakeholders
   python scripts/emergency_notification.py --level 3 --project $PROJECT_ID

   # Activate emergency procedures
   python scripts/emergency_recovery.py --project $PROJECT_ID --full-restore
   ```

2. **Full system restoration**
   ```bash
   # Restore from latest backup
   python scripts/full_system_restore.py --project $PROJECT_ID --backup-id $LATEST_BACKUP

   # Validate system integrity
   python scripts/validate_system_integrity.py --project $PROJECT_ID
   ```

### 2. Data Recovery Procedures

#### BigQuery Recovery
```bash
#!/bin/bash
# scripts/restore_bigquery.sh

PROJECT_ID=$1
SOURCE_BACKUP=$2
TARGET_DATASET=$3

# Restore from backup
bq cp --project_id $PROJECT_ID "$PROJECT_ID:$SOURCE_BACKUP.*" "$PROJECT_ID:$TARGET_DATASET"

# Verify data integrity
python scripts/verify_data_integrity.py --dataset $TARGET_DATASET --project $PROJECT_ID
```

#### Bigtable Recovery
```bash
#!/bin/bash
# scripts/restore_bigtable.sh

PROJECT_ID=$1
INSTANCE_NAME=$2
BACKUP_NAME=$3

# Restore from backup
gcloud bigtable instances tables restore $BACKUP_NAME \
    --project $PROJECT_ID \
    --instance $INSTANCE_NAME \
    --table $TABLE_NAME

# Verify table integrity
python scripts/verify_bigtable_integrity.py --instance $INSTANCE_NAME --project $PROJECT_ID
```

#### Pipeline State Recovery
```bash
#!/bin/bash
# scripts/restore_pipeline_state.sh

PROJECT_ID=$1
STATE_BACKUP_FILE=$2

# Restore pipeline configuration
python scripts/restore_pipeline_config.py --from-file $STATE_BACKUP_FILE --project $PROJECT_ID

# Restart pipeline with restored state
python run.py flex --use-state-backup $STATE_BACKUP_FILE
```

## 📊 Monitoring and Validation

### 1. Backup Monitoring

#### Automated Checks
```bash
#!/bin/bash
# scripts/monitor_backups.sh

# Check BigQuery backup status
bq ls --project_id $PROJECT_ID --dataset_id $BACKUP_DATASET

# Check Bigtable backup status
gcloud bigtable backups list --project $PROJECT_ID --instance $INSTANCE_NAME

# Verify backup integrity
python scripts/verify_backup_integrity.py --project $PROJECT_ID
```

#### Backup Success Metrics
- **Backup completion rate**: > 99.5%
- **Backup verification rate**: > 99.9%
- **Recovery success rate**: > 99.9%
- **Data loss incidents**: < 1 per year

### 2. Recovery Testing

#### Regular Recovery Drills
- **Monthly**: Test BigQuery table recovery
- **Quarterly**: Test Bigtable instance recovery
- **Annually**: Full system recovery test

#### Automated Recovery Testing
```bash
#!/bin/bash
# scripts/test_recovery.sh

# Create test environment
python scripts/create_test_environment.py --project $PROJECT_ID --suffix test

# Run sample data through pipeline
python scripts/generate_test_data.py --project $PROJECT_ID --events 1000

# Test recovery procedures
python scripts/test_bigquery_recovery.py --project $PROJECT_ID --test-env test
python scripts/test_bigtable_recovery.py --project $PROJECT_ID --test-env test

# Clean up test environment
python scripts/cleanup_test_environment.py --project $PROJECT_ID --suffix test
```

## 🚨 Emergency Procedures

### 1. Data Loss Incident Response

#### Immediate Actions (First 15 minutes)
1. **Stop data processing** to prevent further corruption
   ```bash
   python scripts/emergency_stop.py --project $PROJECT_ID --reason "data_loss_investigation"
   ```

2. **Isolate affected systems**
   ```bash
   python scripts/isolate_system.py --project $PROJECT_ID --components bigquery,bigtable
   ```

3. **Notify stakeholders**
   ```bash
   python scripts/emergency_notification.py --project $PROJECT_ID --severity critical --message "Data loss detected"
   ```

#### Investigation (15-60 minutes)
1. **Assess data loss scope**
   ```bash
   python scripts/assess_data_loss.py --project $PROJECT_ID --timeframe "last_24h"
   ```

2. **Identify root cause**
   ```bash
   python scripts/analyze_incident.py --project $PROJECT_ID --incident-id $INCIDENT_ID
   ```

#### Recovery (1-4 hours)
1. **Restore from backup**
   ```bash
   python scripts/emergency_restore.py --project $PROJECT_ID --backup-id $LATEST_VALID_BACKUP
   ```

2. **Validate data integrity**
   ```bash
   python scripts/validate_restored_data.py --project $PROJECT_ID --expected-rows $EXPECTED_COUNT
   ```

3. **Resume operations**
   ```bash
   python scripts/resume_operations.py --project $PROJECT_ID --validation-passed
   ```

### 2. Security Incident Response

#### Immediate Actions
1. **Secure the environment**
   ```bash
   python scripts/security_lockdown.py --project $PROJECT_ID --isolate-network
   ```

2. **Preserve evidence**
   ```bash
   python scripts/preserve_forensics.py --project $PROJECT_ID --incident-id $INCIDENT_ID
   ```

3. **Notify security team**
   ```bash
   python scripts/security_notification.py --project $PROJECT_ID --severity critical
   ```

## 📋 Maintenance Procedures

### 1. Backup Cleanup

#### Automated Cleanup
```bash
#!/bin/bash
# scripts/cleanup_old_backups.sh

# Remove BigQuery backups older than 90 days
bq ls --project_id $PROJECT_ID --dataset_id $BACKUP_DATASET | grep "$(date -d '90 days ago' +%Y%m%d)" | awk '{print $1}' | xargs -I {} bq rm --project_id $PROJECT_ID $BACKUP_DATASET.{}

# Remove Bigtable backups older than 30 days
gcloud bigtable backups list --project $PROJECT_ID --instance $INSTANCE_NAME --filter 'create_time < "-30d"' --format "value(name)" | xargs -I {} gcloud bigtable backups delete {} --project $PROJECT_ID --instance $INSTANCE_NAME
```

### 2. Backup Validation

#### Regular Validation Checks
```bash
#!/bin/bash
# scripts/validate_backups.sh

# Test BigQuery backup restoration
python scripts/test_bigquery_restore.py --project $PROJECT_ID --dry-run

# Test Bigtable backup restoration
python scripts/test_bigtable_restore.py --project $PROJECT_ID --dry-run

# Verify backup metadata
python scripts/verify_backup_metadata.py --project $PROJECT_ID
```

## 📚 Documentation and Training

### 1. Recovery Documentation
- [BigQuery Recovery Guide](docs/bigquery_recovery.md)
- [Bigtable Recovery Guide](docs/bigtable_recovery.md)
- [Pipeline State Recovery Guide](docs/pipeline_recovery.md)

### 2. Training Materials
- Recovery procedure walkthroughs
- Emergency response simulations
- Backup validation tutorials

## 🔄 Update Procedures

This document should be reviewed and updated:
- After any backup or recovery incident
- When new data stores are added
- When recovery procedures change
- At least quarterly

**Last Updated:** 2025-01-01
**Next Review:** 2025-04-01

## 📞 Emergency Contacts

**Technical Emergencies:**
- On-call Engineer: data-eng-team@yourcompany.com
- SRE Team: sre@yourcompany.com
- Emergency Line: +1-555-DATA-RECOVERY

**Business Emergencies:**
- Business Operations: business-ops@yourcompany.com
- Executive Team: executives@yourcompany.com

**External Support:**
- GCP Support: https://cloud.google.com/support
- Data Recovery Specialists: recovery-team@yourcompany.com