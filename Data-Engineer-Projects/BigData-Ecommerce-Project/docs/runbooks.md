# Operational Runbooks & Incident Response

## Overview

This document provides operational procedures for managing the e-commerce streaming pipeline in production. It includes incident response procedures, troubleshooting guides, and operational best practices.

## 🚨 Incident Response Procedures

### 1. Pipeline Health Check

**Trigger:** Pipeline health alerts or monitoring dashboard shows issues

**Severity Levels:**
- 🔴 **CRITICAL**: Pipeline stopped or >50% error rate
- 🟠 **WARNING**: Degraded performance or >10% error rate
- 🟡 **INFO**: Minor issues or maintenance notifications

**Response Steps:**

1. **Check Pipeline Status**
   ```bash
   # Check if pipeline is running
   python scripts/validate_environment.py

   # Check recent logs
   gcloud logging read "resource.type=dataflow_step" --project $PROJECT_ID --limit 50
   ```

2. **Identify Issue Type**
   - **Resource Issues**: Check quotas, permissions, resource availability
   - **Data Issues**: Check data formats, schema changes, source availability
   - **Infrastructure Issues**: Check network, GCP service status

3. **Immediate Actions**
   - **If pipeline is stopped**: Restart with `python run.py flex`
   - **If high error rate**: Check dead letter queue with `make dlq-consumer`
   - **If resource issues**: Check quotas and scaling settings

### 2. Data Quality Issues

**Trigger:** Data quality validation failures or business rule violations

**Response Steps:**

1. **Assess Impact**
   ```bash
   # Check data quality metrics
   python -c "
   from beam.data_quality import DataQualityValidator
   validator = DataQualityValidator()
   print(f'Total events: {validator.metrics.total_events}')
   print(f'Valid events: {validator.metrics.valid_events}')
   print(f'Error rate: {(validator.metrics.invalid_events/validator.metrics.total_events)*100:.2f}%')
   "
   ```

2. **Identify Root Cause**
   - Schema changes in source data
   - New required fields missing
   - Invalid data formats
   - Business rule violations

3. **Containment Actions**
   - Route invalid data to dead letter queue
   - Enable detailed logging for debugging
   - Notify data source owners

4. **Recovery Actions**
   - Fix schema validation rules if needed
   - Update business rules if requirements changed
   - Reprocess valid data from dead letter queue

### 3. Cost Spike Incidents

**Trigger:** Cost monitoring alerts or budget threshold exceeded

**Response Steps:**

1. **Immediate Assessment**
   ```bash
   # Generate current cost report
   python scripts/cost_monitoring.py --project $PROJECT_ID --budget 200.00

   # Check for resource leaks
   gcloud dataflow jobs list --region $REGION --status active
   ```

2. **Identify Cause**
   - Unexpected traffic increase
   - Resource scaling issues
   - Inefficient queries or processing
   - Billing configuration changes

3. **Cost Control Actions**
   - Scale down over-provisioned resources
   - Implement autoscaling policies
   - Optimize BigQuery queries
   - Check for stuck jobs

### 4. Performance Degradation

**Trigger:** High latency or low throughput alerts

**Response Steps:**

1. **Performance Analysis**
   ```bash
   # Run load test to benchmark current performance
   python scripts/load_test.py --project $PROJECT_ID --duration 60 --rate 100
   ```

2. **Bottleneck Identification**
   - Check Dataflow job metrics
   - Monitor Pub/Sub backlog
   - Check BigQuery query performance
   - Review Bigtable throughput

3. **Optimization Actions**
   - Adjust Dataflow worker configuration
   - Optimize window sizes and triggers
   - Implement caching for hot data
   - Scale Bigtable cluster if needed

## 🔧 Troubleshooting Guides

### Common Issues and Solutions

#### 1. Pipeline Won't Start

**Symptoms:**
- `python run.py flex` fails immediately
- Permission errors in logs

**Troubleshooting Steps:**

1. **Check Authentication**
   ```bash
   gcloud auth list
   gcloud config get-value project
   ```

2. **Validate Configuration**
   ```bash
   python scripts/validate_environment.py
   ```

3. **Check Required APIs**
   ```bash
   gcloud services list --enabled | grep -E "(dataflow|bigquery|pubsub|bigtable)"
   ```

4. **Verify Resources Exist**
   ```bash
   # Check topics
   gcloud pubsub topics list --project $PROJECT_ID

   # Check BigQuery dataset
   bq ls --project_id $PROJECT_ID

   # Check Bigtable instance
   gcloud bigtable instances list --project $PROJECT_ID
   ```

#### 2. High Error Rate

**Symptoms:**
- Many events in dead letter queue
- Dataflow job showing high error count

**Troubleshooting Steps:**

1. **Examine Dead Letter Queue**
   ```bash
   make dlq-consumer -- --ack  # Acknowledge and delete after review
   ```

2. **Check Data Formats**
   ```bash
   # Sample recent events
   gcloud pubsub subscriptions pull dead-letter-sub --limit 10 --format json
   ```

3. **Review Schema Validation**
   - Check if source data format changed
   - Verify field requirements
   - Update validation rules if needed

#### 3. High Latency

**Symptoms:**
- Events taking too long to process
- Backlog building up in Pub/Sub

**Troubleshooting Steps:**

1. **Check Resource Utilization**
   ```bash
   # Monitor Dataflow metrics
   gcloud monitoring dashboards create --config-from-file monitoring/dashboard_ecommerce_pipeline.json
   ```

2. **Identify Bottlenecks**
   - Check BigQuery query performance
   - Monitor Bigtable write throughput
   - Review Dataflow worker allocation

3. **Optimization Opportunities**
   - Adjust window sizes
   - Implement batch processing
   - Add caching layers

#### 4. Cost Overruns

**Symptoms:**
- Monthly costs exceeding budget
- Unexpected resource usage

**Troubleshooting Steps:**

1. **Analyze Cost Patterns**
   ```bash
   python scripts/cost_monitoring.py --project $PROJECT_ID --visualize
   ```

2. **Identify Cost Drivers**
   - Check for over-provisioned resources
   - Look for inefficient queries
   - Review data transfer costs

3. **Cost Optimization**
   - Implement autoscaling
   - Use committed use discounts
   - Optimize storage settings

## 📋 Operational Procedures

### Daily Operations

#### 1. Health Monitoring
- Review pipeline metrics dashboard
- Check error rates and latencies
- Monitor resource utilization
- Verify data freshness

#### 2. Data Quality Checks
- Review data quality metrics
- Check for schema violations
- Verify business rule compliance
- Monitor data completeness

#### 3. Cost Monitoring
- Review daily cost reports
- Check budget utilization
- Identify cost optimization opportunities

### Weekly Operations

#### 1. Performance Review
- Analyze throughput and latency trends
- Review resource utilization patterns
- Identify optimization opportunities

#### 2. Data Quality Assessment
- Review data quality trends
- Update validation rules as needed
- Assess business rule effectiveness

#### 3. Cost Optimization
- Review cost trends and forecasts
- Implement cost-saving measures
- Plan capacity adjustments

### Monthly Operations

#### 1. Capacity Planning
- Review growth trends
- Plan resource scaling
- Budget forecasting

#### 2. Process Improvement
- Review incident patterns
- Update operational procedures
- Implement lessons learned

## 🚨 Escalation Procedures

### When to Escalate

**Immediately Escalate:**
- 🔴 Pipeline completely down > 15 minutes
- 🔴 Data loss or corruption detected
- 🔴 Security incident suspected
- 🔴 Budget overrun > 20%

**Escalate Within 1 Hour:**
- 🟠 Pipeline degraded performance > 1 hour
- 🟠 Error rate > 25% for > 30 minutes
- 🟠 Cost spike > 50% above normal

**Escalate Within 4 Hours:**
- 🟡 Persistent minor issues
- 🟡 Resource constraint warnings
- 🟡 Performance degradation trends

### Escalation Contacts

**Level 1: On-Call Engineer**
- Primary: data-eng-team@yourcompany.com
- Backup: sre-team@yourcompany.com
- Emergency: +1-555-DATA-ENG

**Level 2: Technical Lead**
- data-eng-lead@yourcompany.com
- Escalation threshold: > 2 hours unresolved

**Level 3: Engineering Manager**
- data-eng-manager@yourcompany.com
- Escalation threshold: > 4 hours unresolved

**Level 4: Director**
- data-director@yourcompany.com
- Escalation threshold: > 8 hours unresolved

## 📞 Emergency Contacts

**Infrastructure Issues:**
- GCP Support: Contact via GCP Console
- Network Issues: network-team@yourcompany.com

**Data Issues:**
- Data Source Owners: data-sources@yourcompany.com
- Business Stakeholders: business-ops@yourcompany.com

**Security Issues:**
- Security Team: security@yourcompany.com
- Compliance Officer: compliance@yourcompany.com

## 🔄 Post-Incident Review

### Required for All Critical Incidents

1. **Incident Summary**
   - Timeline of events
   - Root cause analysis
   - Impact assessment

2. **Lessons Learned**
   - What went well
   - What could be improved
   - Preventive measures

3. **Action Items**
   - Immediate fixes
   - Long-term improvements
   - Process changes

4. **Follow-up**
   - Track action item completion
   - Update procedures
   - Share learnings with team

## 📚 Reference Documents

- [API Documentation](api_documentation.yaml)
- [Pipeline Architecture](README.md#architecture)
- [Deployment Guide](README.md#deployment)
- [Troubleshooting Guide](README.md#troubleshooting)

## 🔄 Update Procedures

This runbook should be reviewed and updated:
- After any significant incident
- When new components are added
- When procedures change
- At least quarterly

**Last Updated:** 2025-01-01
**Next Review:** 2025-04-01