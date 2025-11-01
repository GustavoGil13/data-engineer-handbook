# Week 5 – Data Pipeline Maintenance

## Ownership

| Pipeline | Primary Owner | Secondary Owner |
|-----------|----------------|----------------|
| Profit – Unit-level profit | Ana | João |
| Profit – Aggregate profit | Beatriz | Carlos |
| Growth – Aggregate growth | João | Ana |
| Growth – Daily growth | Carlos | Beatriz |
| Engagement – Aggregate engagement | Ana | Carlos |

---

## On-Call Schedule

| Week | On-Call Engineer | Backup Engineer | Notes |
|------|------------------|------------------|--------|
| Week 1 | Ana | João | |
| Week 2 | Beatriz | Carlos | |
| Week 3 | João | Ana | |
| Week 4 | Carlos | Beatriz | |
| Week 5 (holiday week) | Rotating pair (light duty) | — | Reduced schedule due to holidays |

**Rules:**
- Each engineer rotates weekly.
- If a holiday falls on your week, swap with the next person.
- Backup engineer helps during major incidents.

---

## Runbooks (for Investor-Reported Pipelines)

These runbooks describe **what could go wrong** with the pipelines that report metrics to investors.

### 1. Profit – Aggregate Profit
**Possible Issues:**
- ETL job fails due to missing source data.
- Incorrect currency conversion rates applied.
- Late arrival of sales data from external systems.
- Duplicated rows leading to inflated profit.
- Dashboard not updating due to BI tool sync error.

---

### 2. Growth – Aggregate Growth
**Possible Issues:**
- API rate limit exceeded when collecting usage data.
- Transformation logic error (e.g., wrong date aggregation).
- Data warehouse connection timeout.
- Missing partition in daily load.
- Incorrect handling of new user cohorts.

---

### 3. Engagement – Aggregate Engagement
**Possible Issues:**
- User activity logs delayed from streaming service.
- Schema change in source data not reflected in the pipeline.
- Aggregation step fails due to memory limits.
- Dashboard visualization fails to refresh.
- Missing data for specific platforms (e.g., mobile only).