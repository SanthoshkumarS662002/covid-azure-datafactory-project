# Azure End-to-End COVID-19 Data Engineering Project

## 📌 Project Overview

This is a hands-on data engineering project I built while learning Azure Data Factory (ADF) as part of my transition into data engineering (following Ramesh Retnasamy's ADF course on Udemy). Rather than just following along, I used the course project as a base to explore the full Azure data ecosystem end-to-end — ingestion, transformation, orchestration, and visualization — on real COVID-19 reporting, hospitalization, and testing datasets.

The goal was to move raw data from external HTTP sources and Azure Blob Storage into a clean, structured Azure SQL Database, and finally serve it through Power BI dashboards, while getting practical exposure to ADF pipelines, Data Flows, Databricks/PySpark, and trigger-based orchestration.

---

## 🏗️ Solution Architecture

```
[ Source 1: 4 CSVs over HTTP (GitHub) ] ──┐
                                          ├──► [ ADF Ingestion Pipeline ] ──► [ ADLS Gen2 - Raw Container ]
[ Source 2: 1 TSV uploaded to Blob ]     ──┘
                                                          │
                        ┌─────────────────────────────────┴─────────────────────────────────┐
                        ▼                                                                   ▼
          [ ADF Mapping Data Flows ]                                              [ Azure Databricks ]
        (Easy–Medium Transformations)                                          (Complex PySpark Notebooks)
                        │                                                                   │
                        └─────────────────────────────┬─────────────────────────────────────┘
                                                        ▼
                                          [ ADLS Gen2 - Transformed Container ]
                                                        ▼
                                          [ ADF Copy Activity → Azure SQL DB ]
                                                        ▼
                                                [ Azure SQL Database ]
                                                        ▼
                                              [ Power BI Dashboards ]
```

---

## 🛠️ Tech Stack & Key Concepts

- **Orchestration & Data Integration:** Azure Data Factory (ADF)
  - Control Flow: Lookup, ForEach, Copy Activity, Databricks Notebook Activity
  - Triggers: Tumbling Window Triggers, Parent-Child Dependency, Downstream Trigger Alignment
- **Transformation Engines:**
  - ADF Mapping Data Flows — code-free, UI-driven transformations
  - Azure Databricks (PySpark) — custom/complex transformation logic
- **Storage & Serving Layer:**
  - Azure Data Lake Storage Gen2 (ADLS Gen2) — raw and transformed containers
  - Azure Blob Storage — landing zone for the uploaded TSV source
  - Azure SQL Database — serving layer for downstream querying and reporting
- **Security & Authentication:** Service Principal (App Registration) credentials, configured directly within the Databricks notebook to connect to ADLS Gen2 (no Key Vault used in this iteration — a planned next step)
- **Visualization:** Power BI Desktop, connected directly to Azure SQL Database

---

## 🚀 Step-by-Step Implementation

### Phase 1: Ingestion & Metadata-Driven Pipelines (ADF)

**Sources:**
1. Four CSV datasets hosted on HTTP endpoints (course instructor's GitHub repo)
2. One TSV dataset uploaded directly to Azure Blob Storage

**What I learned and built:**
- Got hands-on with the core building blocks of ADF — Linked Services, Datasets, and Pipelines — and how they connect to external sources.
- Hit an early issue where ADF was ingesting the raw HTML page of the GitHub URL instead of the actual file content. Resolved it by pointing the dataset to the raw file endpoint instead of the webpage URL.
- Instead of building four separate pipelines for the four CSV sources (which would've meant repetitive, hard-to-maintain pipelines), I designed a **metadata-driven pipeline**: a single JSON file holding the source names, base URLs, and relative URLs, read via a **Lookup** activity and looped over with a **ForEach** activity that triggers a parameterized **Copy Activity** for each source.
- All five raw datasets landed successfully in the **raw container** of ADLS Gen2.

### Phase 2: Data Transformation — Data Flows vs. Databricks

To compare the two transformation approaches available in ADF, I intentionally split the workload:

**1. ADF Mapping Data Flows (2 datasets)**
- Built transformations using ADF's code-free, drag-and-drop UI.
- Takeaway: Data Flows are fast and intuitive for easy-to-medium transformations (column renaming, type casting, basic aggregations), but get harder to manage once the transformation logic gets more complex.

**2. Azure Databricks + PySpark (2 datasets)**
- Set up Databricks clusters and wrote PySpark notebooks for the more complex transformations.
- Connected Databricks to ADLS Gen2 using **Service Principal** credentials (Client ID, Tenant ID, Secret), configured directly inside the notebook for simplicity (not via Key Vault, in this version).
- Orchestrated and automated notebook execution from ADF using the **Databricks Notebook Activity**.

### Phase 3: SQL Database Loading & Trigger Orchestration

- Created tables in Azure SQL Database to host the transformed datasets.
- Used **ADF Copy Activities** to load the transformed output files from ADLS Gen2 into Azure SQL Database.
- Implemented **Tumbling Window Triggers** with parent-child dependency configuration, so downstream pipelines only fire once the upstream ingestion trigger completes successfully.

### Phase 4: Business Intelligence & Power BI Visualization

- Connected Power BI Desktop directly to the Azure SQL Database.
- Built a 3-page interactive dashboard:
  1. **Executive Overview** — global case volume and mortality trends over time
  2. **Healthcare Infrastructure Strain** — daily hospital bed and ICU occupancy levels
  3. **Testing Surveillance** — weekly positivity rates and testing coverage per population

---

## ⚡ Technical Challenges & Solutions

| # | Challenge | Root Cause | Solution |
|---|-----------|------------|----------|
| 1 | Ingesting HTML instead of CSV | ADF's HTTP source pointed to the GitHub UI page, not the raw file | Reconfigured the dataset's relative URL to point to the raw file endpoint |
| 2 | Redundant, repetitive pipelines | One pipeline per CSV source created clutter and maintenance overhead | Built a metadata-driven pipeline using a JSON config file with Lookup + ForEach + parameterized Copy Activity |
| 3 | Power BI couldn't connect to Azure SQL DB | Azure SQL Server firewall blocked external connections by default | Whitelisted my local machine's IP in the Azure SQL firewall settings |
| 4 | Tumbling Window trigger not firing downstream | Boundary/offset misalignment between parent and child trigger start times | Aligned UTC start times and adjusted the dependency offset (00:00:00 / -00:18:03) |

---

## 💡 Key Takeaways

1. **Metadata-driven design saves time and scales better** — abstracting source details into a JSON config instead of hardcoding pipelines makes the solution modular and much easier to maintain.
2. **Right tool for the right job** — ADF Data Flows are great for quick, UI-driven transformations, while PySpark on Databricks gives far more control and flexibility for complex logic.
3. **Data engineering is more than pipeline code** — firewall rules, service principal auth, trigger dependencies, and schema design are all just as critical to a reliable end-to-end pipeline.

---

## 🔜 What's Next

- Move Service Principal credentials out of the notebook and into **Azure Key Vault** for secure secret management.
- Explore parameterizing the Databricks notebooks further and adding data quality checks between layers.

---

## 🙏 Acknowledgements

This project was built while following the **Azure Data Factory course by Ramesh Retnasamy on Udemy**, extended with my own exploration of Databricks, trigger orchestration, and Power BI reporting on top of the course material.
