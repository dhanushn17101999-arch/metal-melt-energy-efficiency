# Metal Melt Energy Efficiency Analysis
## Overview

This project analyses energy efficiency in the UK metals sector (iron, steel, and non-ferrous metals) using datasets from ECUK (UK Government) and Eurostat.

The objective is to understand how energy consumption, output, and efficiency have evolved over time in metal recycling and melting processes, and to benchmark the UK against EU countries.

A complete data pipeline was developed in Python, and results were visualised using Tableau.

## Objectives

* Analyse energy consumption vs output trends
* Evaluate efficiency improvements in metal melting processes
* Identify anomalies and variability (e.g., 2009 disruption)
* Benchmark UK performance against EU countries
* Build an interactive dashboard for analysis

## Tech Stack

* Python (Pandas, NumPy)
* Tableau
* Excel (ECUK dataset)

##  Data Pipeline

The script `metal_melt_pipeline.py` performs the following:

### Data Cleaning

* Handles missing values (`:`, `..`, `x`, `n/a`)
* Converts mixed data types into numeric format
* Extracts metals sector data from complex ECUK Excel structure

### Data Transformation

* Calculates **energy efficiency proxy**:

 Efficiency = 1 / (consumption per unit output)
* Rebases UK intensity index (2000 = 100) → EEDI05 (2005 = 100)

### Data Integration

* Combines:

  * UK data (ECUK)
  * EU data (Eurostat)
* Creates a unified dataset for benchmarking

### Output Generation

The pipeline generates the following datasets:

| File                              | Description                             |
| --------------------------------- | --------------------------------------- |
| `uk_metals_intensity_clean.csv`   | Clean UK metals sector data             |
| `eu_eedi05_benchmark_clean.csv`   | EU + UK efficiency benchmark            |
| `metal_melt_dashboard_master.csv` | Final dataset for Tableau dashboard     |
| `data_quality_log.csv`            | Data validation and completeness report |

## Project Structure

```
metal-melt-energy-efficiency/
│
├── metal_melt_pipeline.py
├── cleaned_data/
│   ├── uk_metals_intensity_clean.csv
│   ├── eu_eedi05_benchmark_clean.csv
│   ├── metal_melt_dashboard_master.csv
│   └── data_quality_log.csv
```

## Dashboard

 [https://public.tableau.com/your-link](https://public.tableau.com/app/profile/dhanush.nagaraj3098/viz/UKMetalsEnergyEfficiencyEUBenchmarkAnalysis/Dashboard1?publish=yes)


## Key Insights

* Energy consumption reduced significantly over time
* Output remained stable → improved efficiency
* UK shows moderate performance compared to EU
* Efficiency improvements are inconsistent and volatile
* External factors (e.g., financial crisis) influence trends

## Data Quality Checks

A data quality log is generated to ensure reliability of analysis.
It includes:

* Missing value tracking
* Dataset coverage (year range)
* Per-country benchmark validation
* Summary of cleaned datasets


## Limitations

* Data is aggregated at sector level (no furnace-level detail)
* UK benchmark derived due to missing Eurostat data post-Brexit
* External factors influence efficiency trends
* No real-time operational data available


## How to Run

```bash
pip install pandas numpy openpyxl
python metal_melt_pipeline.py
```

## Author

**Dhanush Nagaraj**
MSc Business Analytics
University of Exeter

