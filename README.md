# PySpark Data Processing Application

## Overview

The PySpark Data Processing Application is a Python-based tool designed for the efficient processing and analysis of large datasets using Apache PySpark. Tailored for a hypothetical company named KommatiPara, this application focuses on the cleansing, filtering, merging, and preparation of client and financial data for enhanced marketing strategies and operational efficiency.

## Key Features

- **Data Cleansing:** Safeguard privacy by removing sensitive personal and financial information.
- **Country-Based Filtering:** Selectively process records based on client geography, focusing on strategic markets.
- **Efficient Data Merging:** Leverage common identifiers to unify client and financial datasets into a comprehensive overview.
- **Intuitive Column Renaming:** Transform dataset columns into more readable and meaningful names for business use.
- **Robust Logging:** Track and monitor the application's process flow to ensure reliability and facilitate debugging.

## Usage

Execute the application by running the `main.py` script with the appropriate datasets and parameters. It's designed to be flexible, allowing for customization based on specific data processing needs.

Example of running from the root directory:

```bash
spark-submit app/main.py \
    --dataset_one_path "input_data/dataset_one.csv" \
    --dataset_two_path "input_data/dataset_two.csv" \
    --countries "United Kingdom" "Netherlands"
```

## Notes

The project also uses GitHub Actions for CI pipeline which runs tests and generates a wheel package if tests are successful.

Example of such pipeline run can be found [here](https://github.com/Valkoiset/assignment/actions/runs/8089343213).
