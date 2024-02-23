# Project Title

Pinterest Data Pipeline Project

## Table of Contents
- [Description](#description)
- [Installation Instructions](#installation-instructions)
- [Usage Instructions](#usage-instructions)
- [File Structure](#file-structure)
- [License](#license)

## Description

This project implements a data pipeline for handling Pinterest data using various AWS services. The pipeline involves batch processing, streaming data, and orchestration of Databricks workloads. It includes the following milestones:

1. **Set up GitHub Repository**
   - Created a GitHub repository to track changes and store the project code.

2. **Set up AWS**
   - Created a new AWS account for cloud services usage.

3. **Get Started**
   - Downloaded Pinterest infrastructure and signed in to the AWS console.

4. **Batch Processing: Configure the EC2 Kafka client**
   - Configured an EC2 instance for Kafka client usage.

5. **Batch Processing: Connect an MSK cluster to an S3 bucket**
   - Connected an MSK cluster to an S3 bucket for storing data.

6. **Batch Processing: Configuring an API in API Gateway**
   - Created an API in API Gateway for sending data to the MSK cluster.

7. **Batch Processing: Databricks**
   - Set up Databricks, mounted an S3 bucket, and read data from AWS into Databricks.

8. **Batch Processing: Spark on Databricks**
   - Cleaned and performed computations on data using Spark on Databricks.

9. **Batch Processing: AWS MWAA**
   - Orchestrated Databricks workloads on AWS MWAA.

10. **Stream Processing: AWS Kinesis**
    - Sent streaming data to Kinesis, read and transformed the data in Databricks.

11. **Complete Project**
    - Documented the entire project, including code updates to GitHub.

## Installation Instructions

1. **GitHub Repository:**
   - Clone the GitHub repository.

2. **AWS Account:**
   - Set up an AWS account using the provided instructions.

3. **AWS Services:**
   - Follow the specific instructions in each milestone to configure AWS services.

## Usage Instructions

1. **Batch Processing:**
   - Execute batch processing tasks as described in each milestone.

2. **Stream Processing:**
   - Follow steps for streaming data to Kinesis and processing it in Databricks.

3. **Orchestration:**
   - Set up and trigger Databricks workloads using MWAA.

## File Structure

The project structure is organized based on the different milestones and tasks. Below is an overview:

- /scripts
  - user_posting_emulation.py
  - user_posting_emulation_streaming.py
- /dags
  - 0e6999790cc9.py>
- /notebooks
  - s3_bucket_to_databricks.ipynb
  - transform_and_query_batch_data.ipynb
  - transforming_and_querying_the_streaming_data.ipynb
- README.md

## License

This project is licensed under the [MIT License](LICENSE).





