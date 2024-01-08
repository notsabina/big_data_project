
# Project Readme - Big Data Processing Project

## Overview:
This GitHub repository houses the source code and documentation for a comprehensive Big Data processing project. The project involves the extraction, transformation, and loading (ETL) of aviation data, utilizing various tools and technologies in the Big Data ecosystem.

## Project Components:
### Data Acquisition:

Data is initially acquired using Python scripts.
NiFi tool is employed to load and process the data, following a predefined schedule.
### Data Processing Pipeline:

* Data undergoes stages from loading to the bronze layer, involving processes such as staging and archiving.
* Transition to the silver layer involves flattening and alignment with the proposed schema.
* Data in the silver layer is further processed in the gold layer through grouping and filtering.
Finalized data is stored in HBase, ready for consumption by business users.
### Serving Layer:

NiFi script automates the update process in the serving layer, appending new rows to HBase.
A logical key (row{flight_iata}_{timestamp_odlotu_planowego}) facilitates data handling and ensures uniqueness.
### User Interaction:

In the serving layer, users can utilize a notebook for visualizations or independently create new analyses.
### Project Management:

The project is managed by a comprehensive system, enabling efficient data management and analysis using various Big Data tools.
## Getting Started:
To get started with the project, follow these steps:

* Clone the Repository:

    git clone https://github.com/notsabina/big_data_project.git
* Requirements:

    Ensure that the required dependencies and tools (NiFi, Hadoop, HBase, etc.) are installed and configured.
* Run the Scripts:

    Execute the Python scripts for data acquisition and NiFi flows as per the provided documentation.
* Explore the Code:

    Review the source code to understand the data processing pipeline and configurations.
## Documentation:
Refer to the documentation directory for more detailed information about the project components, configurations, and usage guidelines.

