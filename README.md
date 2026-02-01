## E-Commerce Stream Processing Pipeline
<br>

### **Overview**
- This project implements a real-time streaming pipeline that ingests high-velocity e-commerce orders data via Azure Event Hubs and processes it using Spark Structured Streaming on Databricks.
- The pipeline leverages a three-tier Delta Lake approach (Bronze, Silver, and Gold) to transform raw JSON events into cleaned, deduplicated, and aggregated business-ready insights stored in Azure Data Lake Storage (ADLS).

### **Tech Stack**
Python, Faker, Kafka, Azure Event Hubs, Databricks, ADLS Gen2, Spark Structured Streaming.

### **Description**
1. Data Generation (The Producer)
   - Streaming data is generated programmatically using a custom Python producer. The python producer utilizes Faker library to generate e-commerce orders (Order ID, Timestamp, Category, Location, Price, etc.). These events are streamed to Azure Event Hubs using the Kafka protocol.

2. Bronze Layer (Ingestion)
   - The Bronze layer serves as the initial landing zone for the raw data ingested from the streaming platform.
   - In this stage, Azure Databricks connects to the Event Hub to read the data in binary format, casts it to a string, and parses the JSON into a structured format based on a predefined schema.

3. Silver Layer (Transformation)
   - The Silver layer is dedicated to data cleaning, filtering, and enrichment.
   - During this stage, the raw data from the Bronze layer undergoes several transformations to ensure quality:-
      - Null Handling: The system checks for null values in critical columns like price and quantity.
      - Deduplication: Any duplicate records are dropped based on the Order ID.
      - Filtering: The data is filtered to include only relevant records.
      - Data Type Conversion
      - Feature Engineering: New column is derived that adds immediate value for business reporting.

4. Gold Layer (Aggregation)
   - The Gold layer contains business-ready, aggregated data optimized for visualization and analytics.





