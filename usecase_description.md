Electricity Maps ETL Pipeline
Background
To ensure that your skills align with the expectations for this role, we have prepared a hands‑on technical challenge. If you are selected for the position, the work you complete here will also help us tailor your onboarding experience within the team.

Assignment overview
In this assignment, you will explore electricity flows and production mixes for France (FR).
As a data engineer, your task is to design and build a robust ETL pipeline that enables analysts and data scientists to evaluate France’s energy transition, from fossil fuels to renewable sources.
You will use the Electricity Maps API to build a functioning Python-based ETL pipeline structured according to the Bronze → Silver → Gold medallion architecture.

Deliverables
Please create a GitHub repository containing your full solution and share it with us.
We expect:
• All code used for the assignment
• The ability to run the pipeline end‑to‑end

Prerequisites
Developer environment
A laptop/PC with your favorite IDE installed and a working Python 3 environment for performing ETL operations.
GitHub account
If you don’t already have a GitHub account, sign up for one:
https://github.com/
ElectricityMaps sandbox API access
Sign up for a free developer account: https://www.electricitymaps.com/
And obtain a sandbox API key: https://help.electricitymaps.com/en/articles/13169368-using-a-sandbox-api-key
Technical Requirements
Python Framework Options

Choose any of the following:
•Polars + Delta-RS
•Pandas + Delta-RS
•PySpark

File & Table Formats
Layer
File Format
Table Format
Bronze
Any
n/a
Silver
Parquet
Delta Lake
Gold
Parquet
Delta Lake
Partitioning Requirements
•
Bronze & Silver: year=YYYY / month=MM / day=DD (Use ingestion timestamp for Bronze, data timestamp for Silver)
• Gold: Partitioning strategy is up to you.

Exercises
A. Bronze Layer — Raw Immutable Data
The Bronze layer stores raw, unprocessed data exactly as returned by the API. You will ingest two hourly data streams for France (FR):
• Electricity Flows
• Electricity Mix
Requirements
• Store raw API responses in any format (Delta not required)
• Add minimal metadata:
o Ingestion timestamp
o Source URL
• Partition using ingestion timestamp
• No schema enforcement
B. Silver Layer — Clean, Structured Delta Tables
The Silver layer contains cleaned, normalized, and strongly typed datasets.
Requirements
• Read Bronze files
• Flatten nested JSON structures
• Convert timestamps to proper datetime types
• Enforce schema and data types
• Deduplicate using relevant unique identifiers
• Store results as:
o Delta Lake tables
o Parquet files
• Partition using data timestamps

C. Gold Layer — Business-Ready Data Products
The Gold layer contains refined, analytics-ready datasets built on top of Silver.
Choose at least one of the following data products:
Data Product 1 — Daily Relative Electricity Mix
A table containing:
• Percentage contribution per energy source
• Zone metadata
• Reference timestamps
Data Product 2 — Daily Net Import/Export (MWh) for France
Two tables:
1. Electricity Imports (net MWh, by source zone → France)
2. Electricity Exports (net MWh, by destination zone ← France)
Both must include:
• Calculate net MWh
• Zone metadata
• Reference timestamps
Requirements
• Store in Parquet + Delta Lake
• Use clean, flat schemas (no nesting)

D. Data-to-LLM Architecture & RAG-Enabled Chatbot
Objective
Design a high-level solution architecture that connects the ETL pipeline you built (Bronze → Silver → Gold) to an LLM-based conversational interface.
The chatbot should allow internal users (analysts, managers, engineers) to:
• Ask natural language questions about electricity production, flows, imports/exports, and trends
• Retrieve factual answers grounded in Electricity Maps data
• Use RAG (Retrieval-Augmented Generation) to answer questions based on:
o Curated Electricity Maps datasets (Gold layer)
o FAQs, documentation, and domain knowledge related to electricity maps data
This is a design-focused exercise, not a full chatbot implementation.
Scope & Expectations
You are expected to:
• Design an end-to-end high-level architecture, including:
o Overview of the application layer of the target solution
o Overview of the infrastructure layer of the target solution
• Clearly explain component interactions
• Show how structured ETL outputs and unstructured documentation are combined via RAG
You are not expected to:
• Train an LLM
• Build a production-ready frontend
• Implement a fully working chatbot

What to submit
Your GitHub repository must include:
1. High-Level Solution Architecture
• Target overview and design decisions for:
o application view
o infrastructure view
• Expected document format: Markdown or PDF
2. Code
• ETL pipeline scripts
• Clear modular structure and reusable functions
3. README.md
Must document:
• Installation & environment setup
• How to run the pipeline
• Silver & Gold schema descriptions
4. Sample Outputs
• Example partitioned Bronze raw files
• Silver & Gold tables and partitioned Parquet output
5. Optional Bonus (Extra Points)
• Store the data products in cloud storage (e.g., S3)
• Orchestration logic
• Unit tests for transformations
• Data quality checks & data contracts
• Error handling and API rate-limit retries
• Incremental ingestion logic
• Simple CI/CD pipeline for validation & deployment
