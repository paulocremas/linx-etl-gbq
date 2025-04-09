# Data Pipeline, Database Design and Cloud Integration for E-commerce Analytics

---

## Context

This project was developed at Agência FG, a company specializing in building e-commerce platforms using tools like Linx and VTEX. After delivering the websites, the agency provided marketing campaign services to these e-commerce clients. When I joined, the Business Intelligence (BI) initiative was handled by a single team member who created dashboards in Google Looker and manually processed and loaded data using Google Sheets. The data was limited to basic sales metrics from the stores and consumed nearly 2 hours of daily work from a highly skilled business intelligence specialist [(Damian Carvalho)](https://www.linkedin.com/in/damian-carvalho-business-intelligence/). Dashboards were reviewed daily and played a key role in decision-making.

---

# Outcome

ETL processes were developed for VTEX and Linx e-commerce platforms, running daily via cron on an AWS EC2 instance.
These processes tracked user recurrence and loaded data into Google BigQuery. Real-time email notifications were configured to report process status and eventual errors. 
Additionally, SQL queries integrated GA4 campaign data into the data warehouse, and the processed data was subsequently loaded into Looker for analysis and visualization.
