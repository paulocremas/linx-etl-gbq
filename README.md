# Data Pipeline, Database Design and Cloud Integration for E-commerce Analytics

---

## Context


This project was developed at Agência FG, a company specializing in building e-commerce platforms using tools like Linx and VTEX. After delivering the websites, the agency provided marketing campaign services to these e-commerce clients. When I joined, the Business Intelligence (BI) initiative was handled by a single team member who created dashboards in Google Looker and manually processed and loaded data using Google Sheets. The data was limited to basic sales metrics from the stores and consumed nearly 2 hours of daily work from a highly skilled business intelligence specialist [(Damian Carvalho)](https://www.linkedin.com/in/damian-carvalho-business-intelligence/). Dashboards were reviewed daily and played a key role in decision-making.
</br></br>
![Context diagram ETL-FG ](https://github.com/user-attachments/assets/31f7f6fb-3840-468c-a1a9-aaea9f997021)

# Outcome

ETL processes were developed for the VTEX and Linx e-commerce platforms, running daily via cron on an AWS EC2 instance. 
These processes handled data originating from sales transactions, including item details and purchase values, and tracked user recurrence. The data was loaded into Google BigQuery for centralized analysis. Real-time email notifications were configured to report process status and alert for potential errors. Additionally, SQL queries integrated GA4 campaign data into the data warehouse, and the processed datasets were visualized through Looker, enabling detailed analysis of sales performance and marketing attribution.
</br></br>

![Outcome diagram ETL-FG](https://github.com/user-attachments/assets/98b208ee-a326-4dd5-92cc-3ed9a55b9929)
