# Crypto-Data-Pipeline
Data Pipeline for loading scraped data to redshift.

The project involves utilizing various tools and technologies to perform data transformation, scraping, storage, and analysis related to cryptocurrency data. Here's how these tools are being used:

1.Airflow: Airflow is an open-source platform used to programmatically author, schedule, and monitor workflows. It allows the user to define complex data pipelines as directed acyclic graphs (DAGs), making it ideal for data orchestration and transformation. In this project, Airflow is used to schedule and monitor the workflow, ensuring that data transformation tasks are executed in the correct order.

1.EC2 instance: Amazon Elastic Compute Cloud (EC2) is a web service that provides resizable compute capacity in the cloud. In this project, an EC2 instance is used to create the Airflow server, allowing for the scheduling and monitoring of workflows.

2.BeautifulSoup library: BeautifulSoup is a Python library used for web scraping purposes to extract the data from HTML and XML files. In this project, BeautifulSoup is used to extract cryptocurrency data from the crypto.com website, which is then processed further.

3.Pandas library: Pandas is a fast, powerful, flexible, and easy-to-use open-source data analysis and manipulation tool. In this project, Pandas is used to process the extracted cryptocurrency data, creating a DataFrame which is then stored in S3 in CSV format.

4.S3: Amazon Simple Storage Service (S3) is an object storage service that offers industry-leading scalability, data availability, security, and performance. In this project, S3 is used to store incoming and outgoing data, allowing for easy access and retrieval.

5.Lambda: AWS Lambda is a serverless computing service that runs code in response to events and automatically manages the computing resources required by that code. In this project, Lambda is used to pick up incoming data, perform transformations, and then load it into Redshift.

6.AWS EventBridge Scheduler: AWS EventBridge is a serverless event bus service that makes it easy to connect applications together using data from your own applications, integrated Software-as-a-Service (SaaS) applications, and AWS services. In this project, the EventBridge Scheduler is used to create a trigger for Lambda at regular intervals, ensuring that the data transformation process is executed at regular intervals.

7.Redshift: Amazon Redshift is a fully managed, petabyte-scale data warehouse service in the cloud. In this project, Redshift is used as a data warehouse to store the transformed data, making it easy to access and analyze.

8.Power Bi: Power BI is a suite of business analytics tools to analyze data and share insights. In this project, Power Bi is used to analyze data from Redshift and create forecasting charts, providing insights into cryptocurrency data.

In summary, these various tools and technologies are utilized to perform data-related tasks for cryptocurrency data analysis. The project involves using Airflow, EC2, BeautifulSoup, Pandas, S3, Lambda, AWS EventBridge Scheduler, Redshift, and Power Bi to perform data transformation, scraping, storage, and analysis tasks.
