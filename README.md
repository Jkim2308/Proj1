## Project 1
- Project 1 is a Scala console application that is retrieving data using Hive or MapReduce. Your job is to build a real-time news analyzer. This application should allow users to view the trending topics (e.g. all trending topics for news related to "politics", "tv shows", "movies", "video games", or "sports" only [choose one topic for project]). Or it can analyze data on any topic of your choice.

### Goals:
- ALL user interaction must come purely from the console application
- scrape data from datasets from an API based on your topic of choice
- Also load data from and save data to HDFS
- Your console application must:
    - query data to answer at least 6 analysis questions of your choice
    - tell a story about your dataset
    - have a login system for all users with passwords
        - 2 types of users: BASIC and ADMIN
        - Users should also be able to update username and password
- implement all CRUD operations
- implement bucketing, and partitioning

### Stretch Goals:
- Passwords must be encrypted
- Export all results into a JSON file
- Find a trend
- utilize Apache Airflow for workflow scheduling

### Technologies
- Hadoop MapReduce (optional)
- YARN 
- HDFS
- Scala 2.12 (or 2.13)
- Hive
- Spark SQL
- Git + GitHub
