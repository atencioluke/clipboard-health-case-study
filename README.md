# Clipboard Health Data Analyst Case Study
This case study focuses on extracting agent performance metrics from Zendesk's data. Your task is to write SQL queries that can accurately gather these metrics.

#### Zendesk Data Structures
Zendesk primarily organizes its data in multiple tables, but for the purpose of this case study we will focus on the following tables and fields:
- “ZD_Tickets” Table:
    - Key Fields: ID, CREATED_AT, UPDATED_AT, ASSIGNEE_ID, STATUS, SATISFACTION_RATING
    - Description: Contains comprehensive details about customer tickets.
- “ZD_Users” Table:
    - Key Fields: ID, NAME, ROLE
    - Description: Details about users, including agents handling the tickets. 
- “ZD_Ticket_Metrics” Table:
    - Key Fields: TICKET_ID, REPLY_TIME_IN_MINUTES, FIRST_RESOLUTION_TIME_IN_MINUTES
    - Description: Tracks specific metrics related to each ticket, like reply times and resolution times.

You can view some sample data [here](./Sample%20Data/).

#### Your Task

Imagine these tables are located in a database labeled HEVO_DATABASE and within the MONGO_MAIN_APP schema.

#### Guidelines
- Create an SQL query to determine the average first reply time and average first resolution time by month for the past 12 months.
- Create an SQL query that provides average satisfaction score (as calculated by Zendesk) and total tickets solved by agent by month for the last 6 months.
- Use JOIN clauses to combine data from different tables where necessary. Some of the columns are JSON objects (satisfaction score, reply/resolution times).
- Ensure your queries are efficient and well-commented for clarity.
- Queries will be tested in Snowflake - make sure yours can be executed there.

#### Deliverables
Please submit your SQL queries in a .txt format. Your queries should be well formatted and accompanied by comments explaining the rationale behind your approach - it should be straightforward for any reader to understand what is being done at each stage of your code. 
#### Evaluation Criteria
- Accuracy: Correctness of the queries in reflecting the requested metrics. 
- Efficiency: Performance optimization of the queries.
- Clarity: Readability and understandability of the queries and comments.

#### Conclusion
This case study is an opportunity to demonstrate your SQL expertise in a real-world scenario, leveraging Zendesk data to extract meaningful agent performance insights. We're excited to see your analytical approach to this challenge.