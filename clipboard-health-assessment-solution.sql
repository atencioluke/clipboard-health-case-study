/****************************************** PII WARNING ******************************************
PII in Dummy Data_Customer Support Data Analyst_Case study google sheets (VIA, TAGS) including email, phone, names:
    -- https://docs.google.com/spreadsheets/d/1oP7jlO6pDDDhBxUOY7_LtnHjUkXZQp8yAy8iNLpZi_I/edit#gid=0
****************************************** PII WARNING ******************************************/




/****************************************** TASK 1 START & SUMMARY ******************************************
Create an SQL query to determine the average first reply time and average first resolution time by month for the past 12 months:

Recommendations:
    -- Include additional statistics to control for outliers and unreliable averages
    -- Include additional fields for futher EDA (exploratory data analysis) and business insights
    ---- e.g. ticket types, calendar hours, ticket id
Assumptions:
    -- Timezone is UTC unless specified per Zendesk policy.
    -- (ZD_Ticket_Metrics.CREATED_AT != ZD_Tickets.CREATED_AT)
    ---- Easily updated to rely solely on ZD_ticket_metrics for improved performance, but loses extensibility
    -- (Business hours = Calendar hours) for ticket metrics, per Clipboard Health's 24/7 support policy:
    ---- https://www.clipboardhealth.com/about
    ---- As such, metrics with both Business and Calendar hours will have Business as default
    -- Definitions of first reply and first resolution time defined by Zendesk:
    ---- https://support.zendesk.com/hc/en-us/articles/4408834848154-About-native-Support-time-duration-metrics
    ---- https://support.zendesk.com/hc/en-us/articles/4408821871642-Understanding-ticket-reply-time#topic_jvw_nqd_1hb
Resources:
    -- Snowflake, Zendesk API, and HEVO docs used for SQL syntax, Clipboard Health data pipeline & structure, and Zendesk data information (e.g. datatypes, column & table descriptors):
    ---- https://docs.snowflake.com/
    ---- snowflake.snowflake-vsc VS-Code Extension
    -- https://developer.zendesk.com/api-reference/
    -- https://docs.hevodata.com/destinations/data-warehouses/snowflake/snowflake-data-structure/
*/

---- Setting the database and schema for the session for readability
USE HEVO_DATABASE.MONGO_MAIN_APP
GO ---- Instead of ';' if required syntax to separate batches


---- Prequalify the data in temp table before running aggregate operations to improve performance and extensibiity
---- Ticket metrics past 12 months with reply OR resolution
WITH monthly_metrics_prep AS (
    SELECT
        /*********************** REQUIRED START **********************/
        ---- ZD stores created_at as string, casting to date for time operations.
        DATE_TRUNC('month', t.created_at::DATE) AS month -- +'_date' -- suffix if function name causes issues

        ---- Bracket notation used for nested JSON values:
        , m.reply_time_in_minutes['business']::NUMBER AS first_reply_mins
        , m.first_resolution_time_in_minutes['business']::NUMBER AS first_resolution_time_mins
        /*********************** REQUIRED END **********************/

        /*********************** OPTIONAL START *********************/ 
        ---- ticket_id to extend EDA/investigation capabilities
        -- , t.id::NUMBER AS ticket_id

        ---- Ticket type to extend EDA/investigation capabilities
        -- , t.type AS ticket_type

        ---- Includes ONLY messaging tickets in seconds
        -- , m.reply_time_in_seconds['calendar']::NUMBER AS first_reply_secs_msg

        ---- _cal suffix calculated based on calendar time. Calendar optional to check consistency:
        -- , m.reply_time_in_minutes['calendar']::NUMBER AS first_reply_mins_cal
        -- , m.first_resolution_time_in_minutes['calendar']::NUMBER AS first_resolution_time_mins_cal
        /********************** OPTIONAL END ***********************/
    
    ---- ASSUMPTION: (ZD_Ticket_Metrics.CREATED_AT != ZD_Tickets.CREATED_AT), using ZD_tickets table for ticket dates:
    FROM ZD_tickets t
        JOIN ZD_ticket_metrics m
            ON t.id = m.ticket_id

    ---- Tickets in the past 12 months that have a reply OR resolution:
    WHERE 
        ---- Ticket month is within the last 12 calendar months from current date
        (DATE_TRUNC('month', t.created_at::DATE)) >= (DATEADD('month', -12, DATE_TRUNC('month', CURRENT_DATE())))

        ---- Remove new tickets, they don't have replies/resolutions. Used 'NOT IN' for extensibility
        AND t.status NOT IN ('new')

        AND (
        ---- First Reply Time specific filters:
        ---- Filtering nulls, unsure if parses SQL NULL or JSON null; Casting to variant to use IS_INTEGER
        (IS_INTEGER(m.reply_time_in_minutes['business']::VARIANT))

        OR
        
        ---- First Resolution Time specific filters:
        ---- Filtering nulls, unsure if parses SQL NULL or JSON null; Casting to variant to use IS_INTEGER
        (IS_INTEGER(m.first_resolution_time_in_minutes['business']::VARIANT))
        )
)

---- Determine the average first reply time and average first resolution time by month for the past 12 months.
SELECT
    /*********************** REQUIRED START **********************/
    m.month AS month -- +'_date' -- suffix if function name causes issues
    , AVG(m.first_reply_mins) AS avg_first_reply_mins
    , AVG(m.first_resolution_time_mins) AS avg_first_resolution_time_mins
    /*********************** REQUIRED END **********************/

    /*********************** RECOMMENDED START **********************/
    ---- Recommendation: Include median, max and 90th percentile to validate average and check for outliers:
    ---- First Reply Time Business hours:
    -- , MEDIAN(m.first_reply_mins) AS median_first_reply_mins
    -- , MAX(m.first_reply_mins) AS max_first_reply_mins
    -- , APPROX_PERCENTILE(m.first_reply_mins, 0.9) AS _90p_first_reply_mins

    ---- First Resolution Time Business hours:
    -- , MEDIAN(m.first_resolution_time_mins) AS median_first_resolution_time_mins
    -- , MAX(m.first_resolution_time_mins) AS max_first_resolution_time_mins
    -- , APPROX_PERCENTILE(m.first_resolution_time_mins, 0.9) AS _90p_first_resolution_time_mins
    /*********************** RECOMMENDED END  **********************/
    
    /*********************** OPTIONAL START **********************/
    /*****>>> UNCOMMENT AND RUN OPTIONAL FIELDS IN monthly_metrics_prep <<<*****/

    ---- Ticket type to extend EDA/investigation capabilities: e.g. add to GROUP BY for metrics broken out by type
    -- , m.type AS ticket_type

    ---- First Reply Time Calendar Hours. Calendar optional to check consistency:
    -- , AVG(m.first_reply_mins_cal) AS avg_first_reply_mins_cal
    -- , MEDIAN(m.first_reply_mins_cal) AS median_first_reply_mins_cal
    -- , MAX(m.first_reply_mins_cal) AS max_first_reply_mins_cal
    -- , APPROX_PERCENTILE(m.first_reply_mins_cal, 0.9) AS _90p_first_reply_mins_cal

    ---- First Resolution Time Calendar Hours. Calendar optional to check consistency:
    -- , AVG(m.first_resolution_time_mins_cal) AS avg_first_resolution_time_mins_cal
    -- , MEDIAN(m.first_resolution_time_mins_cal) AS median_first_resolution_time_mins_cal
    -- , MAX(m.first_resolution_time_mins_cal) AS max_first_resolution_time_mins_cal
    -- , APPROX_PERCENTILE(m.first_resolution_time_mins_cal, 0.9) AS _90p_first_resolution_time_mins_cal
    
    ---- Includes ONLY messaging tickets in seconds
    -- , AVG(m.first_reply_secs_msg) AS avg_first_reply_secs_msg
    -- , MEDIAN(m.first_reply_secs_msg) AS median_first_reply_secs_msg
    -- , MAX(m.first_reply_secs_msg) AS max_first_reply_secs_msg
    -- , APPROX_PERCENTILE(m.first_reply_secs_msg, 0.9) AS _90p_first_reply_secs_msg
    /*********************** OPTIONAL END ***********************/
FROM monthly_metrics_prep m
GROUP BY 1
ORDER BY 1
;
/****************************************** TASK 1 END ******************************************/


/****************************************** TASK 2 SUMMARY ******************************************
Create an SQL query that provides average satisfaction score (as calculated by Zendesk) and total tickets solved by agent by month for the last 6 months:

Recommendations:
    -- Include ticket type to see breakdown on ticket_type
Assumptions:
    -- Per assessment clarifications from recruiter:
    ---- Satisfaction surveys are sent after the ticket is solved
    ------ Tickets with satisfaction rating offered were marked solved at a certain point
    ------ Solved tickets can be reopened
    -- Timezone is UTC unless specified per Zendesk
    -- Average satisfaction score (as calculated by Zendesk) calculated using formula: (COUNT(Good satisfaction tickets)/COUNT(Satisfaction Responses)) referenced here:
    ---- https://support.zendesk.com/hc/en-us/articles/4408822875930-Explore-recipe-Determining-satisfaction-scores-for-your-agents
    -- (ZD_Ticket_Metrics.CREATED_AT != ZD_Users.CREATED_AT)
    -- (ZD_Ticket_Metrics.ID != ZD_Users.ID)
    -- Solved tickets automatically closed by zd after 4 days
    ---- https://support.zendesk.com/hc/en-us/articles/4408835051546-About-the-Support-default-automations#topic_44p_cty_5t
Resources:
    -- Snowflake, Zendesk API, and HEVO docs used for SQL syntax, Clipboard Health data pipeline & structure, and Zendesk data information (e.g. datatypes, column & table descriptors):
    ---- https://docs.snowflake.com/
    ---- snowflake.snowflake-vsc VS-Code Extension
    -- https://developer.zendesk.com/api-reference/
    -- https://docs.hevodata.com/destinations/data-warehouses/snowflake/snowflake-data-structure/
*/

---- Setting the database and schema for the session for readability
USE HEVO_DATABASE.MONGO_MAIN_APP
GO ---- Instead of ';' if required by syntax to separate batches


---- Prequalify the data in a cte before running aggregate operations to improve performance and extensibiity
---- Find Agent and assigned tickets that were solved (have a satisfaction score offered) in the past 6 calendar months: 
WITH agent_metrics_prep AS (
    SELECT
        /*********************** REQUIRED START **********************/
        ---- ZD stores created_at as string, casting to date for time operations. 
        DATE_TRUNC('month', t.created_at::DATE) AS month -- +'_date' -- suffix if function name causes issues
        ---- ID optimal unique identifier for user without PII
        , u.id::NUMBER AS agent_id
        ---- Included for 
        , t.id::NUMBER AS ticket_id
        
        ---- Prepare for aggregate operations:
        , (CASE WHEN t.satisfaction_rating['score'] = 'good' THEN 1 ELSE NULL) AS positive_satisfaction

        /*********************** REQUIRED END **********************/

        /*********************** OPTIONAL START *********************/ 
        ---- Extend EDA/investigation capabilities. e.g. Helpful for deterimining reopened tickets
        -- , t.status AS ticket_status

        ---- Not included as ID unique and leaves less personally identifiable information. Use case: easier to read for team leads/managers
        -- , u.name AS agent_name

        ---- Not included as ID unique and leaves less personally identifiable information. Use case: mass communications to cohorts of agents
        -- , u.email AS agent_email

        ---- Ticket type to extend EDA/investigation capabilities. Use case: partition results by ticket (i.e. task vs incident satisfaction)
        ---- When t.type not specified labeling unassigned, otherwise use assigned value
        -- , (CASE WHEN t.type IS NULL THEN 'unassigned' ELSE t.type) AS ticket_type

        ---- If satisfaction survey received response. Use case: finding response rate
        -- , (CASE WHEN t.satisfaction_rating['score']IN ('good','bad') THEN 1 ELSE NULL) AS satisfaction_response

        /******* REOPENED TICKETS *******/
        ---- Update WHERE clause for reopened ticket investigation
        ---- Assumption: Survey sent but ticket status not 'closed' or 'solved' = reopened:
        -- , (CASE WHEN t.status IN ('open','pending','hold') AS reopened_ticket

        ---- Use to find total solved tickets if doing reopened ticket investigation
        -- , (CASE WHEN t.satisfaction_rating['score']IN ('good','bad','offered') THEN 1 ELSE NULL) AS solved_ticket
        /********************** OPTIONAL END ***********************/
    ---- Assuming (ZD_Ticket_Metrics.CREATED_AT != ZD_Users.CREATED_AT), using ZD_tickets table for ticket dates:
    FROM ZD_tickets t
        JOIN ZD_users u
            ON t.assignee_id = u.id

    ---- Tickets created or updated in the past 6 months that were solved:
    WHERE
        ---- Only tickets that were marked solved
        ---- Assumption: Satisfaction survey sent = solved ticket
        t.satisfaction_rating['score'] IN ('good','bad','offered')
        
        /******* REOPENED TICKETS OPTIONAL *******/
        ---- Remove new tickets, they don't have satisfaction ratings.
        ---- Remove ('open','pending','hold') for reopened tickets investigation.  Used NOT IN for extensibility
        AND t.status NOT IN ('new','open','pending','hold')
        /******* REOPENED TICKETS END *******/
        
        ---- Tickets created or updated in the past 6 months:
        ---- Ticket created_at is within the last 6 calendar months from current date
        AND (DATE_TRUNC('month', t.created_at::DATE)) >= (DATEADD('month', -6, DATE_TRUNC('month', CURRENT_DATE())))

        ---- Ticket updated_at is within the last 6 calendar months from current date
        OR (DATE_TRUNC('month', t.updated_at::DATE)) >= (DATEADD('month', -6, DATE_TRUNC('month', CURRENT_DATE())))
        
) 


SELECT 
    /*********************** REQUIRED START **********************/
    am.month AS month -- +'_date' -- suffix if function name causes issues
    , am.user_id AS agent_id
    ---- Assumption: If ticket offered satisfaction survey, it was marked solved at some point
    , COUNT(DISTINCT am.ticket_id) AS total_solved_tickets
    ---- Finding average satisfaction score using: (COUNT(Good satisfaction tickets)/COUNT(Satisfaction Responses)) 
    , (COUNT(am.positive_satisfaction) / COUNT(DISTINCT am.ticket_id)) AS average_satisfaction_score
    /*********************** REQUIRED END **********************/


    /*********************** OPTIONAL START **********************/
    ---- Not included as ID unique and leaves less personally identifiable information. Use case: easier to read for team leads/managers
    -- am.agent_name AS agent_name

    ---- Not included as ID unique and leaves less personally identifiable information. Use case: mass communications to cohorts of agents
    -- am.agent_email AS agent_email

    ---- Satisfaction response rate option #1: (Tickets w/ satisfaction survey response) / (Tickets sent survey)
    -- , COUNT(am.satisfaction_response) / COUNT(DISTINCT am.ticket_id)


    /******* REOPENED TICKETS OPTIONAL *******/
    ---- Swap total_solved_tickets Finding average satisfaction score using: (COUNT(Good satisfaction tickets)/COUNT(Satisfaction Responses)) 
    -- , (COUNT(am.solved_ticket)) AS total_solved_tickets

    ---- Assumption: Survey sent but ticket status not 'closed' or 'solved' = reopened:
    -- , COUNT(am.reopened_ticket) AS total_reopened_tickets

    ---- Satisfaction response rate : (Tickets w/ satisfaction survey response) / (Tickets sent survey)
    -- , COUNT(am.satisfaction_response) / COUNT(am.solved_ticket)
    /*********************** OPTIONAL END **********************/
FROM agent_metrics_prep am
GROUP BY 1,2
ORDER BY 1
;