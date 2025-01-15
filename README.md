# Data Pipeline using Postgres + Python + Metabase

### Architectural Diagram
<img width="471" alt="Image" src="https://github.com/user-attachments/assets/bdc5fad2-efc1-400f-9e38-fd04fcf5350d" />


### Schema Diagram



## Prerequisites
* Docker
* Docker Compose
* Git

## Services
The project consists of the following services:

### Required:
* Postgres
* Adminer
* Metabase

## Run Locally

### 1. Clone repository and start docker containers

```shell
git clone 
```

### 2. Go to the project directory
```shell
cd clifford-frempong-de-task/
```

### 3. Run the docker compose file
```shell
docker-compose up -d
```

### 4. Check the command to check if all components are up and running

```shell
docker compose ps
```

If all is well, you'll have everything running in their own containers, with a load generator script configured to load data from 2021-2023 into the postgres db

### 5. Verify PostgreSQL data
Access the [Postgres UI](http://localhost:7775/?pgsql=postgres&username=postgres&db=nyt_db&ns=public) and confirm the presence of 7 tables **dim_book**, **dim_date**, **dim_list**, **dim_publisher**, **fact_book_rankings**, **fact_publisher_performance** and **load_status** with data in them. Use "**postgres**" as the username and password when logging in.
![Image](https://github.com/user-attachments/assets/a864c445-42af-44cb-8f35-a5d772c673cf)
![Image](https://github.com/user-attachments/assets/7f056822-eb01-4637-b196-70222f9eab46)


## Business Intelligence: Metabase

1. In a browser, go to [Metabase UI](http://localhost:3000)

2. Click **Let's get started**.
![Image](https://github.com/user-attachments/assets/7e4f0971-c203-433c-9266-95541d094b90)

3. Complete the first set of fields asking for your email address. This
   information isn't crucial for anything but does have to be filled in.
![Image](https://github.com/user-attachments/assets/99d12eb0-a946-4930-ac30-77a380999f8e)

4. On the **Add your data** page, select Postgres and fill in the following information:
![Image](https://github.com/user-attachments/assets/a0803106-7c1d-47cf-97b4-458422694b31)

   | Field             | Enter...                        |
   | ----------------- | ------------------------------- |
   | Database          | **nyt_db**                  |
   | Name              | **DeelCompanyDB**              |
   | Host              | **postgres**           |
   | Port              | **5432**                        |
   | Database name     | **nyt_db**                     |
   | Database username | **postgres**                     |
   | Database password | **postgres**        |

![Image](https://github.com/user-attachments/assets/2e75c256-429b-4432-a4d8-a78c53b86b1e)

5. Proceed past the screens until you reach your primary dashboard.
![Image](https://github.com/user-attachments/assets/c11c30ff-4c84-4e71-b790-6dec5e8e568e)

6. Click **New**
![Image](https://github.com/user-attachments/assets/40646b3f-620c-45f9-bdf5-870746ef75f1)

7. Click **SQL query**.
![Image](https://github.com/user-attachments/assets/dd86b77d-dc4a-40eb-92e7-d3377c71420a)

8. From **Select a database**, select **DeelCompanyDB**.

9. In the query editor, enter:

   ```sql
   SELECT *
   FROM load_status; 
   ```

10. You can save the output and add it to a dashboard:

### Insights
1. Which book remained in the top 3 ranks for the longest time in 2022?
**description:**


```shell
WITH consecutive_rankings AS (
    SELECT 
        b.title,
        b.author,
        fr.rank,
        d.full_date,
        COUNT(*) OVER (PARTITION BY b.book_key) as weeks_in_top_3
    FROM fact_book_rankings fr
    JOIN dim_book b ON fr.book_key = b.book_key
    JOIN dim_date d ON fr.date_key = d.date_key
    WHERE d.year = 2022 
    AND fr.rank <= 3
)
SELECT 
    title,
    author,
    weeks_in_top_3,
    MIN(full_date) as first_appearance,
    MAX(full_date) as last_appearance
FROM consecutive_rankings
GROUP BY title, author, weeks_in_top_3
ORDER BY weeks_in_top_3 DESC
LIMIT 1;
```

2. Which are the top 3 lists to have the least number of unique books in their
rankings for the entirety of the data?
**description:**


```shell
SELECT 
    l.list_name,
    COUNT(DISTINCT b.book_key) as unique_books_count
FROM fact_book_rankings fr
JOIN dim_list l ON fr.list_key = l.list_key
JOIN dim_book b ON fr.book_key = b.book_key
GROUP BY l.list_key, l.list_name
ORDER BY unique_books_count ASC
LIMIT 3;
```
3. Publishers are ranked based on how their respective books performed on this
list. For each book, a publisher gets points based on the best rank a book got in a
given period of time. The publisher gets 5 points if the book is ranked 1st, 4 for
2nd rank, 3 for 3rd rank, 2 for 4th and 1 point for 5th. Create a quarterly rank for
publishers from 2021 to 2023, getting only the top 5 for each quarter.
**description:**



```shell
WITH publisher_points AS (
    SELECT 
        p.publisher_key,
        p.publisher_name,
        d.year,
        d.quarter,
        d.quarter_name,
        SUM(CASE 
            WHEN fr.rank = 1 THEN 5
            WHEN fr.rank = 2 THEN 4
            WHEN fr.rank = 3 THEN 3
            WHEN fr.rank = 4 THEN 2
            WHEN fr.rank = 5 THEN 1
            ELSE 0
        END) as total_points,
        ROW_NUMBER() OVER (
            PARTITION BY d.year, d.quarter 
            ORDER BY SUM(CASE 
                WHEN fr.rank = 1 THEN 5
                WHEN fr.rank = 2 THEN 4
                WHEN fr.rank = 3 THEN 3
                WHEN fr.rank = 4 THEN 2
                WHEN fr.rank = 5 THEN 1
                ELSE 0 
            END) DESC
        ) as quarterly_rank
    FROM fact_book_rankings fr
    JOIN dim_book b ON fr.book_key = b.book_key
    JOIN dim_publisher p ON b.publisher_key = p.publisher_key
    JOIN dim_date d ON fr.date_key = d.date_key
    WHERE d.year BETWEEN 2021 AND 2023
    AND fr.rank <= 5
    GROUP BY p.publisher_key, p.publisher_name, d.year, d.quarter, d.quarter_name
)
SELECT 
    publisher_name,
    year,
    quarter_name,
    total_points
FROM publisher_points
WHERE quarterly_rank <= 5
ORDER BY year, quarter, quarterly_rank;
```


4. Two friends Jake and Pete have podcasts where they review books. Jake's team
reviews the book ranked first on every list, while Pete’s team reviews the book
ranked third. Both of them share books, if Jake’s team wants to review a book,
they first check with Pete’s before buying and vice versa. Which team bought
what book in 2023?
**description:**



```shell
WITH team_books AS (
    SELECT 
        b.title,
        b.author,
        l.list_name,
        d.full_date,
        fr.rank,
        CASE 
            WHEN fr.rank = 1 THEN 'Jake'
            WHEN fr.rank = 3 THEN 'Pete'
        END as reviewer,
        ROW_NUMBER() OVER (
            PARTITION BY b.book_key 
            ORDER BY d.full_date
        ) as first_appearance
    FROM fact_book_rankings fr
    JOIN dim_book b ON fr.book_key = b.book_key
    JOIN dim_list l ON fr.list_key = l.list_key
    JOIN dim_date d ON fr.date_key = d.date_key
    WHERE d.year = 2023
    AND fr.rank IN (1, 3)
)
SELECT 
    reviewer as purchased_by,
    title,
    author,
    list_name,
    full_date as purchase_date
FROM team_books
WHERE first_appearance = 1
ORDER BY full_date, list_name;
```
## Pipeline Resilience Strategies

### Fault Isolation:
Each service operates within its own container, enabling isolation of any issues that may arise. This ensures that if one component encounters a problem, it won't affect the functioning of the others.

### Restart Policies:
Restart policies are configured for most services to automatically restart in case of failures. Whether it's `restart: always` or `restart: on-failure`, these policies help maintain service availability by quickly recovering from unexpected downtime.

### Error Handling:
Error handling and retry mechanisms are implemented within the Kafka Connect configuration. For instance, the frequency of offset flushing to persistent storage is controlled, reducing the risk of data loss during failures.

### Monitoring:
Monitoring capabilities are set up for critical services like Kafka to closely monitor the health of the pipeline. This allows for proactive identification of issues and prompt resolution to minimize downtime.

### Redundancy and Replication:
Kafka's replication factor ensures data redundancy, while Debezium captures database changes redundantly to prevent potential data loss. Additionally, ClickHouse can be configured for replication and high availability, enhancing data durability and ensuring continuous availability.

### Side note :)
Regular backups of critical databases like Postgres and ClickHouse are essential for recovering from catastrophic failures and ensuring data integrity.


---

#### You have some infrastructure running in Docker containers, so don't forget to run `docker-compose down` to shut everything down!
---


## Contributed by Clifford Frempong


