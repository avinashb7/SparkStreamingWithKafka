CREATE EXTERNAL TABLE covid_stage (
country string,
countryCode string,
newconfirmed bigint,
newdeaths bigint,
newrecovered bigint,
slug string,
totalconfirmed bigint,
totaldeaths bigint,
totalrecovered bigint,
event_date timestamp
)
STORED AS PARQUET
location "/user/training/covid";

CREATE VIEW covid_view AS
SELECT cs.country,cs.countryCode,cs.newconfirmed,cs.newdeaths,cs.newrecovered,
cs.slug,cs.totalconfirmed,cs.totaldeaths,cs.totalrecovered,cs.event_date from covid_stage cs
INNER JOIN (
    SELECT country, max(event_date) as max_event_date
    FROM covid_stage
    GROUP BY country
) cm ON cs.country = cm.country AND cs.event_date = cm.max_event_date;


CREATE TABLE covid_core (
country string,
countryCode string,
newconfirmed bigint,
newdeaths bigint,
newrecovered bigint,
slug string,
totalconfirmed bigint,
totaldeaths bigint,
totalrecovered bigint
)
PARTITIONED BY (event_date timestamp);

set hive.exec.dynamic.partition.mode=nonstrict;

INSERT INTO covid_core PARTITION(event_date) SELECT * FROM covid_stage;
