# Summary
Creates a demo analytical data warehouse for a theoretical music streaming startup Sparkify. 

# Files

**manage_cluster.py**
* IAC script which creates or tears down a Redshift cluster for the data warehouse to be created on

**create_tables.py**
* Drops and recreates staging/analytics tables

**etl.py**
* Copies S3 data to staging tables in Redshift and transforms staging data into appropriate star schema

**dwh.cfg**
* Config for Redshift cluster

**sql_queries.py**
* Queries used in create_tables.py and etl.py

**Fiddle.ipynb**
* Scratch notebook for testing out snippets of code

# Usage

# Project Questions

### Discuss the purpose of this database in context of the startup, Sparkify, and their analytical goals.

This data schema is meant to help answer many of Sparkify's analytics/BI related questions. Specifically questions around user listening behavior. Eg: 
* How much are users listening? 
* What are those users listening to? 
* Where/when are users listening? 
* Distribution of user listening behavior
* Distribution of artist/song consumption
* Etc

### State and justify your database schema design and ETL pipeline.

Star schema w/ the following tables: 

fct_song_plays:
* Fact table with each record representing a distinct play event
* sortkey -> item_in_session: Included because this is the field most likely to be used in the WHERE when querying this table. Including sortkey should help optimize queries.
* distkey -> song_id: FK to songs.id which also has a distkey. This is the largest dim table and also the table most likely to be joined to fct_song_plays, so optimizing this join w/ a distkey is important. 

dim_users:
* Dimension table w/ user (ie song consumer) attributes
* sortkey -> registration: Field most likely to be used in a WHERE clause
* diststyle -> ALL: Table is small enough to be replicated across slices and will likely be used frequently in joins

dim_songs:
* Dimension table w/ song attributes
* distkey -> song_id: Field used for join w/ fct_song_plays. Even though fct_song_plays.song_id doesn't have a corresponding distkey this should still improve the performance of the join
* sortkey -> title: Field most likely to be used in a WHERE clause

dim_artists:
* Dimension table w/ artist attributes
* distkey -> artist_id: Field used for join w/ fct_song_plays.
* sortkey -> artist_name: Field most likely to be used in a WHERE clause

dim_time_dimensions:
* Dimension table w/ various cuts for time
* "time_key" is a timestamp truncated to the hour and cast to an int in the format "YYYYMMDDHH". Truncating the timestamp and storing it as an int will improve the performance of the join 
* distkey -> time_key: Field used for join w/ fct_song_plays.
* sortkey -> date: Field most likely to be used in a WHERE clause
