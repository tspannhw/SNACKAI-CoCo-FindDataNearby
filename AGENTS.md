# Find Data Near me

This project produces an application that finds data that is near me in my Snowflake as well as in external sources.

## Snowflake Connection
- **ROLE:** ACCOUNTADMIN
- **WAREHOUSE:** INGEST
## Tech Stack
- **Dashboard:** React
- **LANGUAGE:** SQL + Python 3.11
## KEY Rules
### Always
- Use fully qualified names: ANALYTICS_DEV_DB.STAGING.TABLE_NAME

- Use CREATE OR REPLACE instead of DROP + CREATE
### Never
- GRANT OR REVOKE roles — RBAC IS managed BY the platform team
- Add Python dependencies without asking first
- Skip diff review ON SQL changes
### Prefer
- CTEs over nested subqueries
- snake_case FOR ALL OBJECT names
- Parameterized filters over hardcoded DATE ranges
## Reference Notes

Build a web tool to browse databases and views and semantic views

Allow for doing queries and doing cortex agent chats

use snowflake  https://docs.snowflake.com/en/sql-reference/functions-geospatial native geo functions as well as Python
nomatim
openstreetmaps
as well as Cortex AI for functions

react app 
also build command line tool, rest , mcp and cortex agent

local data finder

Build a manage shell script that install, setup, start, stop, list, validate, test, backup, document, start all, stop all

Build a full safe github repo with gitignore

Make sure all credentials are used safeless.  Do not use passwords, use key-pair or browser or snow connection.



