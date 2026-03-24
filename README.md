### Background:
dbt is a great tool for standardizing tables (called “models” in dbt) and enforcing more reliable and scalable code in data pipelines. It focuses exclusively on the "T" (Transform) step of the modern ELT (Extract, Load, Transform) data pipeline.

At Middle Seat we use it to standardize data across all our clients, making it easier to quickly stand-up new clients and merge changes consistently for everyone.

### Instructions
We've just onboarded the Benito for President campaign as a client and the Data Department needs to deliver their Data Studio dashboard. They're particularly excited to see where donations are coming from and to keep track of their recurring donations program. Benito loves to say "El signo de dinero ese e' mi nuevo zodiaco".

1. Make a copy of this repo, [this Google Sheet](https://docs.google.com/spreadsheets/d/1Ut6iAexVc3e_GS76aZAhQNLbfI7amDUiSVTSOClAPZY/edit?usp=sharing), and [this Data Studio](https://lookerstudio.google.com/reporting/60cb6699-3e6c-4bb1-a7f6-1d5caaf07508).
2. In your repo, add in comments to explain in a concise and understandable way, what each of the following models and macros does. (The goal here is to understand how you interpret others' code and how you leave breadcrumbs for others.)
    - [likely_source_type](https://github.com/zaqcruze/data-analyst-technical-test/blob/main/middleseat_dbt/macros/likely_source_type.sql) 
    - [get_precore_tables](https://github.com/zaqcruze/data-analyst-technical-test/blob/main/middleseat_dbt/macros/get_precore_tables.sql)
    - [precore_actblue__donations](https://github.com/zaqcruze/data-analyst-technical-test/blob/main/middleseat_dbt/models/precore/precore_actblue__donations.sql)
    - [core__donations](https://github.com/zaqcruze/data-analyst-technical-test/blob/main/middleseat_dbt/models/core/core__donations.sql)
3. For the [core__donations](https://github.com/zaqcruze/data-analyst-technical-test/blob/main/middleseat_dbt/models/core/_core_schema.yml) model add in descriptions of relevant columns in the [_core_schema.yml](https://github.com/zaqcruze/data-analyst-technical-test/blob/main/middleseat_dbt/models/core/_core_schema.yml) file. The descriptions should be easily understood for non-technical teammates. (We want to know how you translate technical code for a non-technical audience.)
4. Update the [reporting__donations_by_category_by_day](https://github.com/zaqcruze/data-analyst-technical-test/blob/main/middleseat_dbt/models/reporting__donations_by_category_by_day.sql) model or create your own model for your own use in question #5. (We need to see your SQL coding level and how you structure your reporting tables.)
5. Create a Data Studio that shows:
    - Donation stats
    - Donor stats
    - Recurring donation stats 

(Our goal is to understand how you visualize data for clients and non-technical teammates.)

### Timing
Take as long as you'd like but this test shouldn't take you more than 1.5 - 2 hours to complete.

### Submission
Please submit your repo, your Data Studio and any additional documentation you may have to irma@middleseat.co
