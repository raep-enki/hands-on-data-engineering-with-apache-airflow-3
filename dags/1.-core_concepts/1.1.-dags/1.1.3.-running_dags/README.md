
# Cron Presets and Scheduling in Airflow

For more elaborate scheduling requirements, you can implement a custom timetable. Note that Airflow parses cron expressions with the croniter library which supports an extended syntax for cron strings. See their documentation in github. For example, you can create a DAG schedule to run at 12AM on the first Monday of the month with their extended cron syntax: `0 0 * * MON#1`.

> **Tip**: You can use an online editor for CRON expressions such as [Crontab guru](https://crontab.guru)

## Schedule Presets

| Preset | Meaning | Cron Expression |
|--------|---------|-----------------|
| `None` | Don't schedule, use for exclusively "externally triggered" DAGs | - |
| `@once` | Schedule once and only once | - |
| `@continuous` | Run as soon as the previous run finishes | - |
| `@hourly` | Run once an hour at the end of the hour | `0 * * * *` |
| `@daily` | Run once a day at midnight (24:00) | `0 0 * * *` |
| `@weekly` | Run once a week at midnight (24:00) on Sunday | `0 0 * * 0` |
| `@monthly` | Run once a month at midnight (24:00) of the first day of the month | `0 0 1 * *` |
| `@quarterly` | Run once a quarter at midnight (24:00) on the first day | `0 0 1 */3 *` |
| `@yearly` | Run once a year at midnight (24:00) of January 1 | `0 0 1 1 *` |

Your DAG will be instantiated for each schedule along with a corresponding DAG Run entry in the database backend.
