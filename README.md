# SQL-Create-Dynamic-Views-Tables
Through this method we can create views or tables dynamically without putting table names &amp; keys manually or we will get a idea how this things are working.
Thank you
<br /> CODE ->>
<br /> df = pd.DataFrame(pd.read_csv('table_primary_keys.csv'))
<br /> for a, b in zip(df['Table Name'], df['Primary Key']):
  dfff = spark.sql("""Create view if not exists dynamic_view.{} as with LTS as (
  select
    case
      when max(INSERT_TS) < max(UPDATE_TS) then max(INSERT_TS)
      else max(UPDATE_TS)
    end as `Latest TS`,
    {} as `ID_1`
  from ettable.{} group by {} ), DLT as (select {} as `ID_2` from ettable.{} where INSERT_TYPE = 'Delete') select * from ettable.{} full outer join DLT on DLT.`ID_2` = {}.{} inner join LTS on LTS.`ID_1` = {}.{} and (LTS.`Latest TS` = {}.INSERT_TS or LTS.`Latest TS` = {}.UPDATE_TS) where DLT.`ID_2` is null or {}.{} is null""".format(a, b, a, b, b, a, a, a, b, a, b, a, a, a, b))
