
def main():
    engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/nass')
    df = pd.read_csv('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\projects\\ag_census\\ag_census_2012.csv')

    table = 'harvested_2012'
    pg_schema = 'ag_census'

    df.to_sql(table, engine, schema=pg_schema)

    #### REFINEMENTS ON TABLES  ##############################
    #DELETE FROM ag_census.harvested_2012 where "Value" = ' (D)'


    ## QUERY  ###################################
create table ag_census.diff_2012_2007 as

SELECT
  LPAD(b."State ANSI"::text, 2, '0') ||  LPAD(b."County ANSI"::text, 3, '0') as b_fips,
  a."Year" as year_2007,
  a."Value" as value_2007,
  b."Year" as year_2012,
  b."Value" as value_2012,
  c.acres_calc,
  (((b."Value"::bigint) - (a."Value"::bigint))/c.acres_calc) * 100 as net_perc,
  c.geom
  
FROM 
  ag_census.harvested_2007 as a INNER JOIN ag_census.harvested_2012 as b ON (LPAD(a."State ANSI"::text, 2, '0') ||  LPAD(a."County ANSI"::text, 3, '0')) = (LPAD(b."State ANSI"::text, 2, '0') ||  LPAD(b."County ANSI"::text, 3, '0')) INNER JOIN spatial.counties as c on (LPAD(b."State ANSI"::text, 2, '0') ||  LPAD(b."County ANSI"::text, 3, '0')) = c.atlas_stco

order by atlas_stco



main()
