CREATE OR REPLACE PROCEDURE ECONOMIC_MODEL_RAW.LOAD_RAW_DATA()
RETURNS VARCHAR(16777216)
LANGUAGE SQL
AS
DECLARE
    tbl varchar;
    destTblWithSchema varchar;
    dbName varchar;
    source varchar;
    fullTblName varchar;
    columnsList varchar;
    createTblsCurr CURSOR FOR select * from economic_model_raw.tablenames cross join (select name as source, dbname from economic_model_raw.sources limit 1);
    loadTblsCurr CURSOR FOR select * from economic_model_raw.tablenames cross join (select name as source, dbname from economic_model_raw.sources);
BEGIN

    create or replace temporary table economic_model_raw.sources as
    select 
        $1 as name, $2 as dbName from
    values
        ('ARC', 'revo_morristown'),
        ('ARL', 'revo_bermuda')
    ;

    -- todo: specify select list for large tables to only include necessary 
    -- columns to minimize data included in snapshots and reduce load times.
    create or replace temporary table economic_model_raw.tablenames as
    select 
        $1 as name from
    values
        ('cedent'),
        ('cedentgroup'),
        ('company'),
        ('layer'),
        ('layerlossanalysis'),
        ('lossanalysis'),
        ('lossviewresult'),
        ('portfolio'), 
        ('portlayer'),
        ('portlayercession'),
        ('program'),
        ('retroallocation'),
        ('retrocommission'),
        ('retroinvestor'),
        ('retroinvestorreset'),
        ('retroprofile'),
        ('retroprogram'),
        ('retroprogramreset'),
        ('retrozone'),
        ('spinsurer'),
        ('submission')
    ;

    -- This table should be the same in all dbs
    -- so loading it just once
    CREATE OR REPLACE TABLE economic_model_raw.TOPUPZONE AS
    SELECT * exclude rowversion FROM REVO_BERMUDA.DBO.TOPUPZONE;

    CREATE OR REPLACE TABLE economic_model_raw.fxrate AS
    SELECT * exclude rowversion FROM REVO_BERMUDA.DBO.fxrate;
    
    FOR tblrecord IN createTblsCurr DO
        set tbl := tblrecord.name;
        set destTblWithSchema := concat('economic_model_raw.', tblrecord.name);
        set dbName := tblRecord.dbName;
        set source := tblRecord.source;
        set fullTblName := concat(dbName, '.dbo.', tbl);
        
        create or replace table identifier(:destTblWithSchema) as
        select '___' as source_db, * exclude rowversion from identifier(:fullTblName) limit 0;
    END FOR;
        
    -- now load the actual data to the table, tailoring the select list to the first
    -- source (it looks like e.g. Portfolio table has different column order in ARC vs ARL)
    FOR tblrecord IN loadTblsCurr DO
        set tbl := tblrecord.name;
        set destTblWithSchema := concat('economic_model_raw.', tblrecord.name);
        set dbName := tblRecord.dbName;
        set source := tblRecord.source;
        set fullTblName := concat(dbName, '.dbo.', tbl);
        
        set columnsList := (
            select 
                LISTAGG(column_name, ', ') within group (order by ordinal_position) 
            from 
                information_schema.columns 
            where 
                lower(table_name) = lower(:tbl) 
                and table_schema = 'ECONOMIC_MODEL_RAW' 
                and lower(column_name) <> 'source_db'
        );

        execute immediate concat(
            'insert into ', :destTblWithSchema, '(source_db, ', columnslist, ')\r\n',
            'select ''', :source, ''', ', columnsList, ' from ', :dbname, '.dbo.', :tbl);
        
    END FOR;

    return concat('Loaded ', (select count(*) from economic_model_raw.tablenames), ' tables from ', (select count(*) from economic_model_raw.sources), ' databases.');
    
END
;