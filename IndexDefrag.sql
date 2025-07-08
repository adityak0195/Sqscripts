-------------------------------------------------- Index Defragmentation Rebuild > 30 pc
SELECT
    dbschemas.[name] AS 'Schema',
    dbtables.[name] AS 'Table',
    dbindexes.[name] AS 'Index',
    indexstats.avg_fragmentation_in_percent,
    indexstats.page_count,
    'ALTER INDEX ' + dbindexes.[name] + ' ON ' + dbtables.[name] + ' REBUILD' as Command
FROM
    sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, NULL) AS indexstats
    INNER JOIN sys.tables dbtables ON dbtables.[object_id] = indexstats.[object_id]
    INNER JOIN sys.schemas dbschemas ON dbtables.[schema_id] = dbschemas.[schema_id]
    INNER JOIN sys.indexes AS dbindexes ON dbindexes.[object_id] = indexstats.[object_id]
    AND indexstats.index_id = dbindexes.index_id
WHERE
    indexstats.database_id = DB_ID()
    AND indexstats.avg_fragmentation_in_percent > 30
ORDER BY
    indexstats.avg_fragmentation_in_percent DESC;

-------------------------------------------------- Index Defragmentation Reorganize  > 5 pc
SELECT
    dbschemas.[name] AS 'Schema',
    dbtables.[name] AS 'Table',
    dbindexes.[name] AS 'Index',
    indexstats.avg_fragmentation_in_percent,
    indexstats.page_count,
    'ALTER INDEX ' + dbindexes.[name] + ' ON ' + dbtables.[name] + ' REORGANIZE' as Command
FROM
    sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, NULL) AS indexstats
    INNER JOIN sys.tables dbtables ON dbtables.[object_id] = indexstats.[object_id]
    INNER JOIN sys.schemas dbschemas ON dbtables.[schema_id] = dbschemas.[schema_id]
    INNER JOIN sys.indexes AS dbindexes ON dbindexes.[object_id] = indexstats.[object_id]
    AND indexstats.index_id = dbindexes.index_id
WHERE
    indexstats.database_id = DB_ID()
    AND indexstats.avg_fragmentation_in_percent > 5
    and indexstats.avg_fragmentation_in_percent < 30
ORDER BY
    indexstats.avg_fragmentation_in_percent DESC;

-------------------------------------------------- To check current running queries on the data base
SELECT
    top(100) sqltext.TEXT,
    req.session_id,
    req.status,
    req.command,
    req.cpu_time,
    req.total_elapsed_time,
    s.name,
    DB_NAME(req.database_id),
    req.last_wait_type,
    req.wait_time,
    req.wait_resource
FROM
    sys.dm_exec_requests req
    CROSS APPLY sys.dm_exec_sql_text(sql_handle) AS sqltext,
    sys.sysusers s
where
    req.user_id = s.uid
    and DB_NAME(req.database_id) = 'twx_ald_prod_ext'
order by
    req.total_elapsed_time desc
