-- Lookup activity query (Lkp_Fetch_Tbl_Name) in the Bronze pipeline.
-- Returns one row per source table to load; the ForEach activity iterates over the result
-- and passes each row to a parameterised Copy activity (source -> Bronze lakehouse).
SELECT  SourceSchema,
        SourceTable,
        SourceColumnList,   -- explicit column projection for the copy
        TargetTable         -- Bronze lakehouse table name
FROM    [DataOpsWarehouse].[Metadata].[Src_Brn_Tbl_Lst]
WHERE   SourceSystem = '@{pipeline().parameters.SystemCode}';
-- Note: the filter column/parameter reflects the framework's SystemCode parameter;
-- the exact production predicate is not reproduced here.
