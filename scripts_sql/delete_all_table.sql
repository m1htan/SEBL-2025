DECLARE @table_name NVARCHAR(256)
DECLARE table_cursor CURSOR FOR
SELECT name
FROM sys.tables

OPEN table_cursor
FETCH NEXT FROM table_cursor INTO @table_name

WHILE @@FETCH_STATUS = 0
BEGIN
    EXEC('DROP TABLE IF EXISTS ' + @table_name)
    FETCH NEXT FROM table_cursor INTO @table_name
END

CLOSE table_cursor
DEALLOCATE table_cursor