select COLUMN_NAME, COLUMN_COMMENT, COLUMN_TYPE
from information_schema.COLUMNS
where TABLE_SCHEMA = ? or TABLE_SCHEMA = 'information_schema'
group by COLUMN_NAME, COLUMN_COMMENT, COLUMN_TYPE;
