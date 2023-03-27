select TABLE_NAME, TABLE_COMMENT
from information_schema.TABLES
where TABLE_SCHEMA = ? or TABLE_SCHEMA = 'information_schema';
