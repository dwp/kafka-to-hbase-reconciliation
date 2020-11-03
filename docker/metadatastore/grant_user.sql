CREATE USER IF NOT EXISTS reconciliationwriter;

GRANT SELECT, INSERT ON `ucfs` to reconciliationwriter;
-- GRANT SELECT, INSERT ON `equalities` to reconciliationwriter;
