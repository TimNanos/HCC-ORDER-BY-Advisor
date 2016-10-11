/*
  Copyright 2015 Tim Nanos, timnanos.com
  Licensed under the Apache License, Version 2.0
*/


CREATE OR REPLACE PACKAGE PKG_HCC_ORDERBY_ADVISOR AS

  -- These names would be used for the tests. Make sure that tables with the same names do not exist
  gv_tempTableName1 CONSTANT VARCHAR2(30) := 'TEMP_HCC_ANALYSIS_TEST_1';
  gv_tempTableName2 CONSTANT VARCHAR2(30) := 'TEMP_HCC_ANALYSIS_TEST_2';

  -- Compression that will be used for the tests
  gv_compressionOption CONSTANT VARCHAR2(64) := 'COMPRESS FOR QUERY HIGH';

  -- Sample table will be created with this amount of rows. A 1000000 is the number recommended by Oracle White papers
  gn_sampleTableRows CONSTANT PLS_INTEGER := 1000000;

  -- Generate table 'tableName' with all possible ORDER BY clauses and discover the most optimal
  PROCEDURE analyseTable(tableName IN VARCHAR2, ownerName IN VARCHAR2, continuePrevious IN BOOLEAN := TRUE);

  -- Show the results for a table that was previously analysed
  PROCEDURE printReport(tableName IN VARCHAR2, ownerName IN VARCHAR2);

  -- Analyse all tables
  PROCEDURE analyseAllTables(ownerName IN VARCHAR2 := USER);

END PKG_HCC_ORDERBY_ADVISOR;
/
