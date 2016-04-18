/*
  Copyright 2015 Tim Nanos, timnanos.com
  Licensed under the Apache License, Version 2.0
*/


CREATE OR REPLACE PACKAGE BODY PKG_HCC_ORDERBY_ADVISOR AS

  FUNCTION getDDLStatement(newTableName IN T_HCC_ORDERBY_ADVISOR_LOG.TABLE_NAME%TYPE, ownerName IN T_HCC_ORDERBY_ADVISOR_LOG.OWNER%TYPE,
    tableName IN T_HCC_ORDERBY_ADVISOR_LOG.TABLE_NAME%TYPE, orderByCols IN T_HCC_ORDERBY_ADVISOR_LOG.ORDER_BY_COLS%TYPE, limitRows IN PLS_INTEGER := 0)
  RETURN CLOB
  AS
    ll_DDLStatement CLOB;
  BEGIN
    ll_DDLStatement := 'SELECT * FROM ' || pkg_hcc_orderby_advisor.getDDLStatement.ownerName || '.' || pkg_hcc_orderby_advisor.getDDLStatement.tableName;

    IF pkg_hcc_orderby_advisor.getDDLStatement.orderByCols IS NOT NULL THEN
      ll_DDLStatement := ll_DDLStatement || ' ORDER BY ' || pkg_hcc_orderby_advisor.getDDLStatement.orderByCols;
    END IF;

    IF pkg_hcc_orderby_advisor.getDDLStatement.limitRows > 0 THEN
      ll_DDLStatement := 'SELECT * FROM (' || ll_DDLStatement || ') WHERE ROWNUM <= ' || TO_CHAR(pkg_hcc_orderby_advisor.getDDLStatement.limitRows);
    END IF;

    ll_DDLStatement := 'CREATE TABLE ' || pkg_hcc_orderby_advisor.getDDLStatement.newTableName || ' ' || pkg_hcc_orderby_advisor.gv_compressionOption
      || ' PARALLEL NOLOGGING AS ' || ll_DDLStatement;

    RETURN ll_DDLStatement;
  END getDDLStatement;


  PROCEDURE printReport(tableName IN VARCHAR2, ownerName IN VARCHAR2)
  AS
    ln_bytesMin     T_HCC_ORDERBY_ADVISOR_LOG.BYTES%TYPE;
    ln_ordersCount  PLS_INTEGER;
    ln_outputLength PLS_INTEGER;
    lt_dateAnalysed T_HCC_ORDERBY_ADVISOR_LOG.DATE_ANALYSED%TYPE;
  BEGIN

    SELECT MIN(t.BYTES),
           MAX(t.DATE_ANALYSED)
      INTO ln_bytesMin,
           lt_dateAnalysed
      FROM T_HCC_ORDERBY_ADVISOR_LOG t
     WHERE t.TABLE_NAME = pkg_hcc_orderby_advisor.printReport.tableName
       AND t.OWNER = pkg_hcc_orderby_advisor.printReport.ownerName;

    IF ln_bytesMin IS NULL THEN
      RAISE_APPLICATION_ERROR(-20001, 'Error. Table ' || pkg_hcc_orderby_advisor.printReport.ownerName ||
        '.' || pkg_hcc_orderby_advisor.printReport.tableName || ' analysis was not found. Please run following command first:' || CHR(10) ||
        'BEGIN PKG_HCC_ORDERBY_ADVISOR.analyseTable(''' || pkg_hcc_orderby_advisor.printReport.tableName || ''', ''' || pkg_hcc_orderby_advisor.printReport.ownerName || '''); END;');

      RETURN;
    END IF;

    DBMS_OUTPUT.PUT_LINE('HCC analysis for table ' || pkg_hcc_orderby_advisor.printReport.ownerName || '.' || pkg_hcc_orderby_advisor.printReport.tableName);

    DBMS_OUTPUT.PUT_LINE('Performed: ' || TO_CHAR(lt_dateAnalysed) || CHR(10));

    DBMS_OUTPUT.PUT_LINE(pkg_hcc_orderby_advisor.getDDLStatement(pkg_hcc_orderby_advisor.printReport.tableName, pkg_hcc_orderby_advisor.printReport.ownerName,
      pkg_hcc_orderby_advisor.printReport.tableName, '... ;'));

    SELECT COUNT(t.ORDER_BY_COLS)
      INTO ln_ordersCount
      FROM T_HCC_ORDERBY_ADVISOR_LOG t
     WHERE t.TABLE_NAME = pkg_hcc_orderby_advisor.printReport.tableName
       AND t.OWNER = pkg_hcc_orderby_advisor.printReport.ownerName;

    DBMS_OUTPUT.PUT_LINE('Measured ORDER BY options: ' || TO_CHAR(ln_ordersCount));

    DBMS_OUTPUT.PUT_LINE(CHR(10) || 'Best options:');

    SELECT MAX(LENGTH(t.ORDER_BY_COLS))
      INTO ln_outputLength
      FROM T_HCC_ORDERBY_ADVISOR_LOG t
     WHERE t.TABLE_NAME = pkg_hcc_orderby_advisor.printReport.tableName
       AND t.OWNER = pkg_hcc_orderby_advisor.printReport.ownerName
       AND t.BYTES = ln_bytesMin;

    DBMS_OUTPUT.PUT_LINE(RPAD('ORDER BY', ln_outputLength) || ' Table size, bytes');

    FOR curs IN (
      SELECT /*+ NOPARALLEL */ t.TABLE_NAME,
             t.ORDER_BY_COLS,
             t.BYTES
        FROM T_HCC_ORDERBY_ADVISOR_LOG t
       WHERE t.TABLE_NAME = pkg_hcc_orderby_advisor.printReport.tableName
         AND t.OWNER = pkg_hcc_orderby_advisor.printReport.ownerName
         AND t.BYTES = ln_bytesMin
       ORDER BY t.COLS_USED,
             t.ORDER_BY_COLS
    )
    LOOP
      DBMS_OUTPUT.PUT_LINE(RPAD(curs.ORDER_BY_COLS, ln_outputLength) || ' ' || TO_CHAR(curs.BYTES));
    END LOOP;

    DBMS_OUTPUT.PUT_LINE(CHR(10));

  END printReport;


  -- For a given table name, owner, rows limit and ORDER BY statement, do a create-measure-drop iteration
  PROCEDURE processAnOrdering(ownerName IN T_HCC_ORDERBY_ADVISOR_LOG.OWNER%TYPE, tableName IN T_HCC_ORDERBY_ADVISOR_LOG.TABLE_NAME%TYPE,
    orderByCols IN T_HCC_ORDERBY_ADVISOR_LOG.ORDER_BY_COLS%TYPE)
  AS
    ln_segmentBytes USER_SEGMENTS.BYTES%TYPE;
  BEGIN
    -- Create a temporary table
    EXECUTE IMMEDIATE pkg_hcc_orderby_advisor.getDDLStatement(pkg_hcc_orderby_advisor.gv_tempTableName2, USER,
      pkg_hcc_orderby_advisor.gv_tempTableName1, pkg_hcc_orderby_advisor.processAnOrdering.orderByCols);

    -- Measure the space used
    SELECT SUM(BYTES)
      INTO ln_segmentBytes
      FROM USER_SEGMENTS
     WHERE SEGMENT_NAME = pkg_hcc_orderby_advisor.gv_tempTableName2;

    -- Log the results
    UPDATE T_HCC_ORDERBY_ADVISOR_LOG t
       SET t.BYTES = ln_segmentBytes,
           t.DATE_ANALYSED = SYSTIMESTAMP
     WHERE t.TABLE_NAME = pkg_hcc_orderby_advisor.processAnOrdering.tableName
       AND t.OWNER = pkg_hcc_orderby_advisor.processAnOrdering.ownerName
       AND (t.ORDER_BY_COLS = pkg_hcc_orderby_advisor.processAnOrdering.orderByCols OR
           (pkg_hcc_orderby_advisor.processAnOrdering.orderByCols IS NULL AND t.ORDER_BY_COLS IS NULL));

    -- Drop the temporary table
    EXECUTE IMMEDIATE 'DROP TABLE ' || pkg_hcc_orderby_advisor.gv_tempTableName2 || ' PURGE';
  END processAnOrdering;


  PROCEDURE generatePossibleOrderings(ownerName IN T_HCC_ORDERBY_ADVISOR_LOG.OWNER%TYPE,
    tableName IN T_HCC_ORDERBY_ADVISOR_LOG.TABLE_NAME%TYPE, columnsCount IN PLS_INTEGER)
  AS
  BEGIN
    IF pkg_hcc_orderby_advisor.generatePossibleOrderings.columnsCount = 0 THEN
      -- Set no ordering
      INSERT INTO T_HCC_ORDERBY_ADVISOR_LOG(TABLE_NAME, OWNER, ORDER_BY_COLS, COLS_USED)
      VALUES (pkg_hcc_orderby_advisor.generatePossibleOrderings.tableName, pkg_hcc_orderby_advisor.generatePossibleOrderings.ownerName,
          NULL, pkg_hcc_orderby_advisor.generatePossibleOrderings.columnsCount);

    ELSIF pkg_hcc_orderby_advisor.generatePossibleOrderings.columnsCount = 1 THEN
      -- Order by every single column
      INSERT INTO T_HCC_ORDERBY_ADVISOR_LOG(TABLE_NAME, OWNER, ORDER_BY_COLS, COLS_USED)
      SELECT t.TABLE_NAME,
             t.OWNER,
             '"' || COLUMN_NAME || '"' AS ORDER_BY_COLS,
             pkg_hcc_orderby_advisor.generatePossibleOrderings.columnsCount AS COLS_USED
        FROM ALL_TAB_COLUMNS t
       WHERE t.TABLE_NAME = pkg_hcc_orderby_advisor.generatePossibleOrderings.tableName
         AND t.OWNER = pkg_hcc_orderby_advisor.generatePossibleOrderings.ownerName;

    ELSE
      -- Choose the options that seem perspective
      INSERT INTO T_HCC_ORDERBY_ADVISOR_LOG (TABLE_NAME, OWNER, ORDER_BY_COLS, COLS_USED)
      SELECT tlog.TABLE_NAME,
             tlog.OWNER,
             tlog.ORDER_BY_COLS || ', ' || tcols.COLUMN_NAME AS ORDER_BY_COLS,
             pkg_hcc_orderby_advisor.generatePossibleOrderings.columnsCount AS COLS_USED
        FROM (SELECT t.TABLE_NAME,
                     t.OWNER,
                     t.ORDER_BY_COLS
                FROM T_HCC_ORDERBY_ADVISOR_LOG t
               WHERE t.TABLE_NAME = pkg_hcc_orderby_advisor.generatePossibleOrderings.tableName
                 AND t.OWNER = pkg_hcc_orderby_advisor.generatePossibleOrderings.ownerName
                 AND t.COLS_USED = pkg_hcc_orderby_advisor.generatePossibleOrderings.columnsCount - 1
                 AND EXISTS (SELECT NULL
                                    FROM T_HCC_ORDERBY_ADVISOR_LOG t2
                                   WHERE t2.TABLE_NAME = t.TABLE_NAME
                                     AND t2.OWNER = t.OWNER
                                     AND t2.COLS_USED = t.COLS_USED - 1
                                     AND t.ORDER_BY_COLS LIKE t2.ORDER_BY_COLS || '%'
                                     AND t2.BYTES IS NOT NULL
                                     AND t2.BYTES > t.BYTES)
             )tlog
       CROSS JOIN
             (SELECT '"' || t.COLUMN_NAME || '"' AS COLUMN_NAME
                FROM ALL_TAB_COLUMNS t
               WHERE t.TABLE_NAME = pkg_hcc_orderby_advisor.generatePossibleOrderings.tableName
                 AND t.OWNER = pkg_hcc_orderby_advisor.generatePossibleOrderings.ownerName
             ) tcols
       WHERE INSTR(tlog.ORDER_BY_COLS, tcols.COLUMN_NAME) = 0;

    END IF;

  END generatePossibleOrderings;


  -- Process not all the possible ORDER BY options, but only the most optimal ones
  PROCEDURE smartOrderingsProcessing(ownerName IN T_HCC_ORDERBY_ADVISOR_LOG.OWNER%TYPE,
    tableName IN T_HCC_ORDERBY_ADVISOR_LOG.TABLE_NAME%TYPE, columnsCountTotal IN PLS_INTEGER)
  AS
  BEGIN

    FOR i IN 0..pkg_hcc_orderby_advisor.smartOrderingsProcessing.columnsCountTotal
    LOOP
      generatePossibleOrderings(pkg_hcc_orderby_advisor.smartOrderingsProcessing.ownerName,
        pkg_hcc_orderby_advisor.smartOrderingsProcessing.tableName, i);

      FOR curs IN (
        SELECT /*+ NOPARALLEL */ t.TABLE_NAME,
               t.OWNER,
               t.ORDER_BY_COLS
          FROM T_HCC_ORDERBY_ADVISOR_LOG t
         WHERE t.TABLE_NAME = pkg_hcc_orderby_advisor.smartOrderingsProcessing.tableName
           AND t.OWNER = pkg_hcc_orderby_advisor.smartOrderingsProcessing.ownerName
           AND t.BYTES IS NULL
           AND t.DATE_ANALYSED IS NULL
           AND t.COLS_USED = i
         ORDER BY t.ORDER_BY_COLS
      )
      LOOP
        pkg_hcc_orderby_advisor.processAnOrdering(curs.OWNER, curs.TABLE_NAME, curs.ORDER_BY_COLS);
      END LOOP;
    END LOOP;

  END smartOrderingsProcessing;


  PROCEDURE analyseTable(tableName IN VARCHAR2, ownerName IN VARCHAR2)
  AS
    ln_columnsCountTotal  PLS_INTEGER;
    ln_tableCount         PLS_INTEGER;
    ln_tempTableExists    PLS_INTEGER;
  BEGIN

    -- Check if the name for the temporary table was chosen properly (if it already exists, raise an error)
    SELECT COUNT(*)
      INTO ln_tempTableExists
      FROM USER_TABLES t
     WHERE t.TABLE_NAME = pkg_hcc_orderby_advisor.gv_tempTableName2;

    IF ln_tempTableExists <> 0 THEN
      /*
        In case you're reading this text, it means that something went wrong.
        I chose a name for a table that would be created and dropped multiple times for size comparison purposes only.
        The name is declared in the variable gv_tempTableName2 above.
        If you already have an object with the same name, it may be because of two reasons:
          1. It's an object you use in your database and you need it. In that case, please change gv_tempTableName2 value.
          2. It is left from a malfunctioned test run. In that case, please drop the table.
      */
      RAISE_APPLICATION_ERROR(-20001, 'Error. Table ' || pkg_hcc_orderby_advisor.gv_tempTableName2 || ' already exists. Please review the PKG_HCC_ORDERBY_ADVISOR code or drop the table.');
    END IF;

    -- Clear previous tests results
    DELETE FROM T_HCC_ORDERBY_ADVISOR_LOG t
     WHERE t.TABLE_NAME = pkg_hcc_orderby_advisor.analyseTable.tableName
       AND t.OWNER = pkg_hcc_orderby_advisor.analyseTable.ownerName;

    -- Get the number of columns in the table
    SELECT COUNT(*)
      INTO ln_columnsCountTotal
      FROM ALL_TAB_COLS t
     WHERE t.TABLE_NAME = pkg_hcc_orderby_advisor.analyseTable.tableName
       AND t.OWNER = pkg_hcc_orderby_advisor.analyseTable.ownerName;

    -- If no columns were found, raise an error
    IF (ln_columnsCountTotal = 0) THEN
      RAISE_APPLICATION_ERROR(-20001, 'Error. Table ' || pkg_hcc_orderby_advisor.analyseTable.ownerName ||
        '.' || pkg_hcc_orderby_advisor.analyseTable.tableName || ' was not found.');
    END IF;

    -- Check if table has any data
    EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM ' || pkg_hcc_orderby_advisor.analyseTable.ownerName || '.' || pkg_hcc_orderby_advisor.analyseTable.tableName
      INTO ln_tableCount;

    -- If not, any ordering makes no sense, skip it
    IF ln_tableCount = 0 THEN
      INSERT INTO T_HCC_ORDERBY_ADVISOR_LOG (TABLE_NAME, OWNER, BYTES)
      VALUES (pkg_hcc_orderby_advisor.analyseTable.tableName, pkg_hcc_orderby_advisor.analyseTable.ownerName, 0);

      pkg_hcc_orderby_advisor.printReport(pkg_hcc_orderby_advisor.analyseTable.tableName, pkg_hcc_orderby_advisor.analyseTable.ownerName);

      RETURN;
    END IF;

    BEGIN
      -- Generate a sample table
      EXECUTE IMMEDIATE pkg_hcc_orderby_advisor.getDDLStatement(pkg_hcc_orderby_advisor.gv_tempTableName1, pkg_hcc_orderby_advisor.analyseTable.ownerName,
        pkg_hcc_orderby_advisor.analyseTable.tableName, 'DBMS_RANDOM.VALUE', pkg_hcc_orderby_advisor.gn_sampleTableRows);
    EXCEPTION
      WHEN OTHERS THEN
        -- ORA-01031: insufficient privileges
        IF (SQLCODE = -1031) THEN
          RAISE_APPLICATION_ERROR(-20001, 'Error. ORA-01031: insufficient privileges. Please run the following command: GRANT CREATE TABLE TO ' || USER);
        ELSE
          RAISE;
        END IF;
    END;

    -- Do the tests
    pkg_hcc_orderby_advisor.smartOrderingsProcessing(pkg_hcc_orderby_advisor.analyseTable.ownerName,
      pkg_hcc_orderby_advisor.analyseTable.tableName, ln_columnsCountTotal);

    -- Drop the sample table
    EXECUTE IMMEDIATE 'DROP TABLE ' || pkg_hcc_orderby_advisor.gv_tempTableName1 || ' PURGE';

    -- Print a report
    pkg_hcc_orderby_advisor.printReport(pkg_hcc_orderby_advisor.analyseTable.tableName, pkg_hcc_orderby_advisor.analyseTable.ownerName);

  END analyseTable;


  PROCEDURE analyseAllTables(ownerName IN VARCHAR2 := USER)
  AS
  BEGIN

    FOR curs IN (
      SELECT /*+ NOPARALLEL */ t.TABLE_NAME
        FROM ALL_TABLES t
       WHERE t.OWNER = pkg_hcc_orderby_advisor.analyseAllTables.ownerName
       ORDER BY t.TABLE_NAME
    )
    LOOP
      pkg_hcc_orderby_advisor.analyseTable(curs.TABLE_NAME, pkg_hcc_orderby_advisor.analyseAllTables.ownerName);
    END LOOP;

  END analyseAllTables;

END PKG_HCC_ORDERBY_ADVISOR;
/
