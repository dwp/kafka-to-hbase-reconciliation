Feature: The alter table stored procedure adds, removes columns and partitions as necessary preserving the table data.

  Scenario: Table has required number of partitions
    Given the trim table has been created and populated and the trim service has been run
    Then the trim table will have 500 rows
    And none of the records on the trim table will be reconciled
