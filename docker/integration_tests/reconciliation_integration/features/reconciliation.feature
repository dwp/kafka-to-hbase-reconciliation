Feature: Matching records in database are reconciled, mismatched are not

  Scenario: Table is partitioned with records
    Given the ucfs table has been created and populated, and the reconciler service has been ran
    Then the ucfs table will have 10000 records
    And the equalities table will have 10000 reconciled records
