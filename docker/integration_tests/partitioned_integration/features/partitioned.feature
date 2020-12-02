Feature: Matching records in partitioned database are reconciled, mismatched are not

  Scenario: Table is partitioned with records
    Given the equalities table has been created and populated, and the reconciler service has been ran
    Then the equalities table will have 1000 records
    And the partition p2 in equalities table will have 250 reconciled records
