Feature: Matching records in partitioned database are reconciled, mismatched are not

  Scenario: Equalities table is partitioned with records
    Given the equalities table has been created and populated, and the reconciler service has been ran
    Then the equalities table will have 1000 records
    Then partition p2 in the equalities table will have 250 records
    And across the equalities table there will be 500 reconciled records within 120 seconds

#  Scenario: UCFS table is partitioned with records
#    Given the ucfs table has been created and populated, and the reconciler service has been ran
#    Then the ucfs table will have 1000 records
#    Then partition p1 in the ucfs table will have 250 records
#    And across the ucfs table there will be 500 reconciled records within 120 seconds