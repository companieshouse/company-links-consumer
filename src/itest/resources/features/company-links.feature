Feature: Process company links information

  Scenario: Processing company links information successfully

    Given Company links consumer api service is running
    When a message is published to the topic "stream-insolvency"
    Then the insolvency consumer should consume and process the message
