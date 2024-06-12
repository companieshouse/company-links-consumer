Feature: Process company profile links for error scenarios

  Scenario: Consume invalid company profile message
    Given Company links consumer is available
    When An invalid message is consumed from the "company-profile" stream
    Then The message is placed on the "invalid" topic

  Scenario: Consume a company profile message with an invalid event type
    Given Company links consumer is available
    When A message is consumed with invalid event type from the "company-profile" stream
    Then The message is placed on the "invalid" topic

  Scenario: Consume a valid company profile message but the user is not authorized
    Given Company links consumer is available
    And The user is not authorized
    When A valid "changed" message is consumed from the "company-profile" stream
    Then The message is placed on the "retry" topic
    And The message is placed on the "error" topic

  Scenario: Consume a valid company profile message but the company profile api is unavailable
    Given Company links consumer is available
    And The company profile api is unavailable
    And "persons-with-significant-control" exist for company "00006400"
    When A valid "changed" message is consumed from the "company-profile" stream
    Then The message is placed on the "retry" topic
    And The message is placed on the "error" topic

  Scenario: Process message when the api returns 503
    Given Company links consumer is available
    And The company profile api is unavailable
    And "persons-with-significant-control" exist for company "00006400"
    When A valid "changed" message is consumed from the "company-profile" stream
    Then the message should retry 3 times on the company-profile topic and then error

