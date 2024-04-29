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
    And The user is unauthorized
    When A valid "changed" message is consumed from the "company-profile" stream
    Then The message is placed on the "retry" topic
    And The message is placed on the "error" topic

  Scenario: Consume a valid company profile message but the company profile api is unavailable
    Given Company links consumer is available
    And The company profile api is unavailable
    When A valid "changed" message is consumed from the "company-profile" stream
    Then The message is placed on the "retry" topic
    And The message is placed on the "error" topic