Feature: Process company links information for exemptions

  Scenario: PATCH company exemptions link successfully
    Given Company links consumer is available
    When  A valid message is consumed
    Then  A PATCH request is sent to the API
    And No messages are placed on the invalid, error or retry topics

  Scenario: Consume invalid exemptions message
    Given Company links consumer is available
    When An invalid message is consumed
    Then The message is placed on the "invalid" topic

  Scenario: Consume an exemptions message with an invalid event type
    Given Company links consumer is available
    When A message is consumed with invalid event type
    Then The message is placed on the "invalid" topic

  Scenario: Consume a valid exemptions message but the user is not authorized
    Given Company links consumer is available
    And   The user is unauthorized
    When A valid message is consumed
    Then The message is placed on the "invalid" topic

  Scenario: Consume a valid exemptions message but the company profile api is unavailable
    Given Company links consumer is available
    And   The company profile api is unavailable
    When A valid message is consumed
    Then The message is placed on the "retry" topic