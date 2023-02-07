Feature: Process company links information for exemptions - remove

  Scenario: PATCH company exemptions link successfully
    Given Company links consumer is available
    When  A valid "deleted" message is consumed
    Then  A remove link PATCH request is sent to the API
    And No messages are placed on the invalid, error or retry topics

  Scenario: Consume a valid exemptions message but the user is not authorized
    Given Company links consumer is available
    And   The user is unauthorized
    When A valid "deleted" message is consumed
    Then The message is placed on the "invalid" topic

  Scenario: Consume a valid exemptions message but the company profile api is unavailable
    Given Company links consumer is available
    And   The company profile api is unavailable
    When A valid "deleted" message is consumed
    Then The message is placed on the "retry" topic