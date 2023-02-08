Feature: Process company links information for officers

  Scenario: PATCH company officers link successfully - add link
    Given Company links consumer is available
    When  A valid "changed" message is consumed from the "officers" stream
    Then  A PATCH request is sent to the API
    And No messages are placed on the invalid, error or retry topics

  Scenario: Consume invalid officers message
    Given Company links consumer is available
    When An invalid message is consumed from the "officers" stream
    Then The message is placed on the "invalid" topic

  Scenario: Consume an officers message with an invalid event type
    Given Company links consumer is available
    When A message is consumed with invalid event type from the "officers" stream
    Then The message is placed on the "invalid" topic

  Scenario: Consume a valid officers message but the user is not authorized - add link
    Given Company links consumer is available
    And   The user is unauthorized
    When A valid "changed" message is consumed from the "officers" stream
    Then The message is placed on the "invalid" topic

  Scenario: Consume a valid officers message but the company profile api is unavailable - add link
    Given Company links consumer is available
    And   The company profile api is unavailable
    When A valid "changed" message is consumed from the "officers" stream
    Then The message is placed on the "retry" topic
    And The message is placed on the "error" topic

  Scenario: PATCH company officers link successfully - remove link
    Given Company links consumer is available
    When  A valid "deleted" message is consumed from the "officers" stream
    Then  A PATCH request is sent to the API
    And No messages are placed on the invalid, error or retry topics

  Scenario: Consume a valid officers message but the user is not authorized - remove link
    Given Company links consumer is available
    And   The user is unauthorized
    When A valid "deleted" message is consumed from the "officers" stream
    Then The message is placed on the "invalid" topic

  Scenario: Consume a valid officers message but the company profile api is unavailable - remove link
    Given Company links consumer is available
    And   The company profile api is unavailable
    When A valid "deleted" message is consumed from the "officers" stream
    Then The message is placed on the "retry" topic
    And  The message is placed on the "error" topic