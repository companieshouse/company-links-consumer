Feature: Process company profile links

  Scenario: Company profile message with no PSC link and existing PSC is processed successfully
    Given Company links consumer service is running
    And Company profile exists with no PSC link for company "00006401"
    And Psc exists for company "00006401"
    When A valid avro Company Profile message is sent to the Kafka topic "stream-company-profile"
    Then The Company Profile message is successfully consumed and company-profile-api PATCH endpoint is invoked with PSC link payload

  Scenario: Company profile message with existing PSC link does not update
    Given Company links consumer service is running
    And Company profile exists with PSC link for company "00006401"
    When A valid avro Company Profile message is sent to the Kafka topic "stream-company-profile" with a psc link
    Then The Company Profile message is successfully consumed and company-profile-api PATCH endpoint is NOT invoked and there were 0 total events

  Scenario: Company profile message with no PSC link and no PSC is processed successfully
    Given Company links consumer service is running
    And Company profile exists with no PSC link for company "00006401"
    And Psc does not exist for company "00006401"
    When A valid avro Company Profile message is sent to the Kafka topic "stream-company-profile"
    Then The Company Profile message is successfully consumed and company-profile-api PATCH endpoint is NOT invoked and there were 1 total events