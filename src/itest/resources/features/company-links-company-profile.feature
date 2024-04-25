Feature: Process company profile links

  Scenario: Company profile message for no PSC link and Existing PSC is processed successfully
    Given Company links consumer service is running
    And Company profile exists with no PSC link for company "00006401"
    And Psc exists for company "00006401"
    When A valid avro Company Profile message is sent to the Kafka topic "stream-company-profile"
    Then The message is successfully consumed and company-profile-api PATCH endpoint is invoked with PSC link payload