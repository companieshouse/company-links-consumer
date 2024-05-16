Feature: Process company profile links

# CHARGES
  Scenario: Company profile message with no Charges link and existing Charges is processed successfully
    Given Company links consumer service is running
    And Company profile exists with no "charges" link for company "00006401"
    And "charges" exist for company "00006401"
    When A valid avro Company Profile without "charges" link message is sent to the Kafka topic "stream-company-profile"
    Then The Company Profile message is successfully consumed and company-profile-api PATCH endpoint is invoked with "charges" link payload

  Scenario: Company profile message with existing Charges link does not update
    Given Company links consumer service is running
    And Company profile exists with "charges" link for company "00006401"
    When A valid avro Company Profile with all links message is sent to the Kafka topic "stream-company-profile"
    Then The Company Profile message is successfully consumed and company-profile-api PATCH endpoint is NOT invoked with "charges" link payload

  Scenario: Company profile message with no Charges link and no Charges is processed successfully
    Given Company links consumer service is running
    And Company profile exists with no "charges" link for company "00006401"
    And "charges" do not exist for company "00006401"
    When A valid avro Company Profile without "charges" link message is sent to the Kafka topic "stream-company-profile"
    Then The Company Profile message is successfully consumed and company-profile-api PATCH endpoint is NOT invoked with "charges" link payload

# PSCS
  Scenario: Company profile message with no PSC link and existing PSC is processed successfully
    Given Company links consumer service is running
    And Company profile exists with no "persons-with-significant-control" link for company "00006401"
    And "persons-with-significant-control" exist for company "00006401"
    When A valid avro Company Profile without "persons-with-significant-control" link message is sent to the Kafka topic "stream-company-profile"
    Then The Company Profile message is successfully consumed and company-profile-api PATCH endpoint is invoked with "persons-with-significant-control" link payload

  Scenario: Company profile message with existing PSC link does not update
    Given Company links consumer service is running
    And Company profile exists with "persons-with-significant-control" link for company "00006401"
    When A valid avro Company Profile with all links message is sent to the Kafka topic "stream-company-profile"
    Then The Company Profile message is successfully consumed and company-profile-api PATCH endpoint is NOT invoked with "persons-with-significant-control" link payload

  Scenario: Company profile message with no PSC link and no PSC is processed successfully
    Given Company links consumer service is running
    And Company profile exists with no "persons-with-significant-control" link for company "00006401"
    And "persons-with-significant-control" do not exist for company "00006401"
    When A valid avro Company Profile without "persons-with-significant-control" link message is sent to the Kafka topic "stream-company-profile"
    Then The Company Profile message is successfully consumed and company-profile-api PATCH endpoint is NOT invoked with "persons-with-significant-control" link payload

# FILING HISTORY
  Scenario: Company profile message with no filing history link and existing filing history is processed successfully
    Given Company links consumer service is running
    And Company profile exists with no "filing-history" link for company "00006401"
    And "filing history" exist for company "00006401"
    When A valid avro Company Profile without "filing-history" link message is sent to the Kafka topic "stream-company-profile"
    Then The Company Profile message is successfully consumed and company-profile-api PATCH endpoint is invoked with filing-history link payload

  Scenario: Company profile message with existing filing history link does not update
    Given Company links consumer service is running
    And Company profile exists with "filing history" link for company "00006401"
    When A valid avro Company Profile with all links message is sent to the Kafka topic "stream-company-profile"
    Then The Company Profile message is successfully consumed and company-profile-api PATCH endpoint is NOT invoked with "filing history" link payload

  Scenario: Company profile message with no Charges link and no Charges is processed successfully
    Given Company links consumer service is running
    And Company profile exists with no "filing-history" link for company "00006401"
    And "filing history" do not exist for company "00006401"
    When A valid avro Company Profile without "filing-history" link message is sent to the Kafka topic "stream-company-profile"
    Then The Company Profile message is successfully consumed and company-profile-api PATCH endpoint is NOT invoked with "filing history" link payload