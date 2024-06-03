Feature: Process company profile links

# CHARGES
  Scenario: Company profile message with no Charges link and existing Charges is processed successfully
    Given Company links consumer service is running
    And Company profile exists with no Charges link for company "00006401"
    And "charges" exist for company "00006401"
    When A valid avro Company Profile without "charges" link message is sent to the Kafka topic "stream-company-profile"
    Then The Company Profile message is successfully consumed and company-profile-api PATCH endpoint is invoked with Charges link payload
    And No messages are placed on the invalid, error or retry topics

  Scenario: Company profile message with existing Charges link does not update
    Given Company links consumer service is running
    And Company profile exists with Charges link for company "00006401"
    When A valid avro Company Profile with all links message is sent to the Kafka topic "stream-company-profile"
    Then The Company Profile message is successfully consumed and company-profile-api PATCH endpoint is NOT invoked with Charges link payload
    And No messages are placed on the invalid, error or retry topics

  Scenario: Company profile message with no Charges link and no Charges is processed successfully
    Given Company links consumer service is running
    And Company profile exists with no Charges link for company "00006401"
    And "charges" do not exist for company "00006401"
    When A valid avro Company Profile without "charges" link message is sent to the Kafka topic "stream-company-profile"
    Then The Company Profile message is successfully consumed and company-profile-api PATCH endpoint is NOT invoked with Charges link payload
    And No messages are placed on the invalid, error or retry topics

# EXEMPTIONS
  Scenario: Company profile message with no Exemptions link and existing Exemptions is processed successfully
    Given Company links consumer service is running
    And Company profile exists with no "exemptions" link for company "00006401"
    And "exemptions" exist for company "00006401"
    When A valid avro Company Profile without "exemptions" link message is sent to the Kafka topic "stream-company-profile"
    Then The Company Profile message is successfully consumed and company-profile-api PATCH endpoint is invoked with "exemptions" link payload
    And No messages are placed on the invalid, error or retry topics

  Scenario: Company profile message with existing Exemptions link does not update
    Given Company links consumer service is running
    And Company profile exists with "exemptions" link for company "00006401"
    When A valid avro Company Profile with all links message is sent to the Kafka topic "stream-company-profile"
    Then The Company Profile message is successfully consumed and company-profile-api PATCH endpoint is NOT invoked with "exemptions" link payload
    And No messages are placed on the invalid, error or retry topics

  Scenario: Company profile message with no Exemptions link and no Exemptions is processed successfully
    Given Company links consumer service is running
    And Company profile exists with no "exemptions" link for company "00006401"
    And "exemptions" do not exist for company "00006401"
    When A valid avro Company Profile without "exemptions" link message is sent to the Kafka topic "stream-company-profile"
    Then The Company Profile message is successfully consumed and company-profile-api PATCH endpoint is NOT invoked with "exemptions" link payload
    And No messages are placed on the invalid, error or retry topics

# FILING HISTORY
  Scenario: Company profile message with no Filing history link and existing Filing history is processed successfully
    Given Company links consumer service is running
    And Company profile exists with no "filing-history" link for company "00006401"
    And "filing-history" exist for company "00006401"
    When A valid avro Company Profile without "filing-history" link message is sent to the Kafka topic "stream-company-profile"
    Then The Company Profile message is successfully consumed and company-profile-api PATCH endpoint is invoked with Filing History link payload
    And No messages are placed on the invalid, error or retry topics

  Scenario: Company profile message with existing Filing history link does not update
    Given Company links consumer service is running
    And Company profile exists with "filing-history" link for company "00006401"
    When A valid avro Company Profile with all links message is sent to the Kafka topic "stream-company-profile"
    Then The Company Profile message is successfully consumed and company-profile-api PATCH endpoint is NOT invoked with "filing-history" link payload
    And No messages are placed on the invalid, error or retry topics

  Scenario: Company profile message with no Filing history link and no Filing history is processed successfully
    Given Company links consumer service is running
    And Company profile exists with no "filing-history" link for company "00006401"
    And "filing-history" do not exist for company "00006401"
    When A valid avro Company Profile without "filing-history" link message is sent to the Kafka topic "stream-company-profile"
    Then The Company Profile message is successfully consumed and company-profile-api PATCH endpoint is NOT invoked with "filing-history" link payload
    And No messages are placed on the invalid, error or retry topics

# INSOLVENCY
  Scenario: Company profile message with no Insolvency link and existing Insolvencies is processed successfully
    Given Company links consumer service is running
    And Company profile exists with no Insolvency link for company "00006401"
    And "insolvency" exist for company "00006401"
    When A valid avro Company Profile without "insolvency" link message is sent to the Kafka topic "stream-company-profile"
    Then The Company Profile message is successfully consumed and company-profile-api PATCH endpoint is invoked with Insolvency link payload
    And No messages are placed on the invalid, error or retry topics

  Scenario: Company profile message with existing Insolvency link does not update
    Given Company links consumer service is running
    And Company profile exists with Insolvency link for company "00006401"
    When A valid avro Company Profile with all links message is sent to the Kafka topic "stream-company-profile"
    Then The Company Profile message is successfully consumed and company-profile-api PATCH endpoint is NOT invoked with Insolvency link payload
    And No messages are placed on the invalid, error or retry topics

  Scenario: Company profile message with no Insolvency link and no Insolvencies is processed successfully
    Given Company links consumer service is running
    And Company profile exists with no Insolvency link for company "00006401"
    And "insolvency" do not exist for company "00006401"
    When A valid avro Company Profile without "insolvency" link message is sent to the Kafka topic "stream-company-profile"
    Then The Company Profile message is successfully consumed and company-profile-api PATCH endpoint is NOT invoked with Insolvency link payload
    And No messages are placed on the invalid, error or retry topics

# OFFICERS
  Scenario: Company profile message with no Officer link and existing Officer is processed successfully
    Given Company links consumer service is running
    And Company profile exists with no "officers" link for company "00006401"
    And "officers" exist for company "00006401"
    When A valid avro Company Profile without "officers" link message is sent to the Kafka topic "stream-company-profile"
    Then The Company Profile message is successfully consumed and company-profile-api PATCH endpoint is invoked with "officers" link payload
    And No messages are placed on the invalid, error or retry topics

  Scenario: Company profile message with existing Officer link does not update
    Given Company links consumer service is running
    And Company profile exists with "officers" link for company "00006401"
    When A valid avro Company Profile with all links message is sent to the Kafka topic "stream-company-profile"
    Then The Company Profile message is successfully consumed and company-profile-api PATCH endpoint is NOT invoked with "officers" link payload
    And No messages are placed on the invalid, error or retry topics

  Scenario: Company profile message with no Officers link and no Officers is processed successfully
    Given Company links consumer service is running
    And Company profile exists with no "officers" link for company "00006401"
    And "officers" do not exist for company "00006401"
    When A valid avro Company Profile without "officers" link message is sent to the Kafka topic "stream-company-profile"
    Then The Company Profile message is successfully consumed and company-profile-api PATCH endpoint is NOT invoked with "officers" link payload
    And No messages are placed on the invalid, error or retry topics

# PSCS
  Scenario: Company profile message with no PSC link and existing PSC is processed successfully
    Given Company links consumer service is running
    And Company profile exists with no "persons-with-significant-control" link for company "00006401"
    And "persons-with-significant-control" exist for company "00006401"
    When A valid avro Company Profile without "persons-with-significant-control" link message is sent to the Kafka topic "stream-company-profile"
    Then The Company Profile message is successfully consumed and company-profile-api PATCH endpoint is invoked with "persons-with-significant-control" link payload
    And No messages are placed on the invalid, error or retry topics

  Scenario: Company profile message with existing PSC link does not update
    Given Company links consumer service is running
    And Company profile exists with "persons-with-significant-control" link for company "00006401"
    When A valid avro Company Profile with all links message is sent to the Kafka topic "stream-company-profile"
    Then The Company Profile message is successfully consumed and company-profile-api PATCH endpoint is NOT invoked with "persons-with-significant-control" link payload
    And No messages are placed on the invalid, error or retry topics

  Scenario: Company profile message with no PSC link and no PSC is processed successfully
    Given Company links consumer service is running
    And Company profile exists with no "persons-with-significant-control" link for company "00006401"
    And "persons-with-significant-control" do not exist for company "00006401"
    When A valid avro Company Profile without "persons-with-significant-control" link message is sent to the Kafka topic "stream-company-profile"
    Then The Company Profile message is successfully consumed and company-profile-api PATCH endpoint is NOT invoked with "persons-with-significant-control" link payload
    And No messages are placed on the invalid, error or retry topics

#PSC STATEMENTS
  Scenario: Company profile message with no PSC Statements link and existing PSC Statements is processed successfully
    Given Company links consumer service is running
    And Company profile exists with no "persons-with-significant-control-statements" link for company "00006401"
    And "persons-with-significant-control-statements" exist for company "00006401"
    When A valid avro Company Profile without "persons-with-significant-control-statements" link message is sent to the Kafka topic "stream-company-profile"
    Then The Company Profile message is successfully consumed and company-profile-api PATCH endpoint is invoked with "persons-with-significant-control-statements" link payload
    And No messages are placed on the invalid, error or retry topics

  Scenario: Company profile message with existing PSC Statements link does not update
    Given Company links consumer service is running
    And Company profile exists with "persons-with-significant-control-statements" link for company "00006401"
    When A valid avro Company Profile with all links message is sent to the Kafka topic "stream-company-profile"
    Then The Company Profile message is successfully consumed and company-profile-api PATCH endpoint is NOT invoked with "persons-with-significant-control-statements" link payload
    And No messages are placed on the invalid, error or retry topics

  Scenario: Company profile message with no PSC Statements link and no PSC Statements is processed successfully
    Given Company links consumer service is running
    And Company profile exists with no "persons-with-significant-control-statements" link for company "00006401"
    And "persons-with-significant-control-statements" do not exist for company "00006401"
    When A valid avro Company Profile without "persons-with-significant-control-statements" link message is sent to the Kafka topic "stream-company-profile"
    Then The Company Profile message is successfully consumed and company-profile-api PATCH endpoint is NOT invoked with "persons-with-significant-control-statements" link payload
    And No messages are placed on the invalid, error or retry topics
