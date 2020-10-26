package kafka

import (
	"fmt"
	"github.com/hashicorp/terraform-plugin-sdk/terraform"
	"testing"

	uuid "github.com/hashicorp/go-uuid"
	r "github.com/hashicorp/terraform-plugin-sdk/helper/resource"
)

func TestAccTopicConfigInfo(t *testing.T) {
	u, err := uuid.GenerateUUID()
	if err != nil {
		t.Fatal(err)
	}
	topicName := fmt.Sprintf("syslog-%s", u)

	r.Test(t, r.TestCase{
		Providers: accProvider(),
		PreCheck:  func() { testAccPreCheck(t) },
		Steps: []r.TestStep{
			{
				Config: fmt.Sprintf(dataSourceTopic_readTopicConfigInfo, topicName),
				Check:  testDataSource_checkTopicConfigForMissingTopic,
			},
			{
				Config: fmt.Sprintf(createResourceTopic_initialConfig, topicName),
			},
			{
				Config: fmt.Sprintf(dataSourceTopic_readTopicConfigInfo, topicName),
				Check:  testDataSource_checkTopicConfig,
			},
		},
	})
}

func testDataSource_checkTopicConfigForMissingTopic(s *terraform.State) error {
	resourceState := s.Modules[0].Resources["data.kafka_topic_config.test"]
	if resourceState == nil {
		return fmt.Errorf("resource not found in state")
	}

	instanceState := resourceState.Primary
	if instanceState == nil {
		return fmt.Errorf("resource has no primary instance")
	}

	if instanceState.ID != "" {
		return fmt.Errorf("topic resource present")
	}

	return nil
}

func testDataSource_checkTopicConfig(s *terraform.State) error {
	resourceState := s.Modules[0].Resources["data.kafka_topic_config.test"]
	if resourceState == nil {
		return fmt.Errorf("resource not found in state")
	}

	instanceState := resourceState.Primary
	if instanceState == nil {
		return fmt.Errorf("resource has no primary instance")
	}

	name := instanceState.ID

	if name != instanceState.Attributes["name"] {
		return fmt.Errorf("id doesn't match name")
	}

	if v, ok := instanceState.Attributes["replication_factor"]; ok && v != "1" {
		return fmt.Errorf("replication_factor did not match, got: %v", instanceState.Attributes["replication_factor"])
	}
	if v, ok := instanceState.Attributes["partitions"]; ok && v != "1" {
		return fmt.Errorf("partitions did not get match, got: %v", instanceState.Attributes["partitions"])
	}
	if v, ok := instanceState.Attributes["config.segment.ms"]; ok && v != "22222" {
		return fmt.Errorf("segment.ms did not get match, got: %v", instanceState.Attributes["config.segment.ms"])
	}
	
	return nil
}

const createResourceTopic_initialConfig = `
provider "kafka" {
  bootstrap_servers = ["localhost:9092"]
}

resource "kafka_topic" "test" {
  name               = "%s"
  replication_factor = 1
  partitions         = 1

  config = {
    "segment.ms" = "22222"
  }
}
`

const dataSourceTopic_readTopicConfigInfo = `
provider "kafka" {
  bootstrap_servers = ["localhost:9092"]
}

data "kafka_topic_config" "test" {
  name               = "%s"
}
`