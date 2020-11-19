package kafka

import (
	"errors"
	"fmt"
	"log"

	"github.com/Shopify/sarama"
	"github.com/hashicorp/terraform-plugin-sdk/helper/schema"
)

type Topic struct {
	Name              string
	Partitions        int32
	ReplicationFactor int16
	Config            map[string]*string
}

func (t *Topic) Equal(other Topic) bool {
	mape := MapEq(other.Config, t.Config)

	if mape == nil && (other.Name == t.Name) && (other.Partitions == t.Partitions) && (other.ReplicationFactor == t.ReplicationFactor) {
		return true
	}
	return false
}

// ReplicaCount returns the replication_factor for a partition
// Returns an error if it cannot determine the count, or if the number of
// replicas is different across partitions
func ReplicaCount(c sarama.Client, topic string, partitions []int32) (int, error) {
	count := -1

	for _, p := range partitions {
		replicas, err := c.Replicas(topic, p)
		if err != nil {
			return -1, errors.New("Could not get replicas for partition")
		}
		if count == -1 {
			count = len(replicas)
		}
		if count != len(replicas) {
			return count, fmt.Errorf("The replica count isn't the same across partitions %d != %d", count, len(replicas))
		}
	}
	return count, nil

}

func configToResources(topic Topic) []*sarama.AlterConfigsResource {
	return []*sarama.AlterConfigsResource{
		{
			Type:          sarama.TopicResource,
			Name:          topic.Name,
			ConfigEntries: topic.Config,
		},
	}
}

func isDefault(tc *sarama.ConfigEntry, version int) bool {
	if version == 0 {
		return tc.Default
	}
	return tc.Source == sarama.SourceDefault || tc.Source == sarama.SourceStaticBroker
}

func metaToTopic(d *schema.ResourceData, meta interface{}) Topic {
	log.Printf("meta = %+v", meta)
	topicName := d.Get("name").(string)
	partitions := d.Get("partitions").(int)
	replicationFactor := d.Get("replication_factor").(int)
	convertedPartitions := int32(partitions)
	convertedRF := int16(replicationFactor)
	config := d.Get("config").(map[string]interface{})

	m2 := make(map[string]*string)
	for key, value := range config {
		switch value := value.(type) {
		case string:
			m2[key] = &value
		}
	}

	return Topic{
		Name:              topicName,
		Partitions:        convertedPartitions,
		ReplicationFactor: convertedRF,
		Config:            m2,
	}
}

func diffToTopic(d *schema.ResourceDiff, meta interface{}) Topic {
	topicName := d.Get("name").(string)
	partitions := d.Get("partitions").(int)
	replicationFactor := d.Get("replication_factor").(int)
	convertedPartitions := int32(partitions)
	convertedRF := int16(replicationFactor)
	config := d.Get("config").(map[string]interface{})

	m2 := make(map[string]*string)
	for key, value := range config {
		switch value := value.(type) {
		case string:
			m2[key] = &value
		}
	}

	return Topic{
		Name:              topicName,
		Partitions:        convertedPartitions,
		ReplicationFactor: convertedRF,
		Config:            m2,
	}
}
