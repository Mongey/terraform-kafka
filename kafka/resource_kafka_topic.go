package kafka

import (
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/hashicorp/terraform-plugin-sdk/helper/resource"
	"github.com/hashicorp/terraform-plugin-sdk/helper/schema"
)

func positiveValue(val interface{}, key string) (warns []string, errs []error) {
	v := val.(int)
	if v < 1 {
		errs = append(errs, fmt.Errorf("%q must be greater than 0, got: %d", key, v))
	}
	return
}

func kafkaTopicResource() *schema.Resource {
	return &schema.Resource{
		Create: topicCreate,
		Read:   topicRead,
		Update: topicUpdate,
		Delete: topicDelete,
		Importer: &schema.ResourceImporter{
			State: schema.ImportStatePassthrough,
		},
		CustomizeDiff: customDiff,
		Schema: map[string]*schema.Schema{
			"name": {
				Type:        schema.TypeString,
				Required:    true,
				ForceNew:    true,
				Description: "The name of the topic.",
			},
			"partitions": {
				Type:         schema.TypeInt,
				Required:     true,
				Description:  "Number of partitions.",
				ValidateFunc: positiveValue,
			},
			"replication_factor": {
				Type:         schema.TypeInt,
				Required:     true,
				ForceNew:     true,
				Description:  "Number of replicas.",
				ValidateFunc: positiveValue,
			},
			"config": {
				Type:        schema.TypeMap,
				Optional:    true,
				ForceNew:    false,
				Description: "A map of string k/v attributes.",
				Elem:        schema.TypeString,
			},
		},
	}
}

func topicCreate(d *schema.ResourceData, meta interface{}) error {
	c := meta.(*LazyClient)
	t := metaToTopic(d, meta)

	err := c.CreateTopic(t)
	if err != nil {
		return err
	}

	d.SetId(t.Name)
	return nil
}

func topicUpdate(d *schema.ResourceData, meta interface{}) error {
	c := meta.(*LazyClient)
	t := metaToTopic(d, meta)

	err := c.UpdateTopic(t)
	if err != nil {
		return err
	}
	if d.HasChange("partitions") {
		// update should only be called when we're increasing partitions
		oi, ni := d.GetChange("partitions")
		oldPartitions := oi.(int)
		newPartitions := ni.(int)
		log.Printf("[INFO] Updating partitions from %d to %d", oldPartitions, newPartitions)
		t.Partitions = int32(newPartitions)
		err = c.AddPartitions(t)
		if err != nil {
			return err
		}
	}

	timeout := time.Duration(c.Config.Timeout) * time.Second
	stateConf := &resource.StateChangeConf{
		Pending:      []string{"Updating"},
		Target:       []string{"Ready"},
		Refresh:      topicRefreshFunc(c, d.Id(), t),
		Timeout:      timeout,
		Delay:        1 * time.Second,
		PollInterval: 1 * time.Second,
		MinTimeout:   2 * time.Second,
	}

	_, err = stateConf.WaitForState()

	if err != nil {
		return fmt.Errorf(
			"Error waiting for topic (%s) to become ready: %s",
			d.Id(), err)
	}

	return err
}

func topicRefreshFunc(client *LazyClient, topic string, expected Topic) resource.StateRefreshFunc {
	return func() (result interface{}, s string, err error) {
		log.Printf("[DEBUG] waiting for topic to update %s", topic)
		actual, err := client.ReadTopic(topic)
		if err != nil {
			log.Printf("[ERROR] could not read topic %s, %s", topic, err)
			return actual, "Error", err
		}

		if expected.Equal(actual) {
			return actual, "Ready", nil
		}

		return nil, fmt.Sprintf("%v != %v", strPtrMapToStrMap(actual.Config), strPtrMapToStrMap(expected.Config)), nil
	}
}

func topicDelete(d *schema.ResourceData, meta interface{}) error {
	c := meta.(*LazyClient)
	t := metaToTopic(d, meta)

	err := c.DeleteTopic(t.Name)
	if err != nil {
		return err
	}

	stateConf := &resource.StateChangeConf{
		Pending:      []string{"Pending"},
		Target:       []string{"Deleted"},
		Refresh:      topicDeleteFunc(c, d.Id(), t),
		Timeout:      300 * time.Second,
		Delay:        3 * time.Second,
		PollInterval: 2 * time.Second,
		MinTimeout:   20 * time.Second,
	}
	_, err = stateConf.WaitForState()

	if err != nil {
		return fmt.Errorf("Error waiting for topic (%s) to delete: %s", d.Id(), err)
	}

	d.SetId("")
	return err
}

func topicDeleteFunc(client *LazyClient, id string, t Topic) resource.StateRefreshFunc {
	return func() (result interface{}, s string, err error) {
		topic, err := client.ReadTopic(t.Name)

		if err != nil {
			_, ok := err.(TopicMissingError)
			if ok {
				return topic, "Deleted", nil
			}
			return topic, "UNKNOWN", err
		}
		return topic, "Pending", nil
	}

}

func topicRead(d *schema.ResourceData, meta interface{}) error {
	name := d.Id()
	client := meta.(*LazyClient)
	topic, err := client.ReadTopic(name)

	if err != nil {
		log.Printf("[ERROR] Error getting topics %s from Kafka", err)
		_, ok := err.(TopicMissingError)
		if ok {
			d.SetId("")
			return nil
		}

		return err
	}

	log.Printf("[DEBUG] Setting the state from Kafka %v", topic)
	d.Set("name", topic.Name)
	d.Set("partitions", topic.Partitions)
	d.Set("replication_factor", topic.ReplicationFactor)
	d.Set("config", topic.Config)

	return nil
}

func customDiff(diff *schema.ResourceDiff, meta interface{}) error {
	log.Printf("[INFO] Checking the diff!")
	log.Printf("[DEBUG] Resource Id = %s", diff.Id())
	if diff.HasChange("partitions") {
		log.Printf("[INFO] Partitions have changed!")
		o, n := diff.GetChange("partitions")
		oi := o.(int)
		ni := n.(int)
		log.Printf("Partitions is changing from %d to %d", oi, ni)
		if ni < oi {
			log.Printf("Partitions decreased from %d to %d. Forcing new resource", oi, ni)
			diff.ForceNew("partitions")
		}
	}
	if diff.HasChange("config") {
		log.Printf("[INFO] Config has changed!")
		if diff.Id() != "" {
			topic := diffToTopic(diff, meta)
			client := meta.(*LazyClient)
			err := client.CheckConfigDiff(topic)
			if err != nil {
				if strings.Contains(err.Error(), "create a new topic") {
					diff.ForceNew("config")
				} else {
					return err
				}
			}
		}
	}
	return nil
}
