package kafka

import (
	"crypto/tls"
	"fmt"
	"log"
	"sync"

	"github.com/Shopify/sarama"
)

type LazyClient struct {
	once    sync.Once
	initErr error
	inner   *Client
	Config  *Config
}

func (c *LazyClient) init() error {
	var err error

	c.once.Do(func() {
		c.inner, err = NewClient(c.Config)
		c.initErr = err
	})
	if c.Config != nil {
		log.Printf("[DEBUG] lazy client init %s; config, %v", c.initErr, c.Config.copyWithMaskedSensitiveValues())
	} else {
		log.Printf("[DEBUG] lazy client init %s", c.initErr)
	}
	if c.initErr == sarama.ErrBrokerNotAvailable || c.initErr == sarama.ErrOutOfBrokers {
		if c.Config.TLSEnabled {
			tlsError := c.checkTLSConfig()
			if tlsError != nil {
				return fmt.Errorf("%w\n%s", tlsError, c.initErr)
			}
		}
	}

	return c.initErr
}

func (c *LazyClient) checkTLSConfig() error {
	kafkaConfig, err := c.Config.newKafkaConfig()
	if err != nil {
		return err
	}

	brokers := *(c.Config.BootstrapServers)
	broker := brokers[0]
	tlsConf := kafkaConfig.Net.TLS.Config
	conn, err := tls.Dial("tcp", broker, tlsConf)
	if err != nil {
		return err
	}

	return conn.Handshake()
}

func (c *LazyClient) CheckConfigDiff(t Topic) error {
	err := c.init()
	if err != nil {
		return err
	}
	return c.inner.CheckConfigDiff(t)
}

func (c *LazyClient) CreateTopic(t Topic) error {
	err := c.init()
	if err != nil {
		return err
	}
	return c.inner.CreateTopic(t)
}

func (c *LazyClient) ReadTopic(name string) (Topic, error) {
	err := c.init()
	if err != nil {
		return Topic{}, err
	}
	return c.inner.ReadTopic(name)
}

func (c *LazyClient) UpdateTopic(t Topic) error {
	err := c.init()
	if err != nil {
		return err
	}
	return c.inner.UpdateTopic(t)
}

func (c *LazyClient) DeleteTopic(t string) error {
	err := c.init()
	if err != nil {
		return err
	}
	return c.inner.DeleteTopic(t)
}

func (c *LazyClient) AddPartitions(t Topic) error {
	err := c.init()
	if err != nil {
		return err
	}
	return c.inner.AddPartitions(t)
}

func (c *LazyClient) CreateACL(s StringlyTypedACL) error {
	err := c.init()
	if err != nil {
		return err
	}
	return c.inner.CreateACL(s)
}

func (c *LazyClient) ListACLs() ([]*sarama.ResourceAcls, error) {
	err := c.init()
	if err != nil {
		return nil, err
	}
	return c.inner.ListACLs()
}

func (c *LazyClient) DeleteACL(s StringlyTypedACL) error {
	err := c.init()
	if err != nil {
		return err
	}
	return c.inner.DeleteACL(s)
}
