package kafka

import (
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/hashicorp/terraform-plugin-sdk/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/terraform"
)

func Provider() terraform.ResourceProvider {
	return &schema.Provider{
		Schema: map[string]*schema.Schema{
			"bootstrap_servers": {
				Type:        schema.TypeList,
				Elem:        &schema.Schema{Type: schema.TypeString},
				DefaultFunc: envDefaultListFunc("KAFKA_BOOTSTRAP_SERVERS", nil),
				Required:    true,
				Description: "A list of Kafka brokers",
			},
			"ca_cert_file": &schema.Schema{
				Type:        schema.TypeString,
				Required:    false,
				Optional:    true,
				DefaultFunc: schema.EnvDefaultFunc("KAFKA_CA_CERT", nil),
				Description: "Path to a CA certificate file to validate the server's certificate.",
				Deprecated:  "This parameter is now deprecated and will be removed in a later release, please use `ca_cert` instead.",
			},
			"client_cert_file": &schema.Schema{
				Type:        schema.TypeString,
				Required:    false,
				Optional:    true,
				DefaultFunc: schema.EnvDefaultFunc("KAFKA_CLIENT_CERT", nil),
				Description: "Path to a file containing the client certificate.",
				Deprecated:  "This parameter is now deprecated and will be removed in a later release, please use `client_cert` instead.",
			},
			"client_key_file": &schema.Schema{
				Type:        schema.TypeString,
				Required:    false,
				Optional:    true,
				DefaultFunc: schema.EnvDefaultFunc("KAFKA_CLIENT_KEY", nil),
				Description: "Path to a file containing the private key that the certificate was issued for.",
				Deprecated:  "This parameter is now deprecated and will be removed in a later release, please use `client_key` instead.",
			},
			"ca_cert": &schema.Schema{
				Type:        schema.TypeString,
				Optional:    true,
				Required:    false,
				DefaultFunc: schema.EnvDefaultFunc("KAFKA_CA_CERT", nil),
				Description: "CA certificate file to validate the server's certificate.",
			},
			"client_cert": &schema.Schema{
				Type:        schema.TypeString,
				Required:    false,
				Optional:    true,
				DefaultFunc: schema.EnvDefaultFunc("KAFKA_CLIENT_CERT", nil),
				Description: "The client certificate.",
			},
			"client_key": &schema.Schema{
				Type:        schema.TypeString,
				Optional:    true,
				Required:    false,
				DefaultFunc: schema.EnvDefaultFunc("KAFKA_CLIENT_KEY", nil),
				Description: "The private key that the certificate was issued for.",
			},
			"client_key_passphrase": {
				Type:        schema.TypeString,
				Optional:    true,
				DefaultFunc: schema.EnvDefaultFunc("KAFKA_CLIENT_KEY_PASSPHRASE", nil),
				Description: "The passphrase for the private key that the certificate was issued for.",
			},
			"sasl_username": &schema.Schema{
				Type:        schema.TypeString,
				Optional:    true,
				Required:    false,
				DefaultFunc: schema.EnvDefaultFunc("KAFKA_SASL_USERNAME", nil),
				Description: "Username for SASL authentication.",
			},
			"sasl_password": &schema.Schema{
				Type:        schema.TypeString,
				Optional:    true,
				Required:    false,
				DefaultFunc: schema.EnvDefaultFunc("KAFKA_SASL_PASSWORD", nil),
				Description: "Password for SASL authentication.",
			},
			"sasl_mechanism": &schema.Schema{
				Type:        schema.TypeString,
				Optional:    true,
				Required:    false,
				DefaultFunc: schema.EnvDefaultFunc("KAFKA_SASL_MECHANISM", "plain"),
				Description: "SASL mechanism, can be plain, scram-sha512, scram-sha256",
			},
			"skip_tls_verify": &schema.Schema{
				Type:        schema.TypeBool,
				Optional:    true,
				DefaultFunc: schema.EnvDefaultFunc("KAFKA_SKIP_VERIFY", "false"),
				Description: "Set this to true only if the target Kafka server is an insecure development instance.",
			},
			"tls_enabled": &schema.Schema{
				Type:        schema.TypeBool,
				Optional:    true,
				DefaultFunc: schema.EnvDefaultFunc("KAFKA_ENABLE_TLS", "true"),
				Description: "Enable communication with the Kafka Cluster over TLS.",
			},
			"timeout": {
				Type:        schema.TypeInt,
				Optional:    true,
				Default:     120,
				Description: "Timeout in seconds",
			},
		},

		ConfigureFunc: providerConfigure,
		ResourcesMap: map[string]*schema.Resource{
			"kafka_topic": kafkaTopicResource(),
			"kafka_acl":   kafkaACLResource(),
		},
	}
}

func envDefaultListFunc(k string, dv interface{}) schema.SchemaDefaultFunc {
	return func() (interface{}, error) {
		if v := os.Getenv(k); v != "" {
			list := strings.Split(v, ",")
			// Convert back to interface to prevent panic
			//https://github.com/hashicorp/terraform-plugin-sdk/issues/480
			res := []interface{}{}
			for _, s := range list {
				res = append(res, s)
				log.Printf("[INFO] envSet %s", s)
			}
			return res, nil
		}

		return dv, nil
	}
}

func providerConfigure(d *schema.ResourceData) (interface{}, error) {
	brokers := dTos("bootstrap_servers", d)
	log.Printf("[DEBUG] configuring provider with Brokers @ %v", brokers)

	saslMechanism := d.Get("sasl_mechanism").(string)
	switch saslMechanism {
	case "scram-sha512", "scram-sha256", "plain":
	default:
		return nil, fmt.Errorf("[ERROR] Invalid sasl mechanism \"%s\": can only be \"scram-sha256\", \"scram-sha512\" or \"plain\"", saslMechanism)
	}

	config := &Config{
		BootstrapServers:        brokers,
		CACert:                  d.Get("ca_cert").(string),
		ClientCert:              d.Get("client_cert").(string),
		ClientCertKey:           d.Get("client_key").(string),
		ClientCertKeyPassphrase: d.Get("client_key_passphrase").(string),
		SkipTLSVerify:           d.Get("skip_tls_verify").(bool),
		SASLUsername:            d.Get("sasl_username").(string),
		SASLPassword:            d.Get("sasl_password").(string),
		SASLMechanism:           saslMechanism,
		TLSEnabled:              d.Get("tls_enabled").(bool),
		Timeout:                 d.Get("timeout").(int),
	}

	if config.CACert == "" {
		config.CACert = d.Get("ca_cert_file").(string)
	}
	if config.ClientCert == "" {
		config.ClientCert = d.Get("client_cert_file").(string)
	}
	if config.ClientCertKey == "" {
		config.ClientCertKey = d.Get("client_key_file").(string)
	}

	log.Printf("[DEBUG] Config @ %v", config.copyWithMaskedSensitiveValues())

	return &LazyClient{
		Config: config,
	}, nil
}

func dTos(key string, d *schema.ResourceData) *[]string {
	var r *[]string

	if v, ok := d.GetOk(key); ok {
		if v == nil {
			return r
		}
		vI := v.([]interface{})
		b := make([]string, len(vI))

		for i, vv := range vI {
			if vv == nil {
				log.Printf("[DEBUG] %d %v was nil", i, vv)
				continue
			}
			b[i] = vv.(string)
		}
		r = &b
	}

	return r
}
