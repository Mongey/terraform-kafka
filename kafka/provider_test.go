package kafka

import (
	"log"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/hashicorp/terraform-plugin-sdk/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/terraform"
)

var testProvider *schema.Provider
var testProviders map[string]terraform.ResourceProvider

func Test_EnvDefaultListFunc(t *testing.T) {
	testCases := []struct {
		env         string
		expected    []string
		errExpected bool
	}{
		{"localhost:8080,b:3333,v:1010", []string{"localhost:8080", "b:3333", "v:1010"}, false},
		{"localhost:8181", []string{"localhost:8181"}, false},
	}

	for _, a := range testCases {
		os.Setenv("KAFKA_BOOTSTRAP_SERVERS", a.env)

		f, err := envDefaultListFunc("KAFKA_BOOTSTRAP_SERVERS", nil)()
		if err != nil && !a.errExpected {
			t.Fatalf("err: %s", err)
		}

		res := []string{}
		for _, v := range f.([]interface{}) {
			res = append(res, v.(string))
		}

		if reflect.DeepEqual(res, a.expected) != true {
			t.Fatalf("got: %s, not: %s", res, a.expected)
		}
	}
}

func TestProvider(t *testing.T) {
	if err := Provider().(*schema.Provider).InternalValidate(); err != nil {
		t.Fatalf("err: %s", err)
	}
}

func testAccPreCheck(t *testing.T) {
	meta := testProvider.Meta()
	if meta == nil {
		t.Fatal("Could not construct client")
	}
	client := meta.(*LazyClient)
	if client == nil {
		t.Fatal("No client")
	}
}

func accProvider() map[string]terraform.ResourceProvider {
	log.Println("[INFO] Setting up override for a provider")
	provider := Provider().(*schema.Provider)
	log.Println("[INFO] env " + os.Getenv("KAFKA_BOOTSTRAP_SERVERS"))
	bs := strings.Split(os.Getenv("KAFKA_BOOTSTRAP_SERVERS"), ",")
	if len(bs) == 0 {
		bs = []string{"localhost:9092"}
	}

	bootstrapServers := []interface{}{}

	for _, v := range bs {
		if v != "" {
			bootstrapServers = append(bootstrapServers, v)
		}
	}

	raw := map[string]interface{}{
		"bootstrap_servers": bootstrapServers,
	}
	err := provider.Configure(terraform.NewResourceConfigRaw(raw))
	if err != nil {
		log.Printf("[ERROR] Could not configure provider %v", err)
	}

	testProvider = provider

	return map[string]terraform.ResourceProvider{
		"kafka": provider,
	}
}
