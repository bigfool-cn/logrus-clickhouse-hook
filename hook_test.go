package hook

import (
	"io/ioutil"
	"testing"

	"github.com/sirupsen/logrus"
)

const (
	clickHouseHost = "localhost"
)

func TestHook(t *testing.T) {

	log := logrus.New()
	log.Out = ioutil.Discard

	clickhouse := makeClickHouseConfig()

	hook, err := NewHook(clickhouse)
	if err != nil {
		t.Errorf("Error when initialization hook: %s", err)
	}

	if err == nil {
		log.AddHook(hook)
	}

	for i := 0; i < 1; i++ {
		log.WithFields(logrus.Fields{
			"origin": "test",
		}).Info("Sync message for clickhouse")
	}

}

func makeClickHouseConfig() *ClickHouse {
	clickhouse := &ClickHouse{
		Db:    "logs",
		Table: "log",
		Host:  clickHouseHost,
		Port:  "8123",
		Columns: []string{
			"origin",
			"level",
			"msg",
		},
		Credentials: struct {
			User     string
			Password string
		}{
			User:     "root",
			Password: "123456",
		},
	}
	return clickhouse
}
