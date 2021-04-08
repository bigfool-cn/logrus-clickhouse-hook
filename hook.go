package hook

import (
	"net/url"
	"os"

	"github.com/mintance/go-clickhouse"
	"github.com/sirupsen/logrus"
)

var log = logrus.New()

func init() {
	log.Formatter = &logrus.TextFormatter{
		DisableTimestamp: true,
		DisableSorting:   true,
		QuoteEmptyFields: true,
	}
	log.SetLevel(logrus.ErrorLevel)
	log.Out = os.Stderr
}

type ClickHouse struct {
	Db          string
	Table       string
	Host        string
	Port        string
	Columns     []string
	Credentials struct {
		User     string
		Password string
	}
}

type Hook struct {
	ClickHouse *ClickHouse
	connection *clickhouse.Conn
	levels     []logrus.Level
}

func (hook *Hook) Save(field map[string]interface{}) error {
	rows := buildRows(hook.ClickHouse.Columns, []map[string]interface{}{field})
	err := persist(hook.ClickHouse, hook.connection, rows)

	return err
}

func persist(config *ClickHouse, connection *clickhouse.Conn, rows clickhouse.Rows) error {
	if rows == nil || len(rows) == 0 {
		return nil
	}

	query, err := clickhouse.BuildMultiInsert(
		config.Db+"."+config.Table,
		config.Columns,
		rows,
	)

	if err != nil {
		return err
	}

	log.Debug("Exec query")

	return query.Exec(connection)
}

func buildRows(columns []string, fields []map[string]interface{}) (rows clickhouse.Rows) {
	for _, field := range fields {
		row := clickhouse.Row{}

		for _, column := range columns {
			if field[column] == nil {
				log.Error("Invalid log item")
				break
			}

			row = append(row, field[column])
		}

		rows = append(rows, row)
	}

	return
}

func getStorage(config *ClickHouse) (*clickhouse.Conn, error) {

	httpTransport := clickhouse.NewHttpTransport()
	conn := clickhouse.NewConn(config.Host+":"+config.Port, httpTransport)

	params := url.Values{}
	params.Add("user", config.Credentials.User)
	params.Add("password", config.Credentials.Password)
	conn.SetParams(params)

	if err := conn.Ping(); err != nil {
		return nil, err
	}

	return conn, nil
}

func NewHook(clickHouse *ClickHouse) (*Hook, error) {
	storage, err := getStorage(clickHouse)

	if err != nil {
		return nil, err
	}

	hook := &Hook{
		ClickHouse: clickHouse,
		connection: storage,
		levels:     nil,
	}

	return hook, nil
}

func (hook *Hook) Fire(entry *logrus.Entry) error {
	fields := entry.Data
	for _, rawColumn := range hook.ClickHouse.Columns {
		if rawColumn == "level" {
			fields[rawColumn] = entry.Level.String()
		}
		if rawColumn == "msg" {
			fields[rawColumn] = entry.Message
		}
		if rawColumn == "time" {
			fields[rawColumn] = entry.Time.String()
		}
	}
	return hook.Save(fields)
}

func (hook *Hook) SetLevels(lvs []logrus.Level) {
	hook.levels = lvs
}

func (hook *Hook) Levels() []logrus.Level {

	if hook.levels == nil {
		return []logrus.Level{
			logrus.PanicLevel,
			logrus.FatalLevel,
			logrus.ErrorLevel,
			logrus.WarnLevel,
			logrus.InfoLevel,
			logrus.DebugLevel,
		}
	}

	return hook.levels
}
