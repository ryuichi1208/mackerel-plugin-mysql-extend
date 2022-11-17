package mysql

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	mp "github.com/mackerelio/go-mackerel-plugin"
	"github.com/ziutek/mymysql/mysql"

	// MySQL Driver
	"github.com/ziutek/mymysql/native"
)

func (m *MySQLPlugin) defaultGraphdef() map[string]mp.Graphs {
	labelPrefix := strings.Title(strings.Replace(m.MetricKeyPrefix(), "mysql", "MySQL", -1))

	return map[string]mp.Graphs{
		"cmd_ext": {
			Label: labelPrefix + " Command Extend",
			Unit:  "float",
			Metrics: []mp.Metrics{
				{Name: "Com_admin_commands", Label: "admin_commands", Diff: true, Stacked: true},
				{Name: "Com_alter_db", Label: "alter_db", Diff: true, Stacked: true},
				{Name: "Com_alter_db_upgrade", Label: "db_upgrade", Diff: true, Stacked: true},
				{Name: "Com_rollback", Label: "rollback", Diff: true, Stacked: true},
				{Name: "Com_purge", Label: "purge", Diff: true, Stacked: true},
				{Name: "Com_kill", Label: "kill", Diff: true, Stacked: true},
				{Name: "Com_stmt_reprepare", Label: "stmt_reprepare", Diff: true, Stacked: true},
				{Name: "Com_alter_table", Label: "alter_table", Diff: true, Stacked: true},
				{Name: "Com_alter_tablespace", Label: "alter_tablespace", Diff: true, Stacked: true},
				{Name: "Com_load", Label: "load", Diff: true, Stacked: true},
				{Name: "Com_prepare_sql", Label: "prepare_sql", Diff: true, Stacked: true},
				{Name: "Com_show_status", Label: "show_status", Diff: true, Stacked: true},
			},
		},
		"connection": {
			Label: labelPrefix + " Connection",
			Unit:  "float",
			Metrics: []mp.Metrics{
				{Name: "Connection_errors_accept", Label: "Errors Accept", Diff: true, Stacked: true},
				{Name: "Connection_errors_internal", Label: "Errors Internal", Diff: true, Stacked: true},
				{Name: "Connection_errors_max_connections", Label: "Errors Max Connections", Diff: true, Stacked: true},
				{Name: "Connection_errors_peer_address", Label: "Errors Peer Address", Diff: true, Stacked: true},
				{Name: "Connection_errors_select", Label: "Errors Select", Diff: true, Stacked: true},
				{Name: "Connection_errors_tcpwrap", Label: "Errors tcpwrap", Diff: true, Stacked: true},
			},
		},
		"lock_extend": {
			Label: labelPrefix + " Lock Extend",
			Unit:  "integer",
			Metrics: []mp.Metrics{
				{Name: "Innodb_row_lock_time_avg", Label: "lock time avg", Diff: false, Stacked: false},
				{Name: "Innodb_row_lock_time_max", Label: "lock time max", Diff: false, Stacked: false},
			},
		},
		"uptime": {
			Label: labelPrefix + " Uptime",
			Unit:  "float",
			Metrics: []mp.Metrics{
				{Name: "Uptime", Label: "uptime", Diff: false, Stacked: false},
				{Name: "Uptime_since_flush_status", Label: "Uptime_since_flush_status", Diff: false, Stacked: false},
			},
		},
	}
}

// MySQLPlugin mackerel plugin for MySQL
type MySQLPlugin struct {
	Target         string
	Tempfile       string
	prefix         string
	Username       string
	Password       string
	DisableInnoDB  bool
	isUnixSocket   bool
	EnableExtended bool
	isAuroraReader bool
	Debug          bool
}

// MetricKeyPrefix returns the metrics key prefix
func (m *MySQLPlugin) MetricKeyPrefix() string {
	if m.prefix == "" {
		m.prefix = "mysql"
	}
	return m.prefix
}

func (m *MySQLPlugin) fetchShowStatus(db mysql.Conn, stat map[string]float64) error {
	rows, _, err := db.Query("show /*!50002 global */ status")
	if err != nil {
		log.Fatalln("FetchMetrics (Status): ", err)
		return err
	}

	for _, row := range rows {
		if len(row) > 1 {
			variableName := string(row[0].([]byte))
			if err != nil {
				log.Fatalln("FetchMetrics (Status Fetch): ", err)
				return err
			}
			v, err := atof(string(row[1].([]byte)))
			if err != nil {
				continue
			}
			stat[variableName] = v
		} else {
			log.Fatalln("FetchMetrics (Status): row length is too small: ", len(row))
		}
	}
	return nil
}

func (m *MySQLPlugin) calculateCapacity(stat map[string]float64) {
	stat["PercentageOfConnections"] = 100.0 * stat["Threads_connected"] / stat["max_connections"]
	if m.DisableInnoDB != true {
		stat["PercentageOfBufferPool"] = 100.0 * stat["database_pages"] / stat["pool_size"]
	}
}

// FetchMetrics interface for mackerelplugin
func (m *MySQLPlugin) FetchMetrics() (map[string]float64, error) {
	proto := "tcp"
	if m.isUnixSocket {
		proto = "unix"
	}
	db := mysql.New(proto, "", m.Target, m.Username, m.Password, "")
	switch c := db.(type) {
	case *native.Conn:
		c.Debug = m.Debug
	}
	err := db.Connect()
	if err != nil {
		log.Fatalln("FetchMetrics (DB Connect): ", err)
		return nil, err
	}
	defer db.Close()

	stat := make(map[string]float64)
	m.fetchShowStatus(db, stat)

	m.calculateCapacity(stat)

	explicitMetricNames := m.metricNames()
	statRet := make(map[string]float64)
	for key, value := range stat {
		if _, ok := explicitMetricNames[key]; !ok {
			continue
		}
		statRet[key] = value
	}

	return statRet, err
}

func (m *MySQLPlugin) metricNames() map[string]struct{} {
	a := make(map[string]struct{})
	for _, g := range m.GraphDefinition() {
		for _, metric := range g.Metrics {
			a[metric.Name] = struct{}{}
		}
	}
	return a
}

// GraphDefinition interface for mackerelplugin
func (m *MySQLPlugin) GraphDefinition() map[string]mp.Graphs {
	graphdef := m.defaultGraphdef()
	return graphdef
}

func atof(str string) (float64, error) {
	str = strings.Replace(str, ",", "", -1)
	str = strings.Replace(str, ";", "", -1)
	str = strings.Replace(str, "/s", "", -1)
	str = strings.Trim(str, " ")
	return strconv.ParseFloat(str, 64)
}

// Do the plugin
func Do() {
	optHost := flag.String("host", "localhost", "Hostname")
	optPort := flag.String("port", "3306", "Port")
	optSocket := flag.String("socket", "", "Path to unix socket")
	optUser := flag.String("username", "root", "Username")
	optPass := flag.String("password", os.Getenv("MYSQL_PASSWORD"), "Password")
	optTempfile := flag.String("tempfile", "", "Temp file name")
	optInnoDB := flag.Bool("disable_innodb", false, "Disable InnoDB metrics")
	optMetricKeyPrefix := flag.String("metric-key-prefix", "mysql-extend", "metric key prefix")
	optEnableExtended := flag.Bool("enable_extended", false, "Enable Extended metrics")
	optDebug := flag.Bool("debug", false, "Print debugging logs to stderr")
	flag.Parse()

	var mysql MySQLPlugin

	if *optSocket != "" {
		mysql.Target = *optSocket
		mysql.isUnixSocket = true
	} else {
		mysql.Target = fmt.Sprintf("%s:%s", *optHost, *optPort)
	}
	mysql.Username = *optUser
	mysql.Password = *optPass
	mysql.DisableInnoDB = *optInnoDB
	mysql.prefix = *optMetricKeyPrefix
	mysql.EnableExtended = *optEnableExtended
	mysql.Debug = *optDebug
	helper := mp.NewMackerelPlugin(&mysql)
	helper.Tempfile = *optTempfile
	helper.Run()
}
