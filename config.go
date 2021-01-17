package psql

import (
	"fmt"
	"strings"
)

type Config struct {
	DbName         string
	Schema         string
	User           string
	Password       string
	Host           string
	Port           string
	ConnectTimeout string
	SSLMode        string
}

func (c *Config) connString() string {
	var options []string
	if c.Schema != "" {
		options = append(options, fmt.Sprintf("--search_path=%v", c.Schema))
	}
	optionsStr := fmt.Sprintf("options='%v'", strings.Join(options, " "))

	var connParams []string
	if c.DbName != "" {
		connParams = append(connParams, fmt.Sprintf("dbname='%v'", c.DbName))
	}
	if c.User != "" {
		connParams = append(connParams, fmt.Sprintf("user='%v'", c.User))
	}
	if c.Password != "" {
		connParams = append(connParams, fmt.Sprintf("password='%v'", c.Password))
	}
	if c.Host != "" {
		connParams = append(connParams, fmt.Sprintf("host=%v", c.Host))
	}
	if c.Port != "" {
		connParams = append(connParams, fmt.Sprintf("port=%v", c.Port))
	}
	if c.ConnectTimeout != "" {
		connParams = append(connParams, fmt.Sprintf("connect_timeout=%v", c.ConnectTimeout))
	}
	if c.SSLMode != "" {
		connParams = append(connParams, fmt.Sprintf("sslmode=%v", c.SSLMode))
	}

	connParams = append(connParams, optionsStr)
	return strings.Join(connParams, " ")
}
