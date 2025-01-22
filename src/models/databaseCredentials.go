package models

import (
	go_ora "github.com/sijms/go-ora/v2"
)

type DatabaseCredentials struct {
	DatabaseUser string
	DatabasePassword string
	ServiceName string
	HostName string
	Port int
}

func (d DatabaseCredentials) GetConnectionString() string {
	
	return go_ora.BuildUrl(d.HostName, d.Port, d.ServiceName, d.DatabaseUser, d.DatabasePassword, nil)
}