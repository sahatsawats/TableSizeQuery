package models

	type Configurations struct {
		Database	DatabaseConfigurations
		Logger		LoggerConfigurations
		Software	SoftwareConfigurations
	}

	type DatabaseConfigurations struct {
		DatabaseUser string
		DatabasePassword string
		HostName string
		Port int
		ServiceName string
		ExcludeOwner string
	}

	type LoggerConfigurations struct {
		LogFileName string
		
	}

	type SoftwareConfigurations struct {
		OutputFile string
		WorkerThreads int
	}

