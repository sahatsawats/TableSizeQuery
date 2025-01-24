package models

	type Configurations struct {
		Database	DatabaseConfigurations
		Logger		LoggerConfigurations
		Software	SoftwareConfigurations
	}

	type DatabaseConfigurations struct {
		Database_User string
		Database_Password string
		Host_Name string
		Port int
		Service_Name string
		Exclude_Owner string
	}

	type LoggerConfigurations struct {
		LogFile_Name string
		
	}

	type SoftwareConfigurations struct {
		Output_File string
		Worker_Threads int
	}

