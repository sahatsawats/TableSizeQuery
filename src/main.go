package main

import (
	_ "context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	_ "sync"
	"time"
	"github.com/sahatsawats/TableSizeQuery/src/models"
	concurrentlog "github.com/sahatsawats/concurrent-log"

	//concurrentqueue "github.com/sahatsawats/concurrent-queue"
	_ "github.com/sijms/go-ora/v2"
	"gopkg.in/yaml.v2"
)

// Reading configuration file
func readingConfigurationsFile() *models.Configurations {
	// get the current execute directory
	baseDir, err := os.Executable()
	if err != nil {
		log.Fatal(err)
	}

	// Join path to config file
	configFile := filepath.Join(filepath.Dir(baseDir), "conf", "config.yaml")
	// Read file in bytes for mapping yaml to structure with yaml package
	readConf, err := os.ReadFile(configFile)
	if err != nil {
		log.Fatalf("Failed to read configurations file: %v", err)
	}

	// Map variable to configuration function
	var conf models.Configurations
	// Map yaml file to config structure
	err = yaml.Unmarshal(readConf, &conf)
	if err != nil {
		log.Fatalf("Failed to unmarshal config file: %v", err)
	}

	return &conf
}

func gracefulExit(statusCode int) {
	time.Sleep(3 * time.Second)
	os.Exit(statusCode)
}

func main() {
	// Parse option "--schema" when execute the binary
	schema := flag.String("schema", "", "Provide the schema name")
	flag.Parse()
	// Error handling when "--schema" is not specify
	if *schema == "" {
		fmt.Printf("No schema provided. Use --schema to specify a file. \n")
		os.Exit(1)
	}

	//programStartTime := time.Now()
	fmt.Println("Start reading configuration file...")
	config := readingConfigurationsFile()
	fmt.Println("Complete reading configuration file.")

	// Initialize logging
	fmt.Println("Starting logging thread...")
	logHandler, err := concurrentlog.NewLogger(config.Logger.LogFileName, 50)
	if err != nil {
		log.Fatalf("Failed to initialize log handler: %v", err)
		gracefulExit(1)
	}

	// Map database credentails with 
	databaseCredentials := &models.DatabaseCredentials{
		DatabaseUser: config.Database.DatabaseUser,
		DatabasePassword: config.Database.DatabasePassword,
		ServiceName: config.Database.ServiceName,
		HostName: config.Database.HostName,
		Port: config.Database.Port,
	}
	
	connectionString := databaseCredentials.GetConnectionString()
	logHandler.Log("INFO", fmt.Sprintf("Establish connection to %v", databaseCredentials.ServiceName))
	// Test connection
	db, err := sql.Open("oracle", connectionString)
	if err != nil {
		logHandler.Log("ERROR", fmt.Sprintf("Cannot create connection: %v", err))
		gracefulExit(1)
	}

	err = db.Ping()
	if err != nil {
		logHandler.Log("ERROR", fmt.Sprintf("Failed to open connection: %v", err))
		gracefulExit(1)
	}
	
	logHandler.Log("INFO", fmt.Sprintf("Successfully open connection to %v", databaseCredentials.ServiceName))

	time.Sleep(time.Second * 5)
	logHandler.Close()
}