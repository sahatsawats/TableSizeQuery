package main

import (
	_ "context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	_ "sync"
	"time"

	"github.com/sahatsawats/TableSizeQuery/src/models"
	concurrentlog "github.com/sahatsawats/concurrent-log"
	concurrentqueue "github.com/sahatsawats/concurrent-queue"
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
	owner := flag.String("owner", "", "Provide the schema name")
	flag.Parse()
	// Error handling when "--schema" is not specify
	if *owner == "" {
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
	// Open connection to oracle database based on given credentials with oracle driver
	db, err := sql.Open("oracle", connectionString)
	if err != nil {
		logHandler.Log("ERROR", fmt.Sprintf("Cannot create connection: %v", err))
		gracefulExit(1)
	}
	// Test connection due to lazy
	err = db.Ping()
	if err != nil {
		logHandler.Log("ERROR", fmt.Sprintf("Failed to open connection: %v", err))
		gracefulExit(1)
	}
	logHandler.Log("INFO", fmt.Sprintf("Successfully open connection to %v", databaseCredentials.ServiceName))

	// Query all tables from given owner
	rows, err := db.Query(fmt.Sprintf("SELECT table_name FROM dba_tables WHERE owner = '%d'", owner))
	if err != nil {
		logHandler.Log("ERROR", fmt.Sprintf("Failed to execute query list all tables from dba_tables: %v", err))
		gracefulExit(1)
	}

	// Concurrent Queue, purpose is for multiple goroutine to dequeue the data.
	queue := concurrentqueue.New[string]()
	enqueueOk := 0
	enqueueNok := 0
	for rows.Next() {
		var table_name string
		err := rows.Scan(&table_name)
		if err != nil {
			// If error occured, logging and skip to the next.
			logHandler.Log("ERROR", fmt.Sprintf("Failed to read results from query: %v", err))
			enqueueNok = enqueueNok + 1
			continue
		}
		queue.Enqueue(table_name)
		enqueueOk = enqueueOk + 1
	}

	logHandler.Log("INFO", fmt.Sprintf("Complete enqueue table name within owner='%v' with status {ok: %d, nok: %d}", owner, enqueueOk, enqueueNok))
	rows.Close()
	db.Close()

	var wg sync.WaitGroup
	workerThreads := config.Software.WorkerThreads
	// Create PrepareStatement to improve performance
	stmt, err := db.Prepare("SELECT COUNT(*) FROM ?")
	if err != nil {
		logHandler.Log("ERROR", fmt.Sprintf("Failed to create prepareStatement: %v", err))
	}
	defer stmt.Close()
	// resultQueue: use for stored query results
	resultQueue := concurrentqueue.New[models.CountRows]()

	for i := 0; i < workerThreads; i++ {
		wg.Add(1)
		go func(id int, preapreStatement *sql.Stmt) {
			defer wg.Done()
			db, err := sql.Open("oracle", connectionString)
			if err != nil {
				logHandler.Log("ERROR", fmt.Sprintf("Failed to create connection from worker %d: %v", id, err))
			}
			defer db.Close()

			err = db.Ping()
			if err != nil {
				logHandler.Log("ERROR", fmt.Sprintf("Failed to open connection from worker %d: %v", id, err))
			}

			for {
				if queue.IsEmpty() {
					return
				}

				tableName := queue.Dequeue()
				var rowCount int64 
				err := stmt.QueryRow(tableName).Scan(&rowCount)
				if err != nil {
					logHandler.Log("ERROR", fmt.Sprintf("Failed to read results from %s: %v", tableName, err))
				}

				resultQueue.Enqueue(models.CountRows{
					TableName: tableName,
					Row: rowCount,
				})
			}
		} (i, stmt)

		wg.Wait()
	}






	time.Sleep(time.Second * 5)
	logHandler.Close()
}