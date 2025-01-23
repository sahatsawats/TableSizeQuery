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

func flushingToDisk(results *concurrentqueue.ConcurrentQueue[models.CountRows], outputfile *os.File) {
	for {
		if results.IsEmpty() {
			return
		}
		data := results.Dequeue()
		line := fmt.Sprintf("%s,%d\n", data.TableName, data.Row)
		_, err := outputfile.WriteString(line)
		if err != nil {
			fmt.Printf("Failed to write line: %s with error log: %v", line, err)
		}
	}
}

func main() {
	// Parse option "--schema" when execute the binary
	owner := flag.String("owner", "", "Provide the schema name")
	timeOut := flag.Int64("timeout", 180, "timeout for query")
	flag.Parse()
	// Error handling when "--schema" is not specify
	if *owner == "" {
		fmt.Printf("No schema provided. Use --owner to specify a file. \n")
		os.Exit(1)
	}

	// programStartTime := time.Now()
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

	file, err := os.Create(config.Software.OutputFile)
	if err != nil {
		logHandler.Log("ERROR", fmt.Sprintf("Failed to create output file: %v", err))
		gracefulExit(1)
	}

	defer file.Close()
	// Map database credentails with 
	databaseCredentials := &models.DatabaseCredentials{
		DatabaseUser: config.Database.DatabaseUser,
		DatabasePassword: config.Database.DatabasePassword,
		ServiceName: config.Database.ServiceName,
		HostName: config.Database.HostName,
		Port: config.Database.Port,
	}
	
	connectionString := databaseCredentials.GetConnectionString(*timeOut)
	logHandler.Log("INFO", fmt.Sprintf("Establish connection to %v", databaseCredentials.ServiceName))
	// Open connection to oracle database based on given credentials with oracle driver
	db, err := sql.Open("oracle", connectionString)
	if err != nil {
		logHandler.Log("ERROR", fmt.Sprintf("Cannot create connection: %v", err))
		gracefulExit(1)
	}
	defer db.Close()
	// Test connection due to lazy
	err = db.Ping()
	if err != nil {
		logHandler.Log("ERROR", fmt.Sprintf("Failed to open connection: %v", err))
		gracefulExit(1)
	}
	logHandler.Log("INFO", fmt.Sprintf("Successfully open connection to %v", databaseCredentials.ServiceName))

	// Set connection pool settings
	db.SetMaxOpenConns(0)
	db.SetMaxIdleConns(config.Software.WorkerThreads)
	
	// Query all tables from given owner
	rows, err := db.Query(fmt.Sprintf("SELECT table_name FROM dba_tables WHERE owner = '%s'", *owner))
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

	logHandler.Log("INFO", fmt.Sprintf("Complete enqueue table name within owner='%v' with status {ok: %d, nok: %d}", *owner, enqueueOk, enqueueNok))
	rows.Close()

	logHandler.Log("INFO", "Starting query threads...")
	var wg sync.WaitGroup
	workerThreads := config.Software.WorkerThreads
	// Create PrepareStatement to improve performance

	// resultQueue: use for stored query results
	resultQueue := concurrentqueue.New[models.CountRows]()

	for i := 0; i < workerThreads; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for {
				if queue.IsEmpty() {
					return
				}

				tableName := queue.Dequeue()

				var rowCount int64 
				err := db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)).Scan(&rowCount)
				if err != nil {
					logHandler.Log("ERROR", fmt.Sprintf("Failed to read results from %s: %v", tableName, err))
				}

				resultQueue.Enqueue(models.CountRows{
					TableName: tableName,
					Row: rowCount,
				})
			}
		} (i)
	}

	wg.Wait()
	logHandler.Log("INFO", "Completed query threads.")

	logHandler.Log("INFO", "Flushing data...")
	flushingToDisk(resultQueue, file)
	logHandler.Log("INFO", "Flushing process is complete.")

	time.Sleep(time.Second * 5)
	logHandler.Close()
}