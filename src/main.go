package main

import (
	_ "context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/sahatsawats/TableSizeQuery/src/models"
	concurrentlog "github.com/sahatsawats/concurrent-log"
	concurrentqueue "github.com/sahatsawats/concurrent-queue"
	_ "github.com/sijms/go-ora/v2"
	"gopkg.in/yaml.v2"
)

// TODO: specify service name by passing args. If passing args, ignore parameter in config files

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

// TODO: Delete the given single quotes and whitespace from configuration file (FINISHED) 24/01/2025
// readStringToList returns a slice of string without single quotes or white-space.
func readStringToList(text string) []string {
	// Replace single quotes
	deleteSinglequotes := strings.ReplaceAll(text, "'", "")
	// Split by comma delimiter
	splitString := strings.Split(deleteSinglequotes, ",")
	// for-loop trim the white-space
	for i := range splitString {
		splitString[i] = strings.TrimSpace(splitString[i])
	}

	return splitString
}

// gracefulExit used for delays the error exit for concurrentlog can flush all of logs in buffer to disk.
func gracefulExit(statusCode int) {
	time.Sleep(3 * time.Second)
	os.Exit(statusCode)
}

// flusingToDisk used for dequeue the results from query to output file.
func flushingToDisk(results *concurrentqueue.ConcurrentQueue[models.CountRows], outputfile *os.File) {
	for {
		if results.IsEmpty() {
			return
		}
		data := results.Dequeue()
		line := fmt.Sprintf("%s,%s,%d\n", data.Owner, data.TableName, data.Row)
		_, err := outputfile.WriteString(line)
		if err != nil {
			fmt.Printf("Failed to write line: %s with error log: %v", line, err)
		}
	}
}

// buildQueryStatement used for create a query statement to find a owner and tables name.
// A excludeOwner is a slice of string that use for filter out owner name.
func buildQueryStatement(excludeOwner []string) string {
	var sb strings.Builder
	sizeOfExcludeOwner := len(excludeOwner)
	sb.WriteString("SELECT owner, table_name FROM dba_tables WHERE owner NOT IN (")
	for i := 0; i < sizeOfExcludeOwner; i++ {
		if i+1 == sizeOfExcludeOwner {
			sb.WriteString(fmt.Sprintf("'%s')",excludeOwner[i]))
		} else {
			sb.WriteString(fmt.Sprintf("'%s',", excludeOwner[i]))
		}
	}
	return sb.String()
}

func main() {
	timeOut := flag.Int64("timeout", 0, "timeout for query")

	flag.Parse()
	
	programStartTime := time.Now()
	fmt.Println("Start reading configuration file...")
	config := readingConfigurationsFile()
	fmt.Println("Complete reading configuration file.")

	// Initialize logging
	fmt.Println("Starting logging thread...")
	logHandler, err := concurrentlog.NewLogger(config.Logger.LogFile_Name, 50)
	if err != nil {
		log.Fatalf("Failed to initialize log handler: %v", err)
		gracefulExit(1)
	}

	file, err := os.Create(config.Software.Output_File)
	if err != nil {
		logHandler.Log("ERROR", fmt.Sprintf("Failed to create output file: %v", err))
		gracefulExit(1)
	}

	defer file.Close()
	// Map database credentails with
	databaseCredentials := &models.DatabaseCredentials{
		DatabaseUser:     config.Database.Database_User,
		DatabasePassword: config.Database.Database_Password,
		ServiceName:      config.Database.Service_Name,
		HostName:         config.Database.Host_Name,
		Port:             config.Database.Port,
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
	db.SetMaxIdleConns(config.Software.Worker_Threads)
	db.SetConnMaxLifetime(0)

	// Query all tables from given owner
	excludeOwner := readStringToList(config.Database.Exclude_Owner)
	queryStatement := buildQueryStatement(excludeOwner)

	rows, err := db.Query(queryStatement)
	if err != nil {
		logHandler.Log("ERROR", fmt.Sprintf("Failed to execute query list all tables from dba_tables: %v", err))
		gracefulExit(1)
	}
	//TODO: Enqueue with owner name as well as table_name (FINISHED) 24/01/2025
	// Concurrent Queue, purpose is for multiple goroutine to dequeue the data.
	queue := concurrentqueue.New[models.QueueDataType]()
	enqueueOk := 0
	enqueueNok := 0
	for rows.Next() {
		var owner_name string
		var table_name string
		err := rows.Scan(&owner_name, &table_name)
		if err != nil {
			// If error occured, logging and skip to the next.
			logHandler.Log("ERROR", fmt.Sprintf("Failed to read results from query: %v", err))
			enqueueNok = enqueueNok + 1
			continue
		}
		enqueueDataType := models.QueueDataType{
			Owner: owner_name,
			TableName: table_name,
		}
		queue.Enqueue(enqueueDataType)
		enqueueOk = enqueueOk + 1
	}

	logHandler.Log("INFO", fmt.Sprintf("Complete enqueue table name with status {ok: %d, nok: %d}", enqueueOk, enqueueNok))
	rows.Close()

	logHandler.Log("INFO", "Starting query threads...")
	var wg sync.WaitGroup
	workerThreads := config.Software.Worker_Threads
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
				// TODO: Change from single table name to full-table name (with owner), change results enqueue to write into output file (FINISHED) 24/01/2025
				queueDataType := queue.Dequeue()
				// ! Include double-quotes between owner and table name (FINISHED) 24/01/2025
				fullNameTable := fmt.Sprintf("\"%s\".\"%s\"", queueDataType.Owner, queueDataType.TableName)
				var rowCount int64
				err := db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", fullNameTable)).Scan(&rowCount)
				if err != nil {
					logHandler.Log("ERROR", fmt.Sprintf("Failed to read results from %s: %v", fullNameTable, err))
				}

				resultQueue.Enqueue(models.CountRows{
					Owner: queueDataType.Owner,
					TableName: queueDataType.TableName,
					Row:       rowCount,
				})
			}
		}(i)
	}

	// Waiting goroutine to be finish
	wg.Wait()
	logHandler.Log("INFO", "Completed query threads.")

	// Flush the query from buffer to output file
	logHandler.Log("INFO", "Flushing data...")
	flushingToDisk(resultQueue, file)
	logHandler.Log("INFO", "Flushing process is complete.")

	// Close the proccesses
	elapsedTime := time.Since(programStartTime)
	logHandler.Log("INFO", fmt.Sprintf("Script successfully executed with elapsed time: %v", elapsedTime))

	time.Sleep(time.Second * 5)
	logHandler.Close()
}