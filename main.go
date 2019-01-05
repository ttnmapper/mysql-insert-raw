package main

import (
	"encoding/json"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/streadway/amqp"
	"log"
	"net/http"
	"os"
	"time"
	"ttnmapper-mysql-insert-raw/types"
)

var messageChannel = make(chan types.TtnMapperUplinkMessage)

type Configuration struct {
	AmqpHost     string
	AmqpPort     string
	AmqpUser     string
	AmqpPassword string

	MysqlHost     string
	MysqlPort     string
	MysqlUser     string
	MysqlPassword string
	MysqlDatabase string

	PromethuesPort string
}

var myConfiguration = Configuration{
	AmqpHost:     "localhost",
	AmqpPort:     "5672",
	AmqpUser:     "user",
	AmqpPassword: "password",

	MysqlHost:     "localhost",
	MysqlPort:     "3306",
	MysqlUser:     "user",
	MysqlPassword: "password",
	MysqlDatabase: "database",

	PromethuesPort: "2112",
}

var (
	dbInserts = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ttnmapper_mysql_inserts_raw_count",
		Help: "The total number of packets inserted into the raw table",
	})

	inserDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "ttnmapper_mysql_inserts_raw_duration",
		Help:    "How long the processing and insert of a packet takes",
		Buckets: []float64{0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1, 1.5, 2, 5, 10, 100, 1000, 10000},
	})
)

func main() {

	file, err := os.Open("conf.json")
	if err != nil {
		log.Print(err.Error())
	}
	decoder := json.NewDecoder(file)
	err = decoder.Decode(&myConfiguration)
	if err != nil {
		log.Print(err.Error())
	}
	err = file.Close()
	if err != nil {
		log.Print(err.Error())
	}
	log.Printf("Using configuration: %+v", myConfiguration) // output: [UserA, UserB]

	http.Handle("/metrics", promhttp.Handler())
	go func() {
		err := http.ListenAndServe("127.0.0.1:"+myConfiguration.PromethuesPort, nil)
		if err != nil {
			log.Print(err.Error())
		}
	}()

	// Start hread to handle MySQL inserts
	go insertToMysql()

	// Start amqp listener on this thread - blocking function
	subscribeToRabbit()
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func subscribeToRabbit() {
	conn, err := amqp.Dial("amqp://" + myConfiguration.AmqpUser + ":" + myConfiguration.AmqpPassword + "@" + myConfiguration.AmqpHost + ":" + myConfiguration.AmqpPort + "/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	err = ch.ExchangeDeclare(
		"new_packets", // name
		"fanout",      // type
		true,          // durable
		false,         // auto-deleted
		false,         // internal
		false,         // no-wait
		nil,           // arguments
	)
	failOnError(err, "Failed to declare an exchange")

	q, err := ch.QueueDeclare(
		"mysql_insert_raw", // name
		false,              // durable
		false,              // delete when usused
		false,              // exclusive
		false,              // no-wait
		nil,                // arguments
	)
	failOnError(err, "Failed to declare a queue")

	err = ch.QueueBind(
		q.Name,        // queue name
		"",            // routing key
		"new_packets", // exchange
		false,
		nil)
	failOnError(err, "Failed to bind a queue")

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	forever := make(chan bool)

	go func() {
		for d := range msgs {
			//log.Printf(" [a] %s", d.Body)
			var packet types.TtnMapperUplinkMessage
			if err := json.Unmarshal(d.Body, &packet); err != nil {
				log.Print(" [a] " + err.Error())
				continue
			}
			log.Print(" [a] Packet received")
			messageChannel <- packet
		}
	}()

	log.Printf(" [a] Waiting for packets. To exit press CTRL+C")
	<-forever

}

func insertToMysql() {
	// Open up our database connection.
	db, err := sqlx.Open("mysql", myConfiguration.MysqlUser+":"+myConfiguration.MysqlPassword+"@tcp("+myConfiguration.MysqlHost+":"+myConfiguration.MysqlPort+")/"+myConfiguration.MysqlDatabase)
	if err != nil {
		panic(err.Error())
	}

	// Open doesn't open a connection. Validate DSN data:
	err = db.Ping()
	if err != nil {
		panic(err.Error()) // proper error handling instead of panic in your app
	}

	// defer the close till after the main function has finished
	// executing
	defer db.Close()

	// Prepare statement for inserting data
	stmtIns, err := db.PrepareNamed("INSERT INTO packets " +
		"(time, dev_id, app_id, gtw_id, modulation, " +
		"datarate, bitrate, coding_rate, snr, rssi, " +
		"frequency, latitude, longitude, altitude, accuracy," +
		"hdop, satellites, accuracy_source, user_agent," +
		"user_id, deleted, experiment, experiment_name) " +
		"VALUES " +
		"(:time, :dev_id, :app_id, :gtw_id, :modulation, " +
		":datarate, :bitrate, :coding_rate, :snr, :rssi, " +
		":frequency, :latitude, :longitude, :altitude, :accuracy," +
		":hdop, :satellites, :accuracy_source, :user_agent," +
		":user_id, :deleted, :experiment, :experiment_name)")
	if err != nil {
		panic(err.Error()) // proper error handling instead of panic in your app
	}
	defer stmtIns.Close() // Close the statement when we leave main() / the program terminates

	for {
		message := <-messageChannel
		log.Printf(" [m] Processing packet")

		for _, gateway := range message.Metadata.Gateways {
			gatewayStart := time.Now()
			entry := messageToEntry(message, gateway)
			result, err := stmtIns.Exec(entry)
			if err != nil {
				log.Print(err.Error())
			} else {
				lastId, err := result.LastInsertId()
				if err != nil {
					log.Print(err.Error())
				}

				rowsAffected, err := result.RowsAffected()
				if err != nil {
					log.Print(err.Error())
				}

				log.Printf("  [m] Inserted entry id=%d (affected %d rows)", lastId, rowsAffected)
				dbInserts.Inc()
			}
			gatewayElapsed := time.Since(gatewayStart)
			inserDuration.Observe(float64(gatewayElapsed.Nanoseconds()) / 1000.0 / 1000.0) //nanoseconds to milliseconds
		}

	}
}

func messageToEntry(message types.TtnMapperUplinkMessage, gateway types.GatewayMetadata) types.MysqlEntry {
	var entry = types.MysqlEntry{}

	//if gateway.Time != types.BuildTime(0) {
	//	entry.Time = gateway.Time
	//} else {
	//	entry.Time = message.Metadata.Time
	//}
	entry.Time = message.Metadata.Time.GetTime() // Do not trust gateway time - always use server time

	entry.AppId = message.AppID
	entry.DevId = message.DevID
	entry.GtwId = gateway.GtwID

	entry.Modulation = message.Metadata.Modulation
	entry.DataRate = message.Metadata.DataRate
	entry.Bitrate = message.Metadata.Bitrate
	entry.CodingRate = message.Metadata.CodingRate

	entry.Frequency = message.Metadata.Frequency
	entry.RSSI = gateway.RSSI
	entry.SNR = gateway.SNR

	entry.Latitude = message.TtnMLatitude
	entry.Longitude = message.TtnMLongitude
	entry.Altitude = message.TtnMAltitude
	entry.Hdop = message.TtnMHdop
	entry.Accuracy = message.TtnMAccuracy
	entry.Satellites = message.TtnMSatellites
	entry.AccuracySource = message.TtnMProvider

	entry.UserAgent = message.TtnMUserAgent
	entry.UserId = message.TtnMUserId
	entry.Deleted = false

	entry.ExperimentName = message.TtnMExperiment
	if entry.ExperimentName == "" {
		entry.Experiment = false
	} else {
		entry.Experiment = true
	}

	return entry
}
