package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/streadway/amqp"
	"github.com/tkanos/gonfig"
	"log"
	"math"
	"net/http"
	"strings"
	"time"
	"ttnmapper-mysql-insert-raw/types"
)

var messageChannel = make(chan amqp.Delivery)

type Configuration struct {
	AmqpHost     string `env:"AMQP_HOST"`
	AmqpPort     string `env:"AMQP_PORT"`
	AmqpUser     string `env:"AMQP_USER"`
	AmqpPassword string `env:"AMQP_PASSWORD"`
	AmqpExchange string `env:"AMQP_EXHANGE"`
	AmqpQueue    string `env:"AMQP_QUEUE"`

	MysqlHost     string `env:"MYSQL_HOST"`
	MysqlPort     string `env:"MYSQL_PORT"`
	MysqlUser     string `env:"MYSQL_USERNAME"`
	MysqlPassword string `env:"MYSQL_PASSWORD"`
	MysqlDatabase string `env:"MYSQL_DATABASE"`

	PrometheusPort string `env:"PROMETHEUS_PORT"`
}

var myConfiguration = Configuration{
	AmqpHost:     "localhost",
	AmqpPort:     "5672",
	AmqpUser:     "user",
	AmqpPassword: "password",
	AmqpExchange: "new_packets",
	AmqpQueue:    "mysql_insert_raw",

	MysqlHost:     "localhost",
	MysqlPort:     "3306",
	MysqlUser:     "user",
	MysqlPassword: "password",
	MysqlDatabase: "database",

	PrometheusPort: "9090",
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

	err := gonfig.GetConf("conf.json", &myConfiguration)
	if err != nil {
		fmt.Println(err)
	}

	log.Printf("[Configuration]\n%s\n", prettyPrint(myConfiguration)) // output: [UserA, UserB]

	http.Handle("/metrics", promhttp.Handler())
	go func() {
		err := http.ListenAndServe(":"+myConfiguration.PrometheusPort, nil)
		if err != nil {
			log.Print(err.Error())
		}
	}()

	// Start thread to handle MySQL inserts
	go insertToMysql()

	// Start amqp listener on this thread - blocking function
	subscribeToRabbit()
}

func prettyPrint(i interface{}) string {
	s, _ := json.MarshalIndent(i, "", "\t")
	return string(s)
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
		myConfiguration.AmqpExchange, // name
		"fanout",                     // type
		true,                         // durable
		false,                        // auto-deleted
		false,                        // internal
		false,                        // no-wait
		nil,                          // arguments
	)
	failOnError(err, "Failed to declare an exchange")

	q, err := ch.QueueDeclare(
		myConfiguration.AmqpQueue, // name
		true,                      // durable
		false,                     // delete when usused
		false,                     // exclusive
		false,                     // no-wait
		nil,                       // arguments
	)
	failOnError(err, "Failed to declare a queue")

	err = ch.Qos(
		10,    // prefetch count
		0,     // prefetch size
		false, // global
	)
	failOnError(err, "Failed to set queue QoS")

	err = ch.QueueBind(
		q.Name,                       // queue name
		"",                           // routing key
		myConfiguration.AmqpExchange, // exchange
		false,
		nil)
	failOnError(err, "Failed to bind a queue")

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		false,  // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	forever := make(chan bool)

	go func() {
		for d := range msgs {
			log.Print(" [a] Packet received")
			messageChannel <- d
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

	// defer the close till after the main function has finished executing
	defer db.Close()

	stmtInsPackets, err := db.PrepareNamed("INSERT INTO packets " +
		"(time, nodeaddr, appeui, gwaddr, modulation, " +
		"datarate, snr, rssi, fcount, " +
		"freq, lat, lon, alt, accuracy," +
		"hdop, sats, provider, user_agent," +
		"user_id) " +
		"VALUES " +
		"(:time, :dev_id, :app_id, :gtw_id, :modulation, " +
		":datarate, :snr, :rssi, :fcount, " +
		":frequency, :latitude, :longitude, :altitude, :accuracy," +
		":hdop, :satellites, :accuracy_source, :user_agent," +
		":user_id)")
	if err != nil {
		panic(err.Error()) // proper error handling instead of panic in your app
	}
	defer stmtInsPackets.Close() // Close the statement when we leave main() / the program terminates

	stmtInsExperiments, err := db.PrepareNamed("INSERT INTO experiments " +
		"(time, nodeaddr, appeui, gwaddr, modulation, " +
		"datarate, snr, rssi, fcount, " +
		"freq, lat, lon, alt, accuracy," +
		"hdop, sats, provider, user_agent," +
		"user_id, name) " +
		"VALUES " +
		"(:time, :dev_id, :app_id, :gtw_id, :modulation, " +
		":datarate, :snr, :rssi, :fcount, " +
		":frequency, :latitude, :longitude, :altitude, :accuracy," +
		":hdop, :satellites, :accuracy_source, :user_agent," +
		":user_id, :experiment_name)")
	if err != nil {
		panic(err.Error()) // proper error handling instead of panic in your app
	}
	defer stmtInsExperiments.Close() // Close the statement when we leave main() / the program terminates

	stmtUpdateGatewayLastSeen, err := db.PrepareNamed(
		"UPDATE `gateways_aggregated` SET `last_heard`=:time WHERE gwaddr=:gtw_id AND last_heard<:time")
	if err != nil {
		panic(err.Error()) // proper error handling instead of panic in your app
	}
	defer stmtUpdateGatewayLastSeen.Close()

	for {
		d := <-messageChannel
		log.Printf(" [m] Processing packet")

		var message types.TtnMapperUplinkMessage
		if err := json.Unmarshal(d.Body, &message); err != nil {
			log.Print(" [a] " + err.Error())
			continue
		}

		shouldRetry := false

		// Only do the actually insert if the network is of type TTNv2.
		// TODO: What shall we do with TTNv3 messages?
		if message.NetworkType == types.NS_TTN_V2 {

			for _, gateway := range message.Gateways {
				gatewayStart := time.Now()
				entry := messageToEntry(message, gateway)

				if strings.HasPrefix(entry.GtwId, "eui-") {
					entry.GtwId = strings.ToUpper(entry.GtwId[4:])
				}

				var result sql.Result
				var err error
				if entry.Experiment {
					result, err = stmtInsExperiments.Exec(entry)
				} else {
					result, err = stmtInsPackets.Exec(entry)
				}

				if err != nil {
					// We only retry on connection errors
					if err.Error() == "invalid connection" {
						// Connection to mysql down, retry
						shouldRetry = true
					}
				} else {
					lastId, err := result.LastInsertId()
					if err != nil {
						log.Printf("  [m] Error in insert id")
						log.Print(err.Error())
					}

					rowsAffected, err := result.RowsAffected()
					if err != nil {
						log.Printf("  [m] Error in rows affected")
						log.Print(err.Error())
					}

					log.Printf("  [m] Inserted packet id=%d (affected %d rows)", lastId, rowsAffected)

					result, err = stmtUpdateGatewayLastSeen.Exec(entry)
					if err != nil {
						log.Print(err.Error())
					}
					rowsAffected, err = result.RowsAffected()
					if err != nil {
						log.Print(err.Error())
					}
					if rowsAffected > 0 {
						log.Printf("  [m] Updated gateway %s last seen to %s", entry.GtwId, entry.Time)
					}

					dbInserts.Inc()
				}

				gatewayElapsed := time.Since(gatewayStart)
				inserDuration.Observe(float64(gatewayElapsed.Nanoseconds()) / 1000.0 / 1000.0) //nanoseconds to milliseconds
			}
		}

		if shouldRetry {
			log.Println("  [m] Connection error. Retrying.")
			time.Sleep(time.Second) // sleep before nack to prevent a flood of messages
			d.Nack(false, true)
		} else {
			d.Ack(false)
		}
	}
}

func messageToEntry(message types.TtnMapperUplinkMessage, gateway types.TtnMapperGateway) types.MysqlRawPacket {
	var entry = types.MysqlRawPacket{}

	//if gateway.Time != types.BuildTime(0) {
	//	entry.Time = gateway.Time
	//} else {
	//	entry.Time = message.Metadata.Time
	//}
	seconds := message.Time / 1000000000
	nanos := message.Time % 1000000000
	entry.Time = time.Unix(seconds, nanos)

	entry.AppId = message.AppID
	entry.DevId = message.DevID

	if gateway.GatewayEui != "" {
		entry.GtwId = gateway.GatewayEui
	} else {
		entry.GtwId = gateway.GatewayId
	}

	entry.Modulation = message.Modulation
	entry.DataRate = fmt.Sprintf("SF%dBW%d", message.SpreadingFactor, message.Bandwidth/1000)
	entry.Bitrate = uint32(message.Bitrate)
	entry.CodingRate = message.CodingRate

	frequency := float64(message.Frequency)
	frequency = frequency / 1000
	frequency = math.Round(frequency) / 1000
	entry.Frequency = frequency

	entry.FrameCount = uint64(message.FCnt)

	entry.RSSI = gateway.Rssi
	entry.SNR = gateway.Snr

	entry.Latitude = message.Latitude
	entry.Longitude = message.Longitude
	entry.Altitude = message.Altitude

	entry.Accuracy = message.AccuracyMeters
	entry.Satellites = message.Satellites
	entry.AccuracySource = message.AccuracySource

	entry.UserAgent = message.UserAgent
	entry.UserId = message.UserId
	entry.Deleted = false

	entry.ExperimentName = message.Experiment
	if entry.ExperimentName == "" {
		entry.Experiment = false

		// HDOP for non experiment is 2.1
		hdop := math.Round(message.Hdop*10) / 10
		hdop = math.Min(hdop, 9.9)
		entry.Hdop = hdop
	} else {
		entry.Experiment = true

		// Hdop for experiment 3.1
		hdop := math.Round(message.Hdop*10) / 10
		hdop = math.Min(hdop, 99.9)
		entry.Hdop = hdop
	}

	return entry
}
