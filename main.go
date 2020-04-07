package main

import (
	"encoding/json"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/shopspring/decimal"
	"github.com/streadway/amqp"
	"github.com/tkanos/gonfig"
	"log"
	"net/http"
	"strings"
	"time"
	"ttnmapper-mysql-insert-raw/types"
)

var messageChannel = make(chan types.TtnMapperUplinkMessage)

type Configuration struct {
	AmqpHost     string `env:"AMQP_HOST"`
	AmqpPort     string `env:"AMQP_PORT"`
	AmqpUser     string `env:"AMQP_USER"`
	AmqpPassword string `env:"AMQP_PASSWORD"`

	MysqlHost     string `env:"MYSQL_HOST"`
	MysqlPort     string `env:"MYSQL_PORT"`
	MysqlUser     string `env:"MYSQL_USERNAME"`
	MysqlPassword string `env:"MYSQL_PASSWORD"`
	MysqlDatabase string `env:"MYSQL_DATABASE"`

	PrometheusPort string `env:"PROMETHEUS_PORT"`

	UseOldDbSchema bool `env:"USE_OLD_DB_SCHEMA"`
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

	PrometheusPort: "9090",

	UseOldDbSchema: false,
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

	if myConfiguration.UseOldDbSchema {

		// Old database schema
		stmtInsPackets, err := db.PrepareNamed("INSERT INTO packets " +
			"(time, nodeaddr, appeui, gwaddr, modulation, " +
			"datarate, snr, rssi, " +
			"freq, lat, lon, alt, accuracy," +
			"hdop, sats, provider, user_agent," +
			"user_id) " +
			"VALUES " +
			"(:time, :dev_id, :app_id, :gtw_id, :modulation, " +
			":datarate, :snr, :rssi, " +
			":frequency, :latitude, :longitude, :altitude, :accuracy," +
			":hdop, :satellites, :accuracy_source, :user_agent," +
			":user_id)")
		if err != nil {
			panic(err.Error()) // proper error handling instead of panic in your app
		}
		defer stmtInsPackets.Close() // Close the statement when we leave main() / the program terminates

		stmtInsExperiments, err := db.PrepareNamed("INSERT INTO experiments " +
			"(time, nodeaddr, appeui, gwaddr, modulation, " +
			"datarate, snr, rssi, " +
			"freq, lat, lon, alt, accuracy," +
			"hdop, sats, provider, user_agent," +
			"user_id, name) " +
			"VALUES " +
			"(:time, :dev_id, :app_id, :gtw_id, :modulation, " +
			":datarate, :snr, :rssi, " +
			":frequency, :latitude, :longitude, :altitude, :accuracy," +
			":hdop, :satellites, :accuracy_source, :user_agent," +
			":user_id, :experiment_name)")
		if err != nil {
			panic(err.Error()) // proper error handling instead of panic in your app
		}
		defer stmtInsExperiments.Close() // Close the statement when we leave main() / the program terminates

		for {
			message := <-messageChannel
			log.Printf(" [m] Processing packet")

			for _, gateway := range message.Gateways {
				gatewayStart := time.Now()
				entry := messageToEntry(message, gateway)

				// Old schema needed the gateway id without the eui- prefix
				if strings.HasPrefix(entry.GtwId, "eui-") {
					entry.GtwId = strings.ToUpper(entry.GtwId[4:])
				}

				if entry.Experiment {

					result, err := stmtInsExperiments.Exec(entry)
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

						log.Printf("  [m] Inserted experiment packet id=%d (affected %d rows)", lastId, rowsAffected)
						dbInserts.Inc()
					}

				} else {

					result, err := stmtInsPackets.Exec(entry)
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

						log.Printf("  [m] Inserted raw packet id=%d (affected %d rows)", lastId, rowsAffected)
						dbInserts.Inc()
					}

				}

				gatewayElapsed := time.Since(gatewayStart)
				inserDuration.Observe(float64(gatewayElapsed.Nanoseconds()) / 1000.0 / 1000.0) //nanoseconds to milliseconds
			}

		}

	} else {

		// New database schema
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

			for _, gateway := range message.Gateways {
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

	frequency := decimal.NewFromInt(int64(message.Frequency))
	frequency = frequency.Div(decimal.NewFromInt(1000000))
	entry.Frequency = frequency
	entry.RSSI = gateway.Rssi
	entry.SNR = gateway.Snr

	entry.Latitude = message.Latitude
	entry.Longitude = message.Longitude
	entry.Altitude = message.Altitude
	entry.Hdop = message.Hdop
	entry.Accuracy = message.AccuracyMeters
	entry.Satellites = message.Satellites
	entry.AccuracySource = message.AccuracySource

	entry.UserAgent = message.UserAgent
	entry.UserId = message.UserId
	entry.Deleted = false

	entry.ExperimentName = message.Experiment
	if entry.ExperimentName == "" {
		entry.Experiment = false
	} else {
		entry.Experiment = true
	}

	return entry
}
