package types

import (
	"time"
)

type MysqlRawPacket struct {
	Id   int       `db:"id"`
	Time time.Time `db:"time"`

	DevId string `db:"dev_id"`
	AppId string `db:"app_id"`
	GtwId string `db:"gtw_id"`

	Modulation string `db:"modulation"`
	DataRate   string `db:"datarate"`
	Bitrate    uint32 `db:"bitrate"`
	CodingRate string `db:"coding_rate"`

	SNR       float32 `db:"snr"`
	RSSI      float32 `db:"rssi"`
	Frequency float64 `db:"frequency"`

	Latitude       float64 `db:"latitude"`
	Longitude      float64 `db:"longitude"`
	Altitude       float64 `db:"altitude"`
	Accuracy       float64 `db:"accuracy"`
	Hdop           float64 `db:"hdop"`
	Satellites     int32   `db:"satellites"`
	AccuracySource string  `db:"accuracy_source"`

	UserAgent string `db:"user_agent"`
	UserId    string `db:"user_id"`
	Deleted   bool   `db:"deleted"`

	Experiment     bool   `db:"experiment"`
	ExperimentName string `db:"experiment_name"`
}
