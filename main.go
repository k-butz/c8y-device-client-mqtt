package main

import (
	"encoding/csv"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/joho/godotenv"
	"github.com/tidwall/sjson"
)

var logger = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
	Level:     slog.LevelInfo,
	AddSource: true,
}))

var connectHandler mqtt.OnConnectHandler = func(client mqtt.Client) {
	logger.Info("Connected to MQTT Broker!")
}

var connectLostHandler mqtt.ConnectionLostHandler = func(client mqtt.Client, err error) {
	logger.Error("Connection lost", slog.Any("error", err))
}

func main() {
	godotenv.Load()

	const brokerURI = "mqtts://mqtt.eu-latest.cumulocity.com:8883"
	const deviceName = "showcase-device-01"
	const deviceSerial = "kobu-sn-7123"

	// init mqtt client and connect to Cumulocity
	opts := mqtt.NewClientOptions()
	opts.AddBroker(brokerURI)
	opts.SetClientID(deviceSerial)
	opts.SetUsername(os.Getenv("USERNAME"))
	opts.SetPassword(os.Getenv("PASSWORD"))
	opts.OnConnect = connectHandler
	opts.OnConnectionLost = connectLostHandler
	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		slog.Error("Failed to connect", "err", token.Error())
		os.Exit(1)
	}

	// Init device in Cloud - this message will create the Device if not existing yet
	publishSmartRestMessage(client, "100,"+deviceName+",yourDeviceType")
	time.Sleep(2 * time.Second)

	// Now tell the platform about the capabilities of your Device (required keywords for each capability are in "fragment library")
	publishSmartRestMessage(client, "114,c8y_Firmware,c8y_Restart,c8y_SoftwareList,c8y_SoftwareUpdate,c8y_LogfileRequest,c8y_RemoteAccessConnect,c8y_DeviceProfile")

	// Now set some device properties to give Users info about the Devce...
	setDeviceProperties(client, deviceName, deviceSerial)

	// Send measurements, events, alarms periodically in an endless loop
	// the "go " prefix is specific to Go, it runs this code in background
	go generateMeasurementsEventsAlarms(client, 5)

	// Ok now let's take care of listening to Cloud Operations, this is done by subscribing to "s/ds" topic
	token := client.Subscribe("s/ds", byte(1), handleReceivedMessage)
	if token.Wait() && token.Error() != nil {
		fmt.Printf("Error subscribing to topic %s: %v\n", "s/ds", token.Error())
		return
	}
	slog.Info("Subscribed to Operations topic (s/ds)")

	// this is specific to Go, used to keep main routine alive
	select {}
}

func setDeviceProperties(client mqtt.Client, deviceName string, deviceSerial string) {
	// template links: https://cumulocity.com/docs/smartrest/mqtt-static-templates/#inventory-templates

	// let platform know which firmware is installed (name, version, url)
	publishSmartRestMessage(client, "115,firmwareName,firmwareVersion,firmwareUrl")
	// let platform know which software is installed (triplets of software name/version/url)
	publishSmartRestMessage(client, "116,software1,1.0.1,url1,software2,1.0.2,url2,software3,1.0.3")
	// let platform know about hardware/OS in use (serial, model, version)
	publishSmartRestMessage(client, "110,"+deviceName+",myHardwareModel,1.2.3")
	// let platform know current latitude/longitude/altitude of the device
	publishSmartRestMessage(client, "112,50.323423,6.423423")
	// let platform know which logfile type can be retrieved from remote
	publishSmartRestMessage(client, "118,dpkg,container,logread")
	// let platform know about currently installed agent (name, version, url, maintainer)
	publishSmartRestMessage(client, "122,your-device-agent,0.1,https://cumulocity.com,\"Korbinian Butz\"")
	// let platform know about the interval the device is expected to send data
	publishSmartRestMessage(client, "117,60")

	// FYI in this example we've sent multiple, individual MQTT messages to the cloud
	// One could also concatenate these message, separate them via "\n" and send in one message to Cloud

	// Lastly, set a Property that is specific to customer and not covered by the static template and fragment library
	// You can update the object with any valid JSON, it will persist it onto the object and can be used by UIs and Applications right away
	publishJsonViaMqttMessage(client, "inventory/managedObjects/update/"+deviceSerial, `{"yourCustomFragment":{"a":"abc", "b":123, "c":[1,2,3]}}`)
}

func generateMeasurementsEventsAlarms(client mqtt.Client, sleepTimeSecs int) {
	for {
		// build a string that will submit measurements/events/alarms to cloud in one message
		// used templates:
		// - measurements (200): https://cumulocity.com/docs/smartrest/mqtt-static-templates/#200
		// - measurements (201): https://cumulocity.com/docs/smartrest/mqtt-static-templates/#201
		// - events (400): https://cumulocity.com/docs/smartrest/mqtt-static-templates/#400
		// - alarms (301): https://cumulocity.com/docs/smartrest/mqtt-static-templates/#301
		msg := `
200,temperature,T,15
200,pressure,p,15
200,yourMeasurementCategory,yourMeasurementName,16
201,yourMeaType,,c8y_SinglePhaseEnergyMeasurement,A1,1234,kWh,c8y_SinglePhaseEnergyMeasurement,A2,2345,kWh
400,yourEventType,"Your Event description"
301,yourAlarmType,"here is your alarm text"
`
		// submit this 6-line CSV to the Cloud, platform will create 5 measurements + 1 event + 1 alarm on your Device Twin
		publishSmartRestMessage(client, msg)

		// similar to Device Properties, let's now create additional Event with custom fragments via the "json-via-mqtt" API
		json := "{}"
		json, _ = sjson.Set(json, "time", time.Now().UTC().Format("2006-01-02T15:04:05.000Z"))
		json, _ = sjson.Set(json, "text", "Your new Event")
		json, _ = sjson.Set(json, "type", "myCustomEventType")
		// could be anything, an int/float/string/array/sub-json/etc.
		// will be persisted in DB and shown in UI (find and expand the Event in "Events" Tab)
		json, _ = sjson.Set(json, "yourCustomFragment", 123)
		publishJsonViaMqttMessage(client, "event/events/create", json)

		time.Sleep(time.Duration(sleepTimeSecs) * time.Second)
	}
}

func publishSmartRestMessage(client mqtt.Client, message string) {
	publishMqttMessage(client, "s/us", message)
}

func publishJsonViaMqttMessage(client mqtt.Client, topic string, jsonMessage string) {
	publishMqttMessage(client, topic, jsonMessage)
}

func publishMqttMessage(client mqtt.Client, topic string, message string) {
	qos := byte(1)
	retained := false
	pubTopic := topic
	token := client.Publish(pubTopic, qos, retained, message)
	token.Wait()
	slog.Info("Published Message", "topic", pubTopic, "msg", message, "qos", qos, "retained", retained)
}

// Every operation scheduled by Users will result in a CSV that is sent to the Device via MQTT
// This function receives and parses these messages, it contains few frequently used Operations like restarts, firmware-/software updates, log file management and SSH access
// Full list of operations can be found here: https://cumulocity.com/docs/smartrest/mqtt-static-templates/#operation-templates
func handleReceivedMessage(client mqtt.Client, msg mqtt.Message) {
	topic := msg.Topic()
	message := string(msg.Payload())
	slog.Info("Received MQTT message", "topic", topic, "msg", message)
	slog.Info("Detecting type of Operation now...")

	reader := csv.NewReader(strings.NewReader(message))
	records, _ := reader.ReadAll()
	record := records[0]
	templateId := record[0]
	switch templateId {

	// sample message: 510,DeviceSerial
	// link: https://cumulocity.com/docs/smartrest/mqtt-static-templates/#510
	case "510":
		slog.Info("A User scheduled a RESTART operation", "templateId", templateId, "serialNo", record[1])
		publishSmartRestMessage(client, "501,c8y_Restart") // set Operation to executing (shows platform Users the restart has been picked up and is done right now)
		time.Sleep(3 * time.Second)                        // simulate restart...
		publishSmartRestMessage(client, "503,c8y_Restart") // set Operation to successful (shows platform Users the restart has been done successfully)
		// if the operation had failed, you would send this message to platform
		// publishSmartRestMessage(client, "502,c8y_Restart,\"Restart failed because of XYZ\"")

	// sample message: 511,DeviceSerial,execute this
	// link: https://cumulocity.com/docs/smartrest/mqtt-static-templates/#511
	case "511":
		slog.Info("A User scheduled a SHELL operation", "templateId", templateId, "serialNo", record[1], "command", record[2])
		publishSmartRestMessage(client, "501,c8y_Command")
		time.Sleep(3 * time.Second) // simulating shell execution
		publishSmartRestMessage(client, "503,c8y_Command")

	// sample message: 515,DeviceSerial,myFirmware,1.0,http://www.my.url
	// link: https://cumulocity.com/docs/smartrest/mqtt-static-templates/#515
	case "515":
		fwName := record[2]
		fwVersion := record[3]
		fwUrl := record[4]
		slog.Info("A User scheduled a FIRMWARE UPDATE operation", "templateId", templateId, "serialNo", record[1],
			"firmwareName", fwName, "firmwareVersion", fwVersion, "firmwareDownloadUrl", fwUrl)
		publishSmartRestMessage(client, "501,c8y_Firmware")
		time.Sleep(3 * time.Second) // simulating host firmware update
		// tell platform about currently installed firmware
		publishSmartRestMessage(client, fmt.Sprintf("115,%s,%s,%s", fwName, fwVersion, fwUrl))
		// succeed Operation
		publishSmartRestMessage(client, "503,c8y_Firmware")

	// sample message: 522,DeviceSerial,logfileA,2013-06-22T17:03:14.000+02:00,2013-06-22T18:03:14.000+02:00,ERROR,1000
	// link: https://cumulocity.com/docs/smartrest/mqtt-static-templates/#522
	case "522":
		slog.Info("A User scheduled a LOG FILE RETRIEVAL operation", "templateId", templateId, "serialNo", record[1],
			"logfileName", record[2], "startDate", record[3], "endDate", record[4], "searchText", record[5], "maxLines", record[5])
		publishSmartRestMessage(client, "501,c8y_LogfileRequest")
		time.Sleep(3 * time.Second) // extract local log file and upload to platform via HTTP
		publishSmartRestMessage(client, "503,c8y_LogfileRequest")

	// sample message: 528,DeviceSerial,softwareA,1.0,url1,install,softwareB,2.0,url2,install
	// link: https://cumulocity.com/docs/smartrest/mqtt-static-templates/#528
	case "528":
		countSoftwarePackages := (len(record) - 2) / 4
		receivedSoftwarePackages := []map[string]string{}
		for i := range countSoftwarePackages {
			sw := map[string]string{
				"name":    record[2+(i*4)],
				"version": record[2+(i*4)+1],
				"url":     record[2+(i*4)+2],
				"action":  record[2+(i*4)+3],
			}
			receivedSoftwarePackages = append(receivedSoftwarePackages, sw)
		}
		slog.Info("A User scheduled a SOFTWARE UPDATE operation", "templateId", templateId, "serialNo", record[1],
			"softwarePackages", receivedSoftwarePackages)
		publishSmartRestMessage(client, "501,c8y_SoftwareUpdate")
		time.Sleep(3 * time.Second) // simulating software updates
		// submit all currently installed software packages to Cloud, see: https://cumulocity.com/docs/smartrest/mqtt-static-templates/#116
		publishSmartRestMessage(client, "116,software1,version1,url1,software2,,url2,software3,version3")
		publishSmartRestMessage(client, "503,c8y_SoftwareUpdate") // set Operation to successful

	// sample message: 530,DeviceSerial,10.0.0.67,22,eb5e9d13-1caa-486b-bdda-130ca0d87df8
	// link: https://cumulocity.com/docs/smartrest/mqtt-static-templates/#530
	case "530":
		slog.Info("A User requested REMOTE SSH ACCESS to a Device", "templateId", templateId, "serialNo", record[1],
			"ip", record[2], "port", record[3], "connectionKey", record[4])
		publishSmartRestMessage(client, "501,c8y_RemoteAccessConnect")
		time.Sleep(3 * time.Second) // connect to stated IP and Port, and route its traffic through a websocket to platform
		publishSmartRestMessage(client, "503,c8y_RemoteAccessConnect")

	default:
		slog.Info("A User requested an Operation that is not supported by the Device", "templateId", templateId, "payload", record)
	}
}
