# About

This is an implementation of a Cumulocity Device-Agent that is using Smart Rest via MQTT. It is: 
* Creating a device twin in the Cloud
* Setting Twin Properties
* and supports following remote Operations: Software-/Firmware Update, Log File Management, SSH Access, Restarts and shell commands
* The operation support is covering all required API aspects to receive and update Operations and the Cloud Twin. The actual actions (e.g. doing the firmware update or fetching local log files) is simulated

This is how the Device will be shown in Cumulocity

# Why does this project exist

It exists to showcase how a Device Integration via MQTT to Cumulocity looks like in case you cannot use https://thin-edge.io . 