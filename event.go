package eventhub

//	MIT License
//
//	Copyright (c) Microsoft Corporation. All rights reserved.
//
//	Permission is hereby granted, free of charge, to any person obtaining a copy
//	of this software and associated documentation files (the "Software"), to deal
//	in the Software without restriction, including without limitation the rights
//	to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
//	copies of the Software, and to permit persons to whom the Software is
//	furnished to do so, subject to the following conditions:
//
//	The above copyright notice and this permission notice shall be included in all
//	copies or substantial portions of the Software.
//
//	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
//	IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
//	FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
//	AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
//	LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
//	OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
//	SOFTWARE

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/Azure/go-amqp"

	"github.com/Azure/azure-event-hubs-go/v3/persist"
)

const (
	batchMessageFormat         uint32 = 0x80013700
	partitionKeyAnnotationName string = "x-opt-partition-key"
	sequenceNumberName         string = "x-opt-sequence-number"
	enqueueTimeName            string = "x-opt-enqueued-time"
)

type (
	// Event is an Event Hubs message to be sent or received
	Event struct {
		Data             []byte
		PartitionKey     *string
		Properties       map[string]interface{}
		ID               string
		message          *amqp.Message
		SystemProperties *SystemProperties
	}

	// SystemProperties are used to store properties that are set by the system.
	SystemProperties struct {
		SequenceNumber *int64     // unique sequence number of the message
		EnqueuedTime   *time.Time // time the message landed in the message queue
		Offset         *int64
		PartitionID    *int16 // This value will always be nil. For information related to the event's partition refer to the PartitionKey field in this type
		PartitionKey   *string
		// Nil for messages other than from Azure IoT Hub. deviceId of the device that sent the message.
		IoTHubDeviceConnectionID *string
		// Nil for messages other than from Azure IoT Hub. Used to distinguish devices with the same deviceId, when they have been deleted and re-created.
		IoTHubAuthGenerationID *string
		// Nil for messages other than from Azure IoT Hub. Contains information about the authentication method used to authenticate the device sending the message.
		IoTHubConnectionAuthMethod *string
		// Nil for messages other than from Azure IoT Hub. moduleId of the device that sent the message.
		IoTHubConnectionModuleID *string
		// Nil for messages other than from Azure IoT Hub. The time the Device-to-Cloud message was received by IoT Hub.
		IoTHubEnqueuedTime *time.Time
		// Raw annotations provided on the message. Includes any additional System Properties that are not explicitly mapped.
		Annotations map[string]interface{}
	}

	mapStructureTag struct {
		Name         string
		PersistEmpty bool
	}
)

func (s *SystemProperties) fromAnnotations(annotations amqp.Annotations) error {
	// Take all string-keyed annotations because the protocol reserves all
	// numeric keys for itself and there are no numeric keys defined in the
	// protocol today:
	//
	//	http://www.amqp.org/sites/amqp.org/files/amqp.pdf (section 3.2.10)
	//
	// This approach is also consistent with the behavior of .NET:
	//
	//	https://docs.microsoft.com/en-us/dotnet/api/azure.messaging.eventhubs.eventdata.systemproperties?view=azure-dotnet#Azure_Messaging_EventHubs_EventData_SystemProperties
	s.Annotations = make(map[string]interface{}, len(annotations))
	for key, val := range annotations {
		if strKey, ok := key.(string); ok {
			s.Annotations[strKey] = val
		}
	}

	for k, v := range s.Annotations {
		switch k {
		case "x-opt-sequence-number":
			val, err := toInt64(v)
			if err != nil {
				return err
			}

			s.SequenceNumber = &val
		case "x-opt-enqueued-time":
			val, err := toTime(v)
			if err != nil {
				return err
			}

			s.EnqueuedTime = &val
		case "x-opt-offset":
			val, err := toInt64(v)
			if err != nil {
				return err
			}

			s.Offset = &val
		case "x-opt-partition-id":
			val, err := toInt16(v)
			if err != nil {
				return err
			}

			s.PartitionID = &val
		case "x-opt-partition-key":
			val, err := toString(v)
			if err != nil {
				return err
			}

			s.PartitionKey = &val
		case "iothub-connection-device-id":
			val, err := toString(v)
			if err != nil {
				return err
			}

			s.IoTHubDeviceConnectionID = &val
		case "iothub-connection-auth-generation-id":
			val, err := toString(v)
			if err != nil {
				return err
			}

			s.IoTHubAuthGenerationID = &val
		case "iothub-connection-auth-method":
			val, err := toString(v)
			if err != nil {
				return err
			}

			s.IoTHubConnectionAuthMethod = &val
		case "iothub-connection-module-id":
			val, err := toString(v)
			if err != nil {
				return err
			}

			s.IoTHubConnectionModuleID = &val
		case "iothub-enqueuedtime":
			val, err := toTime(v)
			if err != nil {
				return err
			}

			s.IoTHubEnqueuedTime = &val
		}
	}

	return nil
}

func toInt64(value interface{}) (int64, error) {
	switch v := value.(type) {
	case int:
		return int64(v), nil
	case int8:
		return int64(v), nil
	case int16:
		return int64(v), nil
	case int32:
		return int64(v), nil
	case int64:
		return int64(v), nil
	case uint:
		return int64(v), nil
	case uint8:
		return int64(v), nil
	case uint16:
		return int64(v), nil
	case uint32:
		return int64(v), nil
	case uint64:
		return int64(v), nil
	case string:
		return strconv.ParseInt(v, 10, 64)
	default:
		return 0, fmt.Errorf("azure-event-hubs-go: cannot parse %T into int64", value)
	}
}

func toInt16(value interface{}) (int16, error) {
	switch v := value.(type) {
	case int:
		return int16(v), nil
	case int8:
		return int16(v), nil
	case int16:
		return int16(v), nil
	case int32:
		return int16(v), nil
	case int64:
		return int16(v), nil
	case uint:
		return int16(v), nil
	case uint8:
		return int16(v), nil
	case uint16:
		return int16(v), nil
	case uint32:
		return int16(v), nil
	case uint64:
		return int16(v), nil
	case string:
		parsed, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return 0, err
		}

		return int16(parsed), nil
	default:
		return 0, fmt.Errorf("azure-event-hubs-go: cannot parse %T into int16", value)
	}
}

func toString(value interface{}) (string, error) {
	switch v := value.(type) {
	case string:
		return v, nil
	default:
		return "", fmt.Errorf("azure-event-hubs-go: cannot parse %T into string", value)
	}
}

func toTime(value interface{}) (time.Time, error) {
	switch v := value.(type) {
	case string:
		return time.Parse(time.RFC3339, v)
	default:
		return time.Time{}, fmt.Errorf("azure-event-hubs-go: cannot parse %T into time.Time", value)
	}
}

// NewEventFromString builds an Event from a string message
func NewEventFromString(message string) *Event {
	return NewEvent([]byte(message))
}

// NewEvent builds an Event from a slice of data
func NewEvent(data []byte) *Event {
	return &Event{
		Data: data,
	}
}

// GetCheckpoint returns the checkpoint information on the Event
func (e *Event) GetCheckpoint() persist.Checkpoint {
	var offset string
	var enqueueTime time.Time
	var sequenceNumber int64
	if val, ok := e.message.Annotations[offsetAnnotationName]; ok {
		offset = fmt.Sprintf("%v", val)
	}

	if val, ok := e.message.Annotations[enqueueTimeName]; ok {
		enqueueTime = val.(time.Time)
	}

	if val, ok := e.message.Annotations[sequenceNumberName]; ok {
		sequenceNumber = val.(int64)
	}

	return persist.NewCheckpoint(offset, sequenceNumber, enqueueTime)
}

// GetKeyValues implements tab.Carrier
func (e *Event) GetKeyValues() map[string]interface{} {
	return e.Properties
}

// Set implements tab.Carrier
func (e *Event) Set(key string, value interface{}) {
	if e.Properties == nil {
		e.Properties = make(map[string]interface{})
	}
	e.Properties[key] = value
}

// Get will fetch a property from the event
func (e *Event) Get(key string) (interface{}, bool) {
	if e.Properties == nil {
		return nil, false
	}

	if val, ok := e.Properties[key]; ok {
		return val, true
	}
	return nil, false
}

func (e *Event) toMsg() (*amqp.Message, error) {
	msg := e.message
	if msg == nil {
		msg = amqp.NewMessage(e.Data)
	}

	msg.Properties = &amqp.MessageProperties{
		MessageID: e.ID,
	}

	if len(e.Properties) > 0 {
		msg.ApplicationProperties = make(map[string]interface{})
		for key, value := range e.Properties {
			msg.ApplicationProperties[key] = value
		}
	}

	if e.SystemProperties != nil {
		// Set the raw annotations first (they may be nil) and add the explicit
		// system properties second to ensure they're set properly.
		msg.Annotations = addMapToAnnotations(msg.Annotations, e.SystemProperties.Annotations)

		sysPropMap, err := encodeStructureToMap(e.SystemProperties)
		if err != nil {
			return nil, err
		}
		msg.Annotations = addMapToAnnotations(msg.Annotations, sysPropMap)
	}

	if e.PartitionKey != nil {
		if msg.Annotations == nil {
			msg.Annotations = make(amqp.Annotations)
		}

		msg.Annotations[partitionKeyAnnotationName] = e.PartitionKey
	}

	return msg, nil
}

func eventFromMsg(msg *amqp.Message) (*Event, error) {
	return newEvent(msg.Data[0], msg)
}

func newEvent(data []byte, msg *amqp.Message) (*Event, error) {
	event := &Event{
		Data:    data,
		message: msg,
	}

	if msg.Properties != nil {
		if id, ok := msg.Properties.MessageID.(string); ok {
			event.ID = id
		}
	}

	if msg.Annotations != nil {
		if val, ok := msg.Annotations[partitionKeyAnnotationName]; ok {
			if valStr, ok := val.(string); ok {
				event.PartitionKey = &valStr
			}
		}

		event.SystemProperties = new(SystemProperties)

		if err := event.SystemProperties.fromAnnotations(msg.Annotations); err != nil {
			fmt.Println("error decoding...", err)
			return event, err
		}
	}

	if msg != nil {
		event.Properties = msg.ApplicationProperties
	}

	return event, nil
}

func encodeStructureToMap(structPointer interface{}) (map[string]interface{}, error) {
	valueOfStruct := reflect.ValueOf(structPointer)
	s := valueOfStruct.Elem()
	if s.Kind() != reflect.Struct {
		return nil, fmt.Errorf("must provide a struct")
	}

	encoded := make(map[string]interface{})
	for i := 0; i < s.NumField(); i++ {
		f := s.Field(i)
		if f.IsValid() && f.CanSet() {
			tf := s.Type().Field(i)
			tag, err := parseMapStructureTag(tf.Tag)
			if err != nil {
				return nil, err
			}

			// Skip any entries with an exclude tag
			if tag.Name == "-" {
				continue
			}

			if tag != nil {
				switch f.Kind() {
				case reflect.Ptr:
					if !f.IsNil() || tag.PersistEmpty {
						if f.IsNil() {
							encoded[tag.Name] = nil
						} else {
							encoded[tag.Name] = f.Elem().Interface()
						}
					}
				default:
					if f.Interface() != reflect.Zero(f.Type()).Interface() || tag.PersistEmpty {
						encoded[tag.Name] = f.Interface()
					}
				}
			}
		}
	}

	return encoded, nil
}

func parseMapStructureTag(tag reflect.StructTag) (*mapStructureTag, error) {
	str, ok := tag.Lookup("mapstructure")
	if !ok {
		return nil, nil
	}

	mapTag := new(mapStructureTag)
	split := strings.Split(str, ",")
	mapTag.Name = strings.TrimSpace(split[0])

	if len(split) > 1 {
		for _, tagKey := range split[1:] {
			switch tagKey {
			case "persistempty":
				mapTag.PersistEmpty = true
			default:
				return nil, fmt.Errorf("key %q is not understood", tagKey)
			}
		}
	}
	return mapTag, nil
}

func addMapToAnnotations(a amqp.Annotations, m map[string]interface{}) amqp.Annotations {
	if a == nil && len(m) > 0 {
		a = make(amqp.Annotations)
	}
	for key, val := range m {
		a[key] = val
	}
	return a
}
