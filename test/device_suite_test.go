package test

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/TerrexTech/agg-device-query/device"
	"github.com/TerrexTech/go-commonutils/commonutil"
	"github.com/TerrexTech/go-eventstore-models/model"
	"github.com/TerrexTech/go-kafkautils/kafka"
	"github.com/TerrexTech/uuuid"
	"github.com/joho/godotenv"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/pkg/errors"
)

func Byf(s string, args ...interface{}) {
	By(fmt.Sprintf(s, args...))
}

func TestDevice(t *testing.T) {
	log.Println("Reading environment file")
	err := godotenv.Load("../.env")
	if err != nil {
		err = errors.Wrap(err,
			".env file not found, env-vars will be read as set in environment",
		)
		log.Println(err)
	}

	missingVar, err := commonutil.ValidateEnv(
		"KAFKA_BROKERS",
		"KAFKA_CONSUMER_EVENT_GROUP",

		"KAFKA_CONSUMER_EVENT_TOPIC",
		"KAFKA_CONSUMER_EVENT_QUERY_GROUP",
		"KAFKA_CONSUMER_EVENT_QUERY_TOPIC",

		"KAFKA_PRODUCER_EVENT_TOPIC",
		"KAFKA_PRODUCER_EVENT_QUERY_TOPIC",
		"KAFKA_PRODUCER_RESPONSE_TOPIC",
	)

	if err != nil {
		err = errors.Wrapf(
			err,
			"Env-var %s is required for testing, but is not set", missingVar,
		)
		log.Fatalln(err)
	}

	RegisterFailHandler(Fail)
	RunSpecs(t, "DeviceAggregate Suite")
}

var _ = Describe("DeviceAggregate", func() {
	var (
		kafkaBrokers          []string
		eventsTopic           string
		producerResponseTopic string

		mockInv   *device.Device
		mockEvent *model.Event
	)

	BeforeSuite(func() {
		kafkaBrokers = *commonutil.ParseHosts(
			os.Getenv("KAFKA_BROKERS"),
		)
		eventsTopic = os.Getenv("KAFKA_PRODUCER_EVENT_TOPIC")
		producerResponseTopic = os.Getenv("KAFKA_PRODUCER_RESPONSE_TOPIC")

		deviceID, err := uuuid.NewV4()
		Expect(err).ToNot(HaveOccurred())

		mockInv = &device.Device{
			DeviceID:      deviceID,
			DateInstalled: time.Now().Unix(),
			Status:        "ok",
		}
		marshalInv, err := json.Marshal(mockInv)
		Expect(err).ToNot(HaveOccurred())

		cid, err := uuuid.NewV4()
		Expect(err).ToNot(HaveOccurred())
		uid, err := uuuid.NewV4()
		Expect(err).ToNot(HaveOccurred())
		uuid, err := uuuid.NewV4()
		Expect(err).ToNot(HaveOccurred())
		mockEvent = &model.Event{
			EventAction:   "insert",
			CorrelationID: cid,
			AggregateID:   device.AggregateID,
			Data:          marshalInv,
			NanoTime:      time.Now().UnixNano(),
			UserUUID:      uid,
			UUID:          uuid,
			Version:       0,
			YearBucket:    2018,
		}
	})

	Describe("Device Operations", func() {
		It("should query record", func(done Done) {
			Byf("Producing MockEvent")
			p, err := kafka.NewProducer(&kafka.ProducerConfig{
				KafkaBrokers: kafkaBrokers,
			})
			Expect(err).ToNot(HaveOccurred())
			marshalEvent, err := json.Marshal(mockEvent)
			Expect(err).ToNot(HaveOccurred())
			p.Input() <- kafka.CreateMessage(eventsTopic, marshalEvent)

			Byf("Creating query args")
			queryArgs := map[string]interface{}{
				"deviceID": mockInv.DeviceID,
			}
			marshalQuery, err := json.Marshal(queryArgs)
			Expect(err).ToNot(HaveOccurred())

			Byf("Creating query MockEvent")
			uuid, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			mockEvent.EventAction = "query"
			mockEvent.Data = marshalQuery
			mockEvent.NanoTime = time.Now().UnixNano()
			mockEvent.UUID = uuid

			Byf("Producing MockEvent")
			p, err = kafka.NewProducer(&kafka.ProducerConfig{
				KafkaBrokers: kafkaBrokers,
			})
			Expect(err).ToNot(HaveOccurred())
			marshalEvent, err = json.Marshal(mockEvent)
			Expect(err).ToNot(HaveOccurred())
			p.Input() <- kafka.CreateMessage(eventsTopic, marshalEvent)

			// Check if MockEvent was processed correctly
			Byf("Consuming Result")
			c, err := kafka.NewConsumer(&kafka.ConsumerConfig{
				KafkaBrokers: kafkaBrokers,
				GroupName:    "agginv.test.group.1",
				Topics:       []string{producerResponseTopic},
			})
			msgCallback := func(msg *sarama.ConsumerMessage) bool {
				defer GinkgoRecover()
				kr := &model.KafkaResponse{}
				err := json.Unmarshal(msg.Value, kr)
				Expect(err).ToNot(HaveOccurred())

				if kr.UUID == mockEvent.UUID {
					Expect(kr.Error).To(BeEmpty())
					Expect(kr.ErrorCode).To(BeZero())
					Expect(kr.CorrelationID).To(Equal(mockEvent.CorrelationID))
					Expect(kr.UUID).To(Equal(mockEvent.UUID))

					result := []device.Device{}
					err = json.Unmarshal(kr.Result, &result)
					Expect(err).ToNot(HaveOccurred())

					for _, r := range result {
						if r.DeviceID == mockInv.DeviceID {
							mockInv.ID = r.ID
							Expect(r).To(Equal(*mockInv))
							return true
						}
					}
				}
				return false
			}

			handler := &msgHandler{msgCallback}
			c.Consume(context.Background(), handler)

			close(done)
		}, 20)
	})
})
