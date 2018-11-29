package device

import (
	"encoding/json"
	"log"

	"github.com/mongodb/mongo-go-driver/mongo/findopt"

	"github.com/TerrexTech/go-eventstore-models/model"
	"github.com/TerrexTech/go-mongoutils/mongo"
	"github.com/pkg/errors"
)

func queryDevice(
	collection *mongo.Collection,
	event *model.Event,
	findopts ...findopt.Find,
) *model.KafkaResponse {
	filter := map[string]interface{}{}

	log.Println(event.Data)
	err := json.Unmarshal(event.Data, &filter)
	if err != nil {
		err = errors.Wrap(err, "Query: Error while unmarshalling Event-data")
		log.Println(err)
		return &model.KafkaResponse{
			AggregateID:   event.AggregateID,
			CorrelationID: event.CorrelationID,
			Error:         err.Error(),
			ErrorCode:     InternalError,
			EventAction:   event.EventAction,
			ServiceAction: event.ServiceAction,
			UUID:          event.UUID,
		}
	}

	log.Println(len(filter))
	if len(filter) == 0 {
		err = errors.New("blank filter provided")
		err = errors.Wrap(err, "Query")
		log.Println(err)
		return &model.KafkaResponse{
			AggregateID:   event.AggregateID,
			CorrelationID: event.CorrelationID,
			Error:         err.Error(),
			ErrorCode:     InternalError,
			EventAction:   event.EventAction,
			ServiceAction: event.ServiceAction,
			UUID:          event.UUID,
		}
	}

	log.Println(filter)
	result, err := collection.Find(filter, findopts...)
	log.Println(result)
	if err != nil {
		err = errors.Wrap(err, "Query: Error in DeleteMany")
		log.Println(err)
		return &model.KafkaResponse{
			AggregateID:   event.AggregateID,
			CorrelationID: event.CorrelationID,
			Error:         err.Error(),
			ErrorCode:     DatabaseError,
			EventAction:   event.EventAction,
			ServiceAction: event.ServiceAction,
			UUID:          event.UUID,
		}
	}

	resultMarshal, err := json.Marshal(result)
	if err != nil {
		err = errors.Wrap(err, "Query: Error marshalling Device Delete-result")
		log.Println(err)
		return &model.KafkaResponse{
			AggregateID:   event.AggregateID,
			CorrelationID: event.CorrelationID,
			Error:         err.Error(),
			ErrorCode:     InternalError,
			EventAction:   event.EventAction,
			ServiceAction: event.ServiceAction,
			UUID:          event.UUID,
		}
	}

	return &model.KafkaResponse{
		AggregateID:   event.AggregateID,
		CorrelationID: event.CorrelationID,
		EventAction:   event.EventAction,
		Result:        resultMarshal,
		ServiceAction: event.ServiceAction,
		UUID:          event.UUID,
	}
}
