package device

import (
	"encoding/json"

	util "github.com/TerrexTech/go-commonutils/commonutil"

	"github.com/TerrexTech/uuuid"
	"github.com/mongodb/mongo-go-driver/bson"
	"github.com/mongodb/mongo-go-driver/bson/objectid"
	"github.com/pkg/errors"
)

// AggregateID is the global AggregateID for Device Aggregate.
const AggregateID int8 = 6

// Device defines the Device Aggregate.
type Device struct {
	ID              objectid.ObjectID `bson:"_id,omitempty" json:"_id,omitempty"`
	DeviceID        uuuid.UUID        `bson:"deviceID,omitempty" json:"deviceID,omitempty"`
	DateInstalled   int64             `bson:"dateInstalled,omitempty" json:"dateInstalled,omitempty"`
	Lot             string            `bson:"lot,omitempty" json:"lot,omitempty"`
	LastMaintenance int64             `bson:"lastMaintenance,omitempty" json:"lastMaintenance,omitempty"`
	Status          string            `bson:"status,omitempty" json:"status,omitempty"`
}

// MarshalBSON returns bytes of BSON-type.
func (d Device) MarshalBSON() ([]byte, error) {
	in := map[string]interface{}{
		"deviceID":        d.DeviceID.String(),
		"dateInstalled":   d.DateInstalled,
		"lot":             d.Lot,
		"lastMaintenance": d.LastMaintenance,
		"status":          d.Status,
	}

	if d.ID != objectid.NilObjectID {
		in["_id"] = d.ID
	}
	return bson.Marshal(in)
}

// MarshalJSON returns bytes of JSON-type.
func (d *Device) MarshalJSON() ([]byte, error) {
	in := map[string]interface{}{
		"deviceID":        d.DeviceID.String(),
		"dateInstalled":   d.DateInstalled,
		"lot":             d.Lot,
		"lastMaintenance": d.LastMaintenance,
		"status":          d.Status,
	}

	if d.ID != objectid.NilObjectID {
		in["_id"] = d.ID.Hex()
	}
	return json.Marshal(in)
}

// UnmarshalBSON returns BSON-type from bytes.
func (d *Device) UnmarshalBSON(in []byte) error {
	m := make(map[string]interface{})
	err := bson.Unmarshal(in, m)
	if err != nil {
		err = errors.Wrap(err, "Unmarshal Error")
		return err
	}

	err = d.unmarshalFromMap(m)
	return err
}

// UnmarshalJSON returns JSON-type from bytes.
func (d *Device) UnmarshalJSON(in []byte) error {
	m := make(map[string]interface{})
	err := json.Unmarshal(in, &m)
	if err != nil {
		err = errors.Wrap(err, "Unmarshal Error")
		return err
	}

	err = d.unmarshalFromMap(m)
	return err
}

// unmarshalFromMap unmarshals Map into Device.
func (d *Device) unmarshalFromMap(m map[string]interface{}) error {
	var err error
	var assertOK bool

	// Hoping to discover a better way to do this someday
	if m["_id"] != nil {
		d.ID, assertOK = m["_id"].(objectid.ObjectID)
		if !assertOK {
			d.ID, err = objectid.FromHex(m["_id"].(string))
			if err != nil {
				err = errors.Wrap(err, "Error while asserting ObjectID")
				return err
			}
		}
	}

	if m["deviceID"] != nil {
		deviceIDStr, assertOK := m["deviceID"].(string)
		if !assertOK {
			err = errors.New("error asserting to string")
			err = errors.Wrap(err, "Error while asserting DeviceID")
			return err
		}
		d.DeviceID, err = uuuid.FromString(deviceIDStr)
		if err != nil {
			err = errors.Wrap(err, "Error while parsing DeviceID")
			return err
		}
	}

	if m["dateInstalled"] != nil {
		d.DateInstalled, err = util.AssertInt64(m["dateInstalled"])
		if err != nil {
			err = errors.Wrap(err, "Error while asserting DateInstalled")
			return err
		}
	}
	if m["lastMaintenance"] != nil {
		d.LastMaintenance, err = util.AssertInt64(m["lastMaintenance"])
		if err != nil {
			err = errors.Wrap(err, "Error while asserting LastMaintenance")
			return err
		}
	}
	if m["lot"] != nil {
		d.Lot, assertOK = m["lot"].(string)
		if !assertOK {
			err = errors.New("error asserting to string")
			err = errors.Wrap(err, "Error while asserting Lot")
			return err
		}
	}
	if m["status"] != nil {
		d.Status, assertOK = m["status"].(string)
		if !assertOK {
			err = errors.New("error asserting to string")
			err = errors.Wrap(err, "Error while asserting Status")
			return err
		}
	}
	return nil
}
