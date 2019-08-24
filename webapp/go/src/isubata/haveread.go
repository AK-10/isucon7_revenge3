package main

import (
	"encoding/json"
	"fmt"
)

const (
	HAVE_READ_KEY = "HR_KEY"
)

func initHaveRead() error {
	r, err := NewRedisful()
	if err != nil {
		return err
	}
	defer r.Close()
	rows, err := db.Query("SELECT user_id, channel_id, message_id FROM haveread")
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var h HaveRead
		if err = rows.Scan(&h.UserID, &h.ChannelID, &h.MessageID); err != nil {
			return err
		}
		field := makeHaveReadField(h.UserID, h.ChannelID)
		r.SetHashToCache(HAVE_READ_KEY, field, h.MessageID)
	}
	return nil
}

func makeHaveReadField(uID, chID int64) string {
	return fmt.Sprintf("%d-%d", uID, chID)
}

func setHaveRead(h HaveRead) error {
	r, err := NewRedisful()
	if err != nil {
		return err
	}
	defer r.Close()
	field := makeHaveReadField(h.UserID, h.ChannelID)
	err = r.SetHashToCache(HAVE_READ_KEY, field, h.MessageID)
	if err != nil {
		return err
	}
	return nil
}

func getHaveRead(uID, chID int64) (int64, error) {
	r, err := NewRedisful()
	if err != nil {
		return 0, err
	}
	defer r.Close()

	field := makeHaveReadField(uID, chID)
	data, err := r.GetHashFromCache(HAVE_READ_KEY, field)
	if err != nil {
		return 0, err
	}
	var mID int64
	json.Unmarshal(data, &mID)
	return mID, nil
}
