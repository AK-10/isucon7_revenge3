package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/labstack/echo"
)

const (
	M_LAST_ID = "M-ID"
	M_ID_KEY  = "M-CH-ID"
)

// func addMessage(channelID, userID int64, content string) (int64, error) {
// 	res, err := db.Exec(
// 		"INSERT INTO message (channel_id, user_id, content, created_at) VALUES (?, ?, ?, NOW())",
// 		channelID, userID, content)
// 	if err != nil {
// 		return 0, err
// 	}
// 	return res.LastInsertId()
// }

// func queryMessages(chanID, lastID int64) ([]Message, error) {
// 	msgs := []Message{}
// 	err := db.Select(&msgs, "SELECT * FROM message WHERE id > ? AND channel_id = ? ORDER BY id DESC LIMIT 100",
// 		lastID, chanID)
// 	return msgs, err
// }

func queryMessages(chanID, lastID int64) ([]Message, error) {
	msgs := []Message{}

	r, err := NewRedisful()
	if err != nil {
		return nil, err
	}
	defer r.Close()
	key := makeMessageKey(chanID)
	keys, err := r.GetHashKeysInCache(key)
	if err != nil {
		return nil, err
	}

	// sort
	sort.Slice(keys, func(i, j int) bool {
		s_i, _ := strconv.Atoi(keys[i])
		s_j, _ := strconv.Atoi(keys[j])
		return s_i < s_j
	})

	// ORDER BY id DESC LIMIT 100
	keys = getLastNArray(keys, 100)
	// 	WHERE id > ?
	f := func(s string, lastID int) bool {
		i, _ := strconv.Atoi(s)
		return (i > lastID)
	}
	keys = selectStringArray(keys, lastID, f)
	data, err := r.GetMultiFromCache(key, keys)
	if err != nil {
		return nil, err
	}
	json.Unmarshal(data, &msgs)
	return msgs, nil
}

//request handlers

func postMessage(c echo.Context) error {
	user, err := ensureLogin(c)
	if user == nil {
		return err
	}

	message := c.FormValue("message")
	if message == "" {
		return echo.ErrForbidden
	}

	var chanID int64
	if x, err := strconv.Atoi(c.FormValue("channel_id")); err != nil {
		return echo.ErrForbidden
	} else {
		chanID = int64(x)
	}

	if err := addMessageToCache(Message{ChannelID: chanID, UserID: user.ID, Content: message}); err != nil {
		return err
	}

	return c.NoContent(204)
}

func getMessage(c echo.Context) error {
	userID := sessUserID(c)
	if userID == 0 {
		return c.NoContent(http.StatusForbidden)
	}

	chanID, err := strconv.ParseInt(c.QueryParam("channel_id"), 10, 64)
	if err != nil {
		return err
	}
	lastID, err := strconv.ParseInt(c.QueryParam("last_message_id"), 10, 64)
	if err != nil {
		return err
	}

	messages, err := queryMessages(chanID, lastID)
	if err != nil {
		return err
	}

	response := make([]map[string]interface{}, 0)
	for i := len(messages) - 1; i >= 0; i-- {
		m := messages[i]
		r, err := jsonifyMessage(m)
		if err != nil {
			return err
		}
		response = append(response, r)
	}

	if len(messages) > 0 {
		err = setHaveRead(HaveRead{UserID: userID, ChannelID: chanID, MessageID: messages[0].ID})
		if err != nil {
			return err
		}
	}

	return c.JSON(http.StatusOK, response)
}

func queryHaveRead(userID, chID int64) (int64, error) {
	mID, err := getHaveRead(userID, chID)
	if err != nil {
		if err == redis.ErrNil {
			return 0, nil
		}
		return 0, nil
	}
	return mID, nil
}

func fetchUnread(c echo.Context) error {
	userID := sessUserID(c)
	if userID == 0 {
		return c.NoContent(http.StatusForbidden)
	}

	time.Sleep(time.Second)

	channels, err := queryChannels()
	if err != nil {
		return err
	}

	resp := []map[string]interface{}{}

	for _, chID := range channels {
		lastID, err := queryHaveRead(userID, chID)
		if err != nil {
			return err
		}

		var cnt int64
		if lastID > 0 {
			count, err := getMultiMessageCount(chID, lastID)
			if err != nil {
				return err
			}
			cnt = int64(count)
		} else {
			count, err := getMessageCount(chID)
			if err != nil {
				return err
			}
			cnt = int64(count)
		}
		if err != nil {
			return err
		}
		r := map[string]interface{}{
			"channel_id": chID,
			"unread":     cnt}
		resp = append(resp, r)
	}

	return c.JSON(http.StatusOK, resp)
}

func initMessages() error {
	r, err := NewRedisful()
	if err != nil {
		return err
	}
	defer r.Close()

	rows, err := db.Query("SELECT id, channel_id, user_id, content FROM message")
	if err != nil {
		return err
	}
	defer rows.Close()

	var lastID int64
	for rows.Next() {
		var m Message
		if err = rows.Scan(&m.ID, &m.ChannelID, &m.UserID, &m.Content); err != nil {
			return err
		}
		lastID = m.ID
		r.SetHashToCache(makeMessageKey(m.ChannelID), makeMessageField(m.ID), m)
	}
	r.SetDataToCache(M_LAST_ID, lastID)
	return nil
}

func makeMessageField(id int64) string {
	return fmt.Sprintf("%d", id)
}

func makeMessageKey(chID int64) string {
	return fmt.Sprintf("%s-%d", M_ID_KEY, chID)
}

func addMessageToCache(m Message) error {
	key := makeMessageKey(m.ChannelID)
	r, err := NewRedisful()
	if err != nil {
		return err
	}
	defer r.Close()
	data, err := r.GetDataFromCache(M_LAST_ID)
	if err != nil {
		return err
	}
	var lastID int64
	json.Unmarshal(data, &lastID)
	m.ID = lastID
	err = r.Transaction(func() {
		r.SetHashToCache(key, makeMessageField(lastID), m)
		r.IncrementDataInCache(M_LAST_ID)
	})

	if err != nil {
		return err
	}
	return nil
}

func getMessageCount(chID int64) (int, error) {
	r, err := NewRedisful()
	if err != nil {
		return 0, err
	}
	defer r.Close()
	data, err := r.GetHashLengthInCache(makeMessageKey(chID))
	if err != nil {
		return 0, err
	}
	return int(data), nil
}

func getMultiMessageCount(chID, lastID int64) (int, error) {
	r, err := NewRedisful()
	if err != nil {
		return 0, err
	}
	defer r.Close()

	key := makeMessageKey(chID)
	keys, err := r.GetHashKeysInCache(key)
	if err != nil {
		return 0, err
	}

	// 	WHERE id > ?
	f := func(s string, lastID int) bool {
		i, _ := strconv.Atoi(s)
		return (i > lastID)
	}
	keys = selectStringArray(keys, lastID, f)

	return len(keys), nil
}
