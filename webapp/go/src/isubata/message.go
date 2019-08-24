package main

import (
	"encoding/json"

	"net/http"
	"strconv"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/labstack/echo"
)

const (
	messageCountPrefix = "MESSAGE-NUM-CHANNEL-"
)

func makeMessageCountKey(chID int64) string {
	return messageCountPrefix + strconv.FormatInt(chID, 10)
}

func initMessageCountCache() error {
	type MessageCounter struct {
		ChannelID int64
		Count     int64
	}
	rows, err := db.Query("SELECT channel_id, COUNT(*) FROM message GROUP BY channel_id")
	if err != nil {
		return err
	}

	for rows.Next() {
		var mc MessageCounter
		if err = rows.Scan(&mc.ChannelID, &mc.Count); err != nil {
			return err
		}
		if err = setMessageCountToCache(mc.ChannelID, mc.Count); err != nil {
			return err
		}
	}
	return nil
}

func setMessageCountToCache(chID, num int64) error {
	r, err := NewRedisful()
	if err != nil {
		return err
	}
	defer r.Close()

	key := makeMessageCountKey(chID)
	if err = r.SetDataToCache(key, num); err != nil {
		return err
	}
	return nil
}

func getMessageCountFromCache(chID int64) (int64, error) {
	r, err := NewRedisful()
	if err != nil {
		return 0, err
	}
	defer r.Close()

	key := makeMessageCountKey(chID)
	var count int64
	data, err := r.GetDataFromCache(key)
	if err != nil {
		return 0, err
	}
	json.Unmarshal(data, &count)

	return count, nil
}

func incrementMessageCount(chID int64) error {
	r, err := NewRedisful()
	if err != nil {
		return nil
	}
	defer r.Close()
	key := makeMessageCountKey(chID)
	err = r.IncrementDataInCache(key)
	if err != nil {
		return err
	}
	return nil
}

func addMessage(channelID, userID int64, content string) (int64, error) {
	res, err := db.Exec(
		"INSERT INTO message (channel_id, user_id, content, created_at) VALUES (?, ?, ?, NOW())",
		channelID, userID, content)
	if err != nil {
		return 0, err
	}
	if err = incrementMessageCount(channelID); err != nil {
		return 0, err
	}

	return res.LastInsertId()
}

func queryMessages(chanID, lastID int64) ([]Message, error) {
	msgs := []Message{}
	err := db.Select(&msgs, "SELECT * FROM message WHERE id > ? AND channel_id = ? ORDER BY id DESC LIMIT 100",
		lastID, chanID)
	return msgs, err
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

	if _, err := addMessage(chanID, user.ID, message); err != nil {
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
			err = db.Get(&cnt,
				"SELECT COUNT(*) as cnt FROM message WHERE channel_id = ? AND ? < id",
				chID, lastID)
		} else {
			cnt, err = getMessageCountFromCache(chID)
			if err != nil {
				err = db.Get(&cnt, "SELECT COUNT(*) as cnt FROM message WHERE channel_id = ?", chID)
				if err != nil {
					return err
				}
			}
		}
		r := map[string]interface{}{
			"channel_id": chID,
			"unread":     cnt}
		resp = append(resp, r)
	}

	return c.JSON(http.StatusOK, resp)
}
