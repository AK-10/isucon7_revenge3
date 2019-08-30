package main

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/labstack/echo"
)

const (
	messageCountPrefix = "MESSAGE-NUM-CHANNEL-"

	M_STRUCT_KEY = string("M-CH-ID-")
	M_ID_KEY     = string("M-ID")
)

// ignore User struct
func (m Message) toJson() interface{} {
	r := make(map[string]interface{})
	r["id"] = m.ID
	r["channel_id"] = m.ChannelID
	r["user_id"] = m.UserID
	r["content"] = m.Content
	r["created_at"] = m.CreatedAt
	return r
}

func makeMessageCountKey(chID int64) string {
	return messageCountPrefix + strconv.FormatInt(chID, 10)
}

func InitMessagesCache() error {
	rows, err := db.Query("SELECT m.*, u.name, u.display_name, u.avatar_icon FROM message m inner join user u on u.id = m.user_id")
	if err != nil {
		return err
	}
	defer rows.Close()
	r, err := NewRedisful()
	if err != nil {
		return err
	}
	defer r.Close()

	var m Message
	var u User
	var key string
	var lastID int64
	for rows.Next() {
		if err := rows.Scan(&m.ID, &m.ChannelID, &m.UserID, &m.Content, &m.CreatedAt, &u.Name, &u.DisplayName, &u.AvatarIcon); err != nil {
			return err
		}
		m.User = u
		key = makeMessagesKey(m)
		r.PushSortedSetToCache(key, int(m.ID), m)
		lastID = m.ID
	}
	key = M_ID_KEY
	r.SetDataToCache(key, int(lastID))
	return nil
}

func makeMessagesKey(m Message) string {
	return fmt.Sprintf("%s%d", M_STRUCT_KEY, m.ChannelID)
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
	err = r.GetDataFromCache(key, &count)
	if err != nil {
		return 0, err
	}
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

func addMessage(channelID int64, user User, content string) (int64, error) {
	r, err := NewRedisful()
	if err != nil {
		return 0, err
	}
	var lastID int64
	err = r.GetDataFromCache(M_ID_KEY, &lastID)
	lastID++
	m := Message{ID: lastID, ChannelID: channelID, UserID: user.ID, Content: content, CreatedAt: time.Now(), User: user}

	key := makeMessagesKey(m)
	ok, err := r.PushSortedSetToCache(key, int(m.ID), m)
	if err != nil {
		return 0, err
	}
	if !ok {
		return 0, errors.New("not inserted")
	}
	r.IncrementDataInCache(M_ID_KEY)
	if err = incrementMessageCount(channelID); err != nil {
		return 0, err
	}
	r.Close()
	return m.ID, nil
}

// func addMessage(channelID, userID int64, content string) (int64, error) {
// 	res, err := db.Exec(
// 		"INSERT INTO message (channel_id, user_id, content, created_at) VALUES (?, ?, ?, NOW())",
// 		channelID, userID, content)
// 	if err != nil {
// 		return 0, err
// 	}
// 	if err = incrementMessageCount(channelID); err != nil {
// 		return 0, err
// 	}
//
// 	return res.LastInsertId()
// }

func queryMessagesWithUser(chID, lastID int64, paginate bool, limit, offset int64) ([]Message, error) {
	msgs := []Message{}
	if paginate {
		rows, err := db.Query("SELECT m.*, u.* FROM message AS m "+
			"INNER JOIN user AS u ON m.user_id = u.id "+
			"WHERE m.channel_id = ? ORDER BY m.id DESC LIMIT ? OFFSET ?",
			chID, limit, offset)

		if err != nil {
			return nil, err
		}

		for rows.Next() {
			var m Message
			var u User
			err := rows.Scan(&m.ID, &m.ChannelID, &m.UserID, &m.Content, &m.CreatedAt, &u.ID, &u.Name, &u.Salt, &u.Password, &u.DisplayName, &u.AvatarIcon, &u.CreatedAt)
			if err != nil {
				return nil, err
			}
			m.User = u
			msgs = append(msgs, m)
		}
	} else {
		rows, err := db.Query("SELECT m.*, u.* FROM message AS m "+
			"INNER JOIN user AS u ON m.user_id = u.id "+
			"WHERE m.id > ? AND m.channel_id = ? ORDER BY m.id DESC LIMIT 100",
			lastID,
			chID)
		if err != nil {
			return nil, err
		}

		for rows.Next() {
			var m Message
			var u User
			err := rows.Scan(&m.ID, &m.ChannelID, &m.UserID, &m.Content, &m.CreatedAt, &u.ID, &u.Name, &u.Salt, &u.Password, &u.DisplayName, &u.AvatarIcon, &u.CreatedAt)
			if err != nil {
				return nil, err
			}
			m.User = u
			msgs = append(msgs, m)
		}
	}
	return msgs, nil
}

func jsonifyMessageWithUser(message Message) map[string]interface{} {
	r := make(map[string]interface{})
	r["id"] = message.ID
	r["user"] = message.User
	r["date"] = message.CreatedAt.Format("2006/01/02 15:04:05")
	r["content"] = message.Content

	return r
}

func queryMessages(chanID, lastID int64, offset, limit int) ([]Message, error) {
	msgs := []Message{}
	r, err := NewRedisful()
	if err != nil {
		return nil, err
	}
	defer r.Close()

	var maxMessageID int
	err = r.GetDataFromCache(M_ID_KEY, &maxMessageID)
	if err != nil {
		return nil, err
	}

	key := makeMessagesKey(Message{ChannelID: chanID})
	err = r.GetSortedSetRankRangeWithLimitFromCache(key, int(lastID), maxMessageID, offset, limit, true, &msgs)
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

	if _, err := addMessage(chanID, *user, message); err != nil {
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

	var messages []Message
	if lastID == 0 {
		messages, err = queryMessages(chanID, lastID, 0, -1)
	} else {
		messages, err = queryMessages(chanID, lastID+1, 0, -1)
	}
	if err != nil {
		return err
	}

	response := make([]map[string]interface{}, 0)
	for i := len(messages) - 1; i >= 0; i-- {
		m := messages[i]
		r := jsonifyMessageWithUser(m)
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

	time.Sleep(2 * time.Second)

	channels, err := queryChannels()
	if err != nil {
		return err
	}

	resp := []map[string]interface{}{}

	r, err := NewRedisful()
	if err != nil {
		return err
	}
	defer r.Close()

	var maxMessageID int
	err = r.GetDataFromCache(M_ID_KEY, &maxMessageID)
	if err != nil {
		return err
	}
	maxMessageID += 10000

	for _, chID := range channels {
		lastID, err := queryHaveRead(userID, chID)
		if err != nil {
			return err
		}

		var cnt int64
		key := makeMessagesKey(Message{ChannelID: chID})
		if lastID > 0 {
			var msgs []Message
			err = r.GetSortedSetRankRangeWithLimitFromCache(key, int(lastID+1), maxMessageID, 0, -1, true, &msgs)
			cnt = int64(len(msgs))
		} else {
			cnt, err = r.GetSortedSetLengthFromCache(key)
		}
		r := map[string]interface{}{
			"channel_id": chID,
			"unread":     cnt}
		resp = append(resp, r)
	}

	return c.JSON(http.StatusOK, resp)
}
