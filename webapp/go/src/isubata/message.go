package main

import (
	"encoding/json"

	"net/http"
	"strconv"
	"time"

	"database/sql"
	"github.com/gomodule/redigo/redis"
	"github.com/labstack/echo"
)

const (
	messagePrefix      = "MESSAGE-CHANNEL-"
	messageCountPrefix = "MESSAGE-NUM-CHANNEL-"
)

func makeMessageKey(chID int64) string {
	return messagePrefix + strconv.FormatInt(chID, 10)
}

func makeMessageCountKey(chID int64) string {
	return messageCountPrefix + strconv.FormatInt(chID, 10)
}

// ここで渡されるmessagesのidは降順のはず
func getMessagesToLastID(messages []Message, lastID int64) []Message {
	var msgs = []Message{}
	for i, msg := range messages {
		if lastID <= msg.ID || i == 100 {
			break
		}
		msgs = append(msgs, msg)
	}
	return msgs
}

// ここで渡されるmessagesのidは降順のはず
func BinarySearchAboutLastId(left, right int, lastID int64, messages []Message) int {
	if messages[left].ID < lastID || messages[right].ID > lastID {
		return len(messages)
	}
	for left < right {
		mid := (left + right) / 2
		if messages[mid].ID == lastID {
			return mid
		} else if messages[mid].ID < lastID {
			left = mid
		} else {
			right = mid
		}
	}
	if messages[left].ID == lastID {
		return left
	}
	return len(messages)
}

func initMessageToCache() error {
	chIDs := []int64{}
	err := db.Select(&chIDs, "SELECT id FROM channel")
	if err != nil {
		return err
	}
	for _, chID := range chIDs {
		rows, err := db.Query("SELECT m.*, u.* FROM message AS m INNER JOIN user AS u ON m.user_id = u.id WHERE channel_id = ? ORDER BY m.id", chID)
		if err != nil {
			return err
		}

		for rows.Next() {
			var (
				m Message
				u User
			)
			err := rows.Scan(&m.ID, &m.ChannelID, &m.UserID, &m.Content, &m.CreatedAt, &u.ID, &u.Name, &u.Salt, &u.Password, &u.DisplayName, &u.AvatarIcon, &u.CreatedAt)
			if err != nil {
				return err
			}
			m.User = u
			if err = addMessageToCache(chID, m); err != nil {
				return err
			}
		}
	}
	return nil
}

func addMessageToCache(chID int64, msg Message) error {
	r, err := NewRedisful()
	if err != nil {
		return err
	}
	defer r.Close()

	key := makeMessageKey(chID)
	if err = r.LPushListToCache(key, msg); err != nil {
		return err
	}
	return nil
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

func queryMessagesWithUserFromCache(chID, lastID int64, paginate bool, limit, offset int64) ([]Message, error) {
	r, err := NewRedisful()
	if err != nil {
		return nil, err
	}
	defer r.Close()

	var msgs []Message
	key := makeMessageKey(chID)
	if paginate {
		data, err := r.GetListFromCacheWithLimitOffset(key, limit, offset)
		if err != nil {
			return nil, err
		}
		json.Unmarshal(data, &msgs)
	} else {
		var messages []Message
		data, err := r.GetListFromCache(key)
		if err != nil {
			return nil, err
		}
		json.Unmarshal(data, messages)
		msgs = getMessagesToLastID(messages, lastID)
	}
	if err != nil {
		return nil, err
	}
	return msgs, nil
}

func queryMessagesWithUser(chID, lastID int64, paginate bool, limit, offset int64) ([]Message, error) {
	msgs := []Message{}
	var (
		rows *sql.Rows
		err  error
	)
	if paginate {
		rows, err = db.Query("SELECT m.*, u.* FROM message AS m "+
			"INNER JOIN user AS u ON m.user_id = u.id "+
			"WHERE m.channel_id = ? ORDER BY m.id DESC LIMIT ? OFFSET ?",
			chID, limit, offset)
	} else {
		rows, err = db.Query("SELECT m.*, u.* FROM message AS m "+
			"INNER JOIN user AS u ON m.user_id = u.id "+
			"WHERE m.id > ? AND m.channel_id = ? ORDER BY m.id DESC LIMIT 100",
			lastID,
			chID)
	}
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
	// set to cache
	msg := Message{ChannelID: chanID, UserID: user.ID, Content: message, CreatedAt: time.Now(), User: *user}
	if err := addMessageToCache(chanID, msg); err != nil {
		return nil
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

	//messages, err := queryMessagesWithUser(chanID, lastID, false, 0, 0)
	messages, err := queryMessagesWithUserFromCache(chanID, lastID, false, 0, 0)
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
	lastID, err := getHaveRead(userID, chID)
	if err != nil {
		if err == redis.ErrNil {
			return 0, nil
		}
		return 0, nil
	}
	return lastID, nil
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
