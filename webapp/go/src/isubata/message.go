package main

import (
	"fmt"
	"net/http"
	"strconv"
	"time"

	// "github.com/gomodule/redigo/redis"
	"github.com/labstack/echo"
)

const (
	messageKey = string("M-CH-ID-")
	MAX_INT    = int(2147483647)
)

func (r *Redisful) initMessages() error {
	rows, err := db.Query(`
SELECT m.*, u.name, u.display_name, u.avatar_icon FROM message m INNER JOIN user u on u.id = m.user_id
	`)
	if err != nil {
		return err
	}

	var m Message

	for rows.Next() {
		if err = rows.Scan(&m.ID, &m.ChannelID, &m.UserID, &m.Content, &m.CreatedAt, &m.User.Name, &m.User.DisplayName, &m.User.AvatarIcon); err != nil {
			return err
		}
		r.addMessage(m)
	}
	return nil
}

func (r *Redisful) getMessageCount(chID int64) (int64, error) {
	key := makeMessageKey(chID)
	return r.GetSortedSetLengthFromCache(key)
}

func (r *Redisful) addMessage(m Message) error {
	key := makeMessageKey(m.ChannelID)
	err := r.PushSortedSetToCache(key, int(m.ID), m)
	if err != nil {
		return err
	}
	return nil
}

func addMessage(channelID int64, user User, content string) (int64, error) {
	timeNow := time.Now()
	res, err := db.Exec(
		"INSERT INTO message (channel_id, user_id, content, created_at) VALUES (?, ?, ?, ?)",
		channelID, user.ID, content, timeNow)
	if err != nil {
		return 0, err
	}
	lastID, err := res.LastInsertId()
	if err != nil {
		return lastID, err
	}
	r, err := NewRedisful()
	if err != nil {
		fmt.Println(err)
		return 0, nil
	}
	m := Message{
		ID:        lastID,
		ChannelID: channelID,
		UserID:    user.ID,
		Content:   content,
		CreatedAt: timeNow,
		User:      user,
	}
	err = r.addMessage(m)
	if err != nil {
		fmt.Println("DEBUG SORTED SET NOT INSERTED: ", err)
	}
	r.Close()

	return lastID, nil
}

func makeMessageKey(chID int64) string {
	return fmt.Sprintf("%s%d", messageKey, chID)
}

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

func (r *Redisful) queryMessagesWithUser(chID, lastID int64, paginate bool, limit, offset int64) ([]Message, error) {
	var msgs []Message
	key := makeMessageKey(chID)
	if paginate {
		err := r.GetSortedSetRankRangeWithLimitFromCache(key, 0, MAX_INT, int(offset), int(limit), true, &msgs)
		if err != nil {
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
		}
	} else {
		err := r.GetSortedSetRankRangeWithLimitFromCache(key, int(lastID), MAX_INT, 0, int(limit), true, &msgs)
		if err != nil {
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

	messages, err := queryMessagesWithUser(chanID, lastID, false, 0, 0)
	if err != nil {
		return err
	}

	response := make([]map[string]interface{}, 0)
	for i := len(messages) - 1; i >= 0; i-- {
		m := messages[i]
		r := jsonifyMessageWithUser(m)
		response = append(response, r)
	}
	// queryMessagesWithUser の処理次第で前に移動
	r, err := NewRedisful()
	if err != nil {
		return nil
	}

	if len(messages) > 0 {
		err = r.setHaveRead(HaveRead{UserID: userID, ChannelID: chanID, MessageID: messages[0].ID})
		if err != nil {
			return err
		}
	}
	r.Close()

	return c.JSON(http.StatusOK, response)
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

	r, err := NewRedisful()
	if err != nil {
		return err
	}
	for _, chID := range channels {
		lastID, err := r.getHaveRead(userID, chID)
		if err != nil {
			return err
		}

		var cnt int64
		if lastID > 0 {
			var msgs []Message
			key := makeMessageKey(chID)
			err := r.GetSortedSetRankRangeFromCache(key, int(lastID+1), MAX_INT, false, &msgs)
			if err != nil {
				err = db.Get(&cnt,
					"SELECT COUNT(*) as cnt FROM message WHERE channel_id = ? AND ? < id",
					chID, lastID)
			} else {
				cnt = int64(len(msgs))
			}
		} else {
			cnt, err := r.getMessageCount(chID)
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
	r.Close()

	return c.JSON(http.StatusOK, resp)
}
