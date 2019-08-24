package main

import (
	"database/sql"
	"net/http"
	"strconv"
	"time"

	"github.com/labstack/echo"
)

func addMessage(channelID, userID int64, content string) (int64, error) {
	res, err := db.Exec(
		"INSERT INTO message (channel_id, user_id, content, created_at) VALUES (?, ?, ?, NOW())",
		channelID, userID, content)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func queryMessagesWithUser(chID, lastID int64, paginate bool, limit, offset int64) ([]Message, error) {
	msgs := []Message{}
	if paginate {
		rows, err := db.Query("SELECT m.*, u.* FROM message AS m"+
			"INNER JOIN user AS u ON m.user_id = u.id "+
			"WHERE channel_id = ? ORDER BY m.id DESC LIMIT ? OFFSET ?",
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
			"WHERE m.id > ? AND channel_id = ? ORDER BY m.id DESC LIMIT 100",
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

	if len(messages) > 0 {
		_, err := db.Exec("INSERT INTO haveread (user_id, channel_id, message_id, updated_at, created_at)"+
			" VALUES (?, ?, ?, NOW(), NOW())"+
			" ON DUPLICATE KEY UPDATE message_id = ?, updated_at = NOW()",
			userID, chanID, messages[0].ID, messages[0].ID)
		if err != nil {
			return err
		}
	}

	return c.JSON(http.StatusOK, response)
}

func queryHaveRead(userID, chID int64) (int64, error) {
	type HaveRead struct {
		UserID    int64     `db:"user_id"`
		ChannelID int64     `db:"channel_id"`
		MessageID int64     `db:"message_id"`
		UpdatedAt time.Time `db:"updated_at"`
		CreatedAt time.Time `db:"created_at"`
	}
	h := HaveRead{}

	err := db.Get(&h, "SELECT * FROM haveread WHERE user_id = ? AND channel_id = ?",
		userID, chID)

	if err == sql.ErrNoRows {
		return 0, nil
	} else if err != nil {
		return 0, err
	}
	return h.MessageID, nil
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
			err = db.Get(&cnt,
				"SELECT COUNT(*) as cnt FROM message WHERE channel_id = ?",
				chID)
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
