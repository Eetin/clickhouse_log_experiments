package main

import (
	"os"
	"log"
	"bufio"
	"regexp"
	"fmt"
	"github.com/satyrius/gonx"
	"io"
	"encoding/csv"
	"database/sql"

	"github.com/kshvakov/clickhouse"
	"time"
	"strings"
	"strconv"
)

func main() {
	//logToCSV("public.log.100.clean")
	fillDB("public.log.100.clean")

}

func fillDB(filename string) {
	connect, err := sql.Open("clickhouse", "tcp://127.0.0.1:9000?debug=true")
	if err != nil {
		log.Fatal(err)
	}
	if err := connect.Ping(); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			fmt.Printf("[%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
		} else {
			fmt.Println(err)
		}
		return
	}

	_, err = connect.Exec(`
			CREATE TABLE IF NOT EXISTS access_logs (
				date Date,
				remote_addr String,
				remote_user String,
				time_local DateTime,
				request String,
				status Int32,
				body_bytes_sent Int64,
				http_referer String,
				http_user_agent String
			) ENGINE = MergeTree(date, (remote_addr, request, status), 8192);
		`)
	if err != nil {
		log.Fatal(err)
	}

	var (
		tx, _   = connect.Begin()
		stmt, _ = tx.Prepare("INSERT INTO access_logs (date, remote_addr, remote_user, time_local, request, status, body_bytes_sent, http_referer, http_user_agent) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)")
	)

	r, err := newLogReader(filename, `$remote_addr - $remote_user [$time_local] "$request" $status $body_bytes_sent "$http_referer" "$http_user_agent"`)
	if err != nil {
		log.Fatal(err)
	}


	//for i := 0; i < 10; i++ {
	for {
		rec, err := r.Read()
		if err == io.EOF {
			break
		}

		row := recToRow(rec)

		st, err := strconv.ParseInt(row[5], 10, 32)
		if err != nil {
			log.Fatal(err)
		}
		bbs, err := strconv.ParseInt(row[6], 10, 64)
		if err != nil {
			log.Fatal(err)
		}

		if _, err := stmt.Exec(row[0], row[1], row[2], row[3], row[4], st, bbs, row[7], row[8]); err != nil {
			log.Fatal(err)
		}
	}

	if err := tx.Commit(); err != nil {
		log.Fatal(err)
	}

	//rows, err := connect.Query("SELECT date, remote_addr, status FROM access_logs")
	//if err != nil {
	//	log.Fatal(err)
	//}
	//
	//for rows.Next() {
	//	var (
	//		date, remote_addr string
	//		status int32
	//	)
	//	if err := rows.Scan(&date, &remote_addr, &status); err != nil {
	//		log.Fatal(err)
	//	}
	//	log.Printf("date: %s, remote_addr: %s, status: %d", date, remote_addr, status)
	//}
	//
	//if _, err := connect.Exec("DROP TABLE access_logs"); err != nil {
	//	log.Fatal(err)
	//}
}

func newLogReader(filename, format string) (*gonx.Reader, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	return gonx.NewReader(f, format), nil
}

func logToCSV(filename string) {
	wf, err := os.Create(fmt.Sprintf("%s.csv", filename))
	if err != nil {
		log.Fatal(err)
	}

	r, err := newLogReader(filename, `$remote_addr - $remote_user [$time_local] "$request" $status $body_bytes_sent "$http_referer" "$http_user_agent"`)
	if err != nil {
		log.Fatal(err)
	}
	w := csv.NewWriter(wf)

	header := []string{"date", "remote_addr", "remote_user", "time_local", "request", "status", "body_bytes_sent", "http_referer", "http_user_agent"}

	if err = w.Write(header); err != nil {
		log.Fatal(err)
	}

	for {
		rec, err := r.Read()
		if err == io.EOF {
			break
		}

		row := recToRow(rec)

		//fmt.Printf("Parsed entry: %+v\n", rec)

		if err = w.Write(row); err != nil {
			log.Fatal(err)
		}
	}

	w.Flush()

	if err := w.Error(); err != nil {
		log.Fatal(err)
	}
}

func recToRow(rec *gonx.Entry) []string {
	var row []string

	remoteAddr, err := rec.Field("remote_addr")
	if err != nil {
		log.Fatal(err)
	}
	remoteUser, err := rec.Field("remote_user")
	if err != nil {
		log.Fatal(err)
	}
	timeLocalStr, err := rec.Field("time_local")
	if err != nil {
		log.Fatal(err)
	}
	request, err := rec.Field("request")
	if err != nil {
		log.Fatal(err)
	}
	status, err := rec.Field("status")
	if err != nil {
		log.Fatal(err)
	}
	//fmt.Printf("!!!!!!!!!!!!!!!!!!!!!!!!!!!!! status: %s\n", status)
	bodyBytesSent, err := rec.Field("body_bytes_sent")
	if err != nil {
		log.Fatal(err)
	}
	httpReferer, err := rec.Field("http_referer")
	if err != nil {
		log.Fatal(err)
	}
	httpUserAgent, err := rec.Field("http_user_agent")
	if err != nil {
		log.Fatal(err)
	}
	timeLocal, err := time.Parse("02/Jan/2006:15:04:05", strings.Split(timeLocalStr, " ")[0])
	if err != nil {
		log.Fatal("blblblb", err)
	}
	year, month, day := timeLocal.Date()
	date := fmt.Sprintf("%d-%02d-%02d", year, month, day)
	timeL := timeLocal.Format("2006-01-02 15:04:05")

	row = append(row, date, remoteAddr, remoteUser, timeL, request, status, bodyBytesSent, httpReferer, httpUserAgent)

	return row
}

func cleanLog(filename string) {
	f, err := os.Open(filename)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	wf, err := os.Create(fmt.Sprintf("%s.clean", filename))
	if err != nil {
		log.Fatal(err)
	}
	defer wf.Close()
	w := bufio.NewWriter(wf)

	re := regexp.MustCompile(`^.*?[^\d]: `)

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		l := scanner.Text()
		//fmt.Println(re.ReplaceAllString(l, ""))
		fmt.Fprintln(w, re.ReplaceAllString(l, ""))
	}

	err = w.Flush()
	if err != nil {
		log.Fatal(err)
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
}