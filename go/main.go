package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/redis/go-redis/v9"
	"gopkg.in/yaml.v2"
)

// type data_s struct {
// 	cnt string `json:"cnt"`
// }

// ******
var Db *sqlx.DB
var rdb *redis.Client
var cnt_r uint64

//******

type conf struct {
	Redis struct {
		Addr     string `yaml:"addr"`
		Password string `yaml:"password"`
		Port     string `yaml:"port"`
	} `yaml:"redis"`
	Mysql struct {
		Username string `yaml:"username"`
		Password string `yaml:"password"`
		Host     string `yaml:"host"`
		Port     string `yaml:"port"`
		Dbname   string `yaml:"dbname"`
	} `yaml:"mysql"`
}

var configData *conf

// ******
func loadConfig() error {
	config := new(conf)
	yamlFile, err := os.ReadFile("/data/config.yaml")
	if err != nil {
		log.Printf("读取配置文件失败 #%v", err)
	}
	err = yaml.Unmarshal(yamlFile, &config)
	if err != nil {
		log.Printf("读取配置文件失败 #%v", err)
	}
	fmt.Println("read config.yaml ok")
	configData = config

	return nil
}

// ******
func main() {
	loadConfig()
	// fmt.Printf("name: %v\n", configData.Mysql.Username)
	initMysql()
	initRedis()
	initCnt()
	// 定时同步
	go syncCnt()
	fmt.Println("start")

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	})
	http.HandleFunc("/api/cnt", handler)
	http.ListenAndServe(":9988", nil)
}

func initMysql() {
	fmt.Println("initMysql")
	//配置MySQL连接参数
	username := configData.Mysql.Username //用户名
	password := configData.Mysql.Password //密码
	host := configData.Mysql.Host         //数据库地址
	port := configData.Mysql.Port         //数据库端口
	Dbname := configData.Mysql.Dbname     //数据库名
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8&parseTime=True&loc=Local", username, password, host, port, Dbname)
	fmt.Println("mysql: " + dsn)
	database, err := sqlx.Open("mysql", dsn)
	if err != nil {
		fmt.Println("open mysql failed,", err)
		return
	}

	Db = database

	query := "select 1 from num"
	_, err = Db.Query(query)
	if err == sql.ErrNoRows {
		fmt.Println("sql.ErrNoRows error,", err)
	} else if err != nil {
		fmt.Println("查找失败, ", err)
		_, err = Db.Exec("CREATE TABLE `num` (`cnt` bigint(20) unsigned DEFAULT '0') ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci")
		if err != nil {
			fmt.Println("create table failed,", err)
		}
		fmt.Println("successfully create table")
	}
	_, err = Db.Query("INSERT INTO num VALUES(0)")
	if err != nil {
		fmt.Println("插入失败, ", err)
	}

	var rows []string
	err = Db.Select(&rows, "select cnt from num")
	if err != nil {
		fmt.Println("select failed, ", err)
	}
	fmt.Println("now cnt in mysql:")
	fmt.Println(rows[0])
}

func initRedis() (err error) {
	fmt.Println("initRedis")
	fmt.Println("redis: " + configData.Redis.Addr + ":" + configData.Redis.Port)
	rdb = redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%s", configData.Redis.Addr, configData.Redis.Port),
		Password: configData.Redis.Password, // no password set
		DB:       0,                         // use default DB
	})
	if err != nil {
		return err
	}
	fmt.Println("Redis ok.")
	return nil
}

// // 初始化cnt
func initCnt() {
	fmt.Println("initCnt")

	var rows []string
	err := Db.Select(&rows, "select cnt from num")
	if err != nil {
		fmt.Println("exec failed, ", err)
	}
	cnt64, err := strconv.Atoi(rows[0])
	cntu64 := uint64(cnt64) //uint64转换

	ctx := context.Background()
	//从mysql中取值，到redis
	rdb.Set(ctx, "cnt", cntu64, 0)
}

// HTTP请求处理
func handler(w http.ResponseWriter, r *http.Request) {
	ctx := context.Background()
	if r.Method == "GET" {
		// 从Redis获取cnt
		data := make(map[string]interface{}, 1)
		data["cnt"] = rdb.Get(ctx, "cnt").Val()

		result, err := json.MarshalIndent(data, "", " ")
		if err != nil {
			fmt.Println("err = ", err)
			return
		}
		w.Header().Set("Access-Control-Allow-Origin", "*")             //允许访问所有域
		w.Header().Add("Access-Control-Allow-Headers", "Content-Type") //header的类型
		w.Header().Set("content-type", "application/json")             //返回数据格式是json

		fmt.Fprintln(w, string(result))

	} else if r.Method == "POST" {
		// cnt + 1
		// var status string
		cntNew, err := rdb.Incr(ctx, "cnt").Result() //redis更新，自增+1
		if err != nil {
			fmt.Println("err = ", err)
			// status = err.Error()
			// } else {
			// 	 status = "ok"
		}
		data := make(map[string]interface{}, 1)
		// data["status"] = status
		data["cnt"] = cntNew

		result, err := json.MarshalIndent(data, "", " ")
		if err != nil {
			fmt.Println("err = ", err)
			return
		}
		w.Header().Set("Access-Control-Allow-Origin", "*")             //允许访问所有域
		w.Header().Add("Access-Control-Allow-Headers", "Content-Type") //header的类型
		w.Header().Set("content-type", "application/json")             //返回数据格式是json

		fmt.Fprintln(w, string(result))
		// w.Write([]byte("postOK_" + strconv.FormatUint(uint64(result), 10)))
	}
}

// 同步cnt到MySQL
func syncCnt() {
	fmt.Println("syncCnt")
	time.Sleep(1 * time.Minute)
	ctx := context.Background()
	for {
		cnt := rdb.Get(ctx, "cnt").Val()
		updatePre, err := Db.Prepare("update num SET cnt = ?")
		if err != nil {
			panic(err.Error())
		}
		updatePre.Exec(cnt)
		fmt.Println("syncCnt_update")
		time.Sleep(5 * time.Minute)
	}
}
