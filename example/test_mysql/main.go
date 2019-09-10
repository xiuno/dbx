package main

import (
	"database/sql"
	"fmt"
	"github.com/xiuno/dbx"
	"os"
	"time"
)

type User struct {
	Uid        int64     `db:"uid"`
	Gid        int64     `db:"gid"`
	Name       string    `db:"name"`
	Amount     float64   `db:"amount""`
	CreateDate time.Time `db:"createDate"`
}

func main() {

	var err error
	var db *dbx.DB

	// db, err = dbx.Open("mysql", "root:root@tcp(localhost)/test?parseTime=true&charset=utf8")
	db, err = dbx.Open("mysql", "root@tcp(localhost)/test?parseTime=true&charset=utf8")
	dbx.Check(err)
	defer db.Close()

	// db 输出信息设置
	db.Stdout = os.Stdout                       // 默认：将 db 产生的错误信息输出到标准输出
	db.Stderr = dbx.OpenFile("./db1_error.log") // 将 db 产生的错误信息输出到指定的文件
	// db.Stdout = ioutil.Discard // 默认：将 db 的输出信息重定向到"黑洞"（不输出执行的 SQL 语句等信息）

	// 参数设置
	db.SetMaxIdleConns(10)
	db.SetMaxOpenConns(10)
	//db.SetConnMaxLifetime(time.Second * 5)

	// 创建表
	_, err = db.Exec(`DROP TABLE IF EXISTS user;`)
	_, err = db.Exec(`CREATE TABLE user(
		uid        INT(11) PRIMARY KEY AUTO_INCREMENT,
		gid        INT(11) NOT NULL DEFAULT '0',
		name       TEXT             DEFAULT '',
		amount     DECIMAL(8,2)   DEFAULT '0.0',
		createDate DATETIME         DEFAULT CURRENT_TIMESTAMP
		);
	`)
	dbx.Check(err)

	// 开启缓存，可选项，一般只针对小表开启缓存，超过 10w 行，不建议开启！
	db.Bind("user", &User{}, true)
	db.EnableCache(true)

	// 插入一条
	u1 := &User{1, 1, "jet", 1.2, time.Now()}
	_, err = db.Table("user").Insert(u1)
	dbx.Check(err)

	// 读取一条
	u2 := &User{}
	err = db.Table("user").WherePK(1).One(u2)
	dbx.Check(err)
	fmt.Printf("%+v\n", u2)

	// 读取一条，判断是否存在
	err = db.Table("user").WherePK(1).One(u2)
	dbx.Check(err)
	if dbx.NoRows(err) {
		panic("not found.")
	}
	fmt.Printf("%+v\n", u2)

	// 日期
	err = db.Table("user").Where("createDate>?", time.Now().Add(-10 * time.Second).Format("2006-01-02 03:04:05")).One(u2)
	dbx.Check(err)
	fmt.Printf("%+v\n", u2)

	// 更新一条
	u2.Name = "jet.li"
	_, err = db.Table("user").Update(u2)
	dbx.Check(err)

	// Where 条件 + 更新
	_, err = db.Table("user").WhereM(dbx.M{{"uid", 1}, {"gid", 1}}).UpdateM(dbx.M{{"name", "jet.li"}})
	dbx.Check(err)

	// 删除一条
	_, err = db.Table("user").WherePK(1).Delete()
	dbx.Check(err)

	// 插入多条
	for i := int64(0); i < 5; i++ {
		u := &User{
			Uid:        i,
			Gid:        i,
			Name:       fmt.Sprintf("name-%v", i),
			CreateDate: time.Now(),
		}
		//time.Sleep(10 * time.Second)
		_, err := db.Table("user").Insert(u)
		if err != nil {
			//fmt.Printf("%v\n", err.Error())
			panic(err)
		}
	}

	// 获取多条
	userList := []*User{}

	// 获取多条无结果
	err = db.Table("user").Where("uid>?", 1000).All(&userList)
	if err != sql.ErrNoRows {
		panic(err)
	}

	err = db.Table("user").Where("uid>?", 1).All(&userList)
	dbx.Check(err)
	for _, u := range userList {
		fmt.Printf("%+v\n", u)
	}

	// 批量更新
	_, err = db.Table("user").Where("uid>?", 3).UpdateM(dbx.M{{"gid", 10}})
	dbx.Check(err)

	// 批量删除
	_, err = db.Table("user").Where("uid>?", 3).Delete()
	dbx.Check(err)

	// 总数
	n, err := db.Table("user").Where("uid>?", -1).Count()
	dbx.Check(err)
	fmt.Printf("count: %v\n", n)

	// 求和
	n, err = db.Table("user").Where("uid>?", -1).Sum("uid")
	dbx.Check(err)
	fmt.Printf("sum(uid): %v\n", n)

	// 求最大值
	n, err = db.Table("user").Where("uid>?", -1).Max("uid")
	dbx.Check(err)
	fmt.Printf("max(uid): %v\n", n)

	// 求最小值
	n, err = db.Table("user").Where("uid>?", -1).Min("uid")
	dbx.Check(err)
	fmt.Printf("min(uid): %v\n", n)

	// 自定义复杂 SQL 获取单条结果（原生）
	var uid int64
	err = db.QueryRow("SELECT uid FROM user WHERE uid=?", 2).Scan(&uid)
	dbx.Check(err)
	fmt.Printf("uid: %v\n", uid)
	db.Table("user").LoadCache() // 自定义需要手动刷新缓存

	// 自定义复杂 SQL 获取多条（原生）
	var name string
	rows, err := db.Query("SELECT `uid`, `name` FROM `user` WHERE 1 ORDER BY uid DESC")
	dbx.Check(err)
	rows.Close()
	for rows.Next() {
		rows.Scan(&uid, &name)
		fmt.Printf("uid: %v, name: %v\n", uid, name)
	}
	db.Table("user").LoadCache() // 自定义需要手动刷新缓存

	// 其他


	//userlist := []*User{}
	mp := db.Table("user").AllFromCache()
	fmt.Printf("mp.Len(): %v\n", mp.Len())

	//userlist := []*User{}
	n, err = db.Table("user").Count()
	fmt.Printf("count: %v\n", n)

	// gid+1
	u := &User{
		Uid:        300,
		Gid:        300,
		Name:       fmt.Sprintf("name-%v", 300),
		CreateDate: time.Now(),
	}

	db.Table("user").Replace(u)
	db.Table("user").WherePK(300).UpdateM(dbx.M{{"gid+", 1}})
	err = db.Table("user").WherePK(300).One(u1)
	dbx.Check(err)
	if u1.Gid != 301 {
		panic("gid error.")
	}


	db.Table("user").Delete()
	err = db.Table("user").One(u1)
	dbx.Check(err)
	if !dbx.NoRows(err) {
		panic("delete error.")
	}



	return
}
