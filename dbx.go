package dbx

import (
	"database/sql"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"reflect"
	"runtime/debug"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/xiuno/dbx/lib/syncmap"

	_ "github.com/go-sql-driver/mysql"
	"github.com/gocql/gocql"
	_ "github.com/mattn/go-sqlite3"
)

// sql action:
const (
	ACTION_SELECT_ONE int = iota
	ACTION_SELECT_ALL
	ACTION_UPDATE
	ACTION_UPDATE_M
	ACTION_DELETE
	ACTION_INSERT
	ACTION_INSERT_IGNORE
	ACTION_REPLACE
	ACTION_COUNT
	ACTION_SUM
)

const (
	DRIVER_MYSQL int = iota
	DRIVER_SQLITE
	DRIVER_CQL
)

//type UUID gocql.UUID

const KEY_SEP string = "-"

//type Time struct {
//	time.Time
//}
//func (t *Time)String() string {
//	return t.Format("2006-01-02 15:04:05")
//}

var ErrNoRows = sql.ErrNoRows

func IsDup(err error) bool {
	if err != nil && strings.Index(err.Error(), "Duplicate") != -1 {
		return true
	}
	return false
}

func NoRows(err error) bool {
	if err == sql.ErrNoRows {
		return true
	}
	return false
}

func Check(err error) {
	if err != nil && err != sql.ErrNoRows {
		panic(err)
	}
}

func Now() string {
	t := time.Now()
	return t.Format("2006-01-02 15:04:05")
}

// 错误处理，仅用于 dbx 类，对外部透明，对外部仅返回标准的 error 和 panic()
type dbxError struct {
	data string
}

func (e dbxError) Error() string {
	return e.data
}
func dbxErrorNew(s string, args ...interface{}) *dbxError {
	e := &dbxError{data: fmt.Sprintf(s, args...)}
	return e
}
func dbxErrorType(err interface{}) bool {
	_, ok := err.(*dbxError)
	return ok
}
func dbxErrorDefer(err *error, db *Query) {
	if err1 := recover(); err1 != nil {
		*err = fmt.Errorf("dbx panic(): %v", err1)
		//return
		if !dbxErrorType(err1) {
			db.ErrorLog((*err).Error())
			debug.PrintStack()
			os.Exit(0)
		}
	}
}

//func dbxPanic(s string, args... interface{}) {
//	panic(dbxErrorNew(s, args...))
//}

type M []Map

type Map struct {
	Key   string
	Value interface{}
}

func (m M) toKeysValues() ([]string, []interface{}) {
	l := len(m)
	keys := make([]string, l)
	values := make([]interface{}, l)
	for k, v := range m {
		keys[k] = v.Key
		values[k] = v.Value
	}
	return keys, values
}

type Col struct {
	ColName     string // 列名: id
	FieldName   string // 结构体中的名字：Id
	FieldPos    []int  // 在结构体中的位置，支持嵌套 [1,0,-1,-1,-1]
	FieldStruct reflect.StructField
}

type ColFieldMap struct {
	fieldMap map[string]int
	fieldArr []string
	colMap   map[string]int
	colArr   []string
	cols     []*Col
}

func NewColFieldMap() *ColFieldMap {
	c := &ColFieldMap{}
	c.fieldMap = map[string]int{}
	c.colMap = map[string]int{}
	c.cols = []*Col{}
	return c

}
func (c *ColFieldMap) Add(col *Col) {
	if c.Exists(col.FieldName) {
		return
	}
	c.cols = append(c.cols, col)
	n := len(c.cols) - 1
	c.fieldMap[col.FieldName] = n
	c.fieldArr = append(c.fieldArr, col.FieldName)

	if col.ColName != "" {
		c.colMap[col.ColName] = n
		c.colArr = append(c.colArr, col.ColName)
	}
}

func (c *ColFieldMap) GetByColName(colName string) *Col {
	i, ok := c.colMap[colName]
	if !ok {
		return nil
	}
	return c.cols[i]
}

func (c *ColFieldMap) GetByFieldName(fieldName string) *Col {
	i, ok := c.fieldMap[fieldName]
	if !ok {
		return nil
	}
	return c.cols[i]
}

func (c *ColFieldMap) Exists(key string) bool {
	_, ok := c.fieldMap[key]
	return ok
}

type TableStruct struct {
	ColFieldMap   *ColFieldMap
	PrimaryKey    []string
	PrimaryKeyPos [][]int
	AutoIncrement string
	Type          reflect.Type
	EnableCache   bool
}

// pointerType 必须为约定值 &struct
func NewTableStruct(db *DB, tableName string, pointerType reflect.Type) (*TableStruct) {
	if pointerType.Kind() != reflect.Ptr {
		pointerType = reflect.New(pointerType).Type()
	}
	colFieldMap := NewColFieldMap()
	struct_fields_range_do(colFieldMap, pointerType, []int{})

	t := &TableStruct{}
	t.ColFieldMap = colFieldMap
	t.Type = pointerType
	t.PrimaryKey, t.AutoIncrement = get_table_info(db, tableName)
	t.EnableCache = false

	// 保存主键的位置
	t.PrimaryKeyPos = make([][]int, 0)
	for _, colName := range t.PrimaryKey {
		n := colFieldMap.colMap[colName]
		col := colFieldMap.cols[n]
		t.PrimaryKeyPos = append(t.PrimaryKeyPos, col.FieldPos)
	}
	return t
}

type DB struct {
	*sql.DB

	CQLSession *gocql.Session
	CQLMeta    *gocql.KeyspaceMetadata
	DriverType int
	DbName     string
	Stdout     io.Writer
	Stderr     io.Writer

	// todo: 按照行缓存数据，只缓存主键条件的查询
	tableStruct      map[string]*TableStruct
	tableData        map[string]*syncmap.Map
	tableEnableCache bool

	readOnly bool // 只读模式，禁止写，防止出错。
	isCQL bool
}

type Query struct {
	*DB
	table  string
	fields []string // SELECT

	primaryKeyStr string
	primaryArgs   []interface{} // 主键的值

	where     string
	whereArgs []interface{}
	whereM    M

	orderBy M

	limitStart int64
	limitEnd   int64

	updateFields []string
	updateOps    []string
	updateArgs   []interface{} // 存储参数
}

func OpenFile(filePath string) *os.File {
	fp, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		panic(err)
	}
	return fp
}

func NewCQLSession(hosts []string, keySpace string) (*gocql.Session, error) {
	cluster := gocql.NewCluster(hosts...) //  "192.168.0.129:9042"
	cluster.Keyspace = keySpace           // dbname "mycas"
	cluster.Consistency = gocql.Consistency(1)
	cluster.NumConns = 3	// 并发连接数，不要开太多，否则连接时间很长！
	//cluster.ConnectTimeout = 600 * time.Second
	//cluster.Timeout = 600 * time.Second
	return cluster.CreateSession()
}

func Open(driverName string, dataSourceNames ... string) (*DB, error) {
	if driverName != "cql" {
		dataSourceName := dataSourceNames[0]
		dbName := ""
		if driverName == "mysql" {
			conf, err := mysql.ParseDSN(dataSourceName)
			if err != nil {
				return nil, err
			}
			dbName = conf.DBName
		}
		db, err := sql.Open(driverName, dataSourceName)
		if err != nil {
			return nil, err
		}
		var driverType int
		if driverName == "mysql" {
			driverType = DRIVER_MYSQL
		} else if driverName == "sqlite" || driverName == "sqlite3" {
			driverType = DRIVER_SQLITE
		} else {
			driverType = DRIVER_MYSQL
		}
		//cacheTable := &map[string][]string{}
		//cacheData := &map[string]*map[string]interface{}{}
		// "root@tcp(localhost)/test?parseTime=true&charset=utf8"
		return &DB{
			DB:               db,
			CQLSession:       nil,
			DriverType:       driverType,
			DbName:           dbName,
			Stdout:           ioutil.Discard,
			Stderr:           os.Stderr,
			tableStruct:      make(map[string]*TableStruct),
			tableData:        make(map[string]*syncmap.Map), // 第一级的 map 会在启动的时候初始化好，第二级的使用安全 map
			tableEnableCache: false,
			isCQL: false,
		}, err
	} else {
		// 支持多个 dataSourceName
		hosts := make([]string, 0)
		dbName := ""
		for _, v := range dataSourceNames {
			conf, err := mysql.ParseDSN(v)
			if err != nil {
				continue
			}
			hosts = append(hosts, conf.Addr)
			dbName = conf.DBName
		}
		cqlSession, err := NewCQLSession(hosts, dbName)
		if err != nil {
			return nil, err
		}
		meta, err := cqlSession.KeyspaceMetadata(dbName)
		return &DB{
			DB:               nil,
			CQLSession:       cqlSession,
			CQLMeta:          meta,
			DriverType:       DRIVER_CQL,
			DbName:           dbName,
			Stdout:           ioutil.Discard,
			Stderr:           os.Stderr,
			tableStruct:      make(map[string]*TableStruct),
			tableData:        make(map[string]*syncmap.Map), // 第一级的 map 会在启动的时候初始化好，第二级的使用安全 map
			tableEnableCache: false,
			isCQL: true,
		}, err
	}
}

func sql2str(sqlstr string, args ...interface{}) string {
	sqlstr = strings.Replace(sqlstr, "?", "%v", -1)
	sql1 := fmt.Sprintf(sqlstr, args...)
	return sql1
}

func (db *DB) Close() (error) {
	if db.DriverType == DRIVER_CQL {
		db.CQLSession.Close()
		return nil
	} else {
		return db.DB.Close()
	}
}

func (db *DB) Log(s string, args ...interface{}) {
	if db.Stdout == nil || db.Stdout == ioutil.Discard {
		return
	}
	str := fmt.Sprintf(s, args...)
	fmt.Fprintf(db.Stdout, "[%v] %v\n", Now(), str)
}
func (db *DB) LogSQL(s string, args ...interface{}) {
	str := sql2str(s, args...)
	db.Log(str)
}
func (db *DB) ErrorLog(s string, args ...interface{}) {
	if db.Stderr == nil || db.Stderr == ioutil.Discard {
		return
	}
	str := fmt.Sprintf(s, args...)
	fmt.Fprintf(db.Stderr, "[%v] %v\n", Now(), str)
}

func (db *DB) ErrorSQL(s string, sql1 string, args ...interface{}) {
	if db.Stderr == nil || db.Stderr == ioutil.Discard {
		return
	}
	str := fmt.Sprintf(s)
	sql2 := sql2str(sql1, args...)
	fmt.Fprintf(db.Stderr, "[%v] %v %v\n", Now(), sql2, str)
}

func (db *DB) Panic(s string, args ...interface{}) {
	db.ErrorLog(s, args...)
	panic(dbxErrorNew(s, args...))
}

// ifc 如果不是指针类型，则 new 出指针类型，方便使用 type / &type
func (db *DB) Bind(tableName string, ifc interface{}, enableCache bool) {
	t := reflect.TypeOf(ifc)
	if t.Kind() == reflect.Struct {
		t = reflect.New(t).Type()
	}
	tableStruct, ok := db.tableStruct[tableName]
	if !ok {
		tableStruct := NewTableStruct(db, tableName, t)
		tableStruct.EnableCache = enableCache
		db.tableStruct[tableName] = tableStruct
	} else {
		tableStruct.EnableCache = enableCache
	}
}

func (db *DB) SetReadOnly(b bool) {
	db.readOnly = b
}

func (db *DB) EnableCache(b bool) {
	db.tableEnableCache = b
	if b == true && len(db.tableData) == 0 {
		db.LoadCache()
	}
}

func (db *DB) loadTableCache(tableName string) {
	tableStruct := db.tableStruct[tableName]
	list := reflect_make_slice_pointer(tableStruct.Type)
	err := db.Table(tableName).All(list)
	if err != nil && err != sql.ErrNoRows {
		db.Panic(err.Error())
	}

	listValue := reflect.ValueOf(list).Elem()

	//mp := &syncmap.Map{}
	mp := new(syncmap.Map)
	for i := 0; i < listValue.Len(); i++ {
		row := listValue.Index(i)
		pkKey := get_pk_keys(tableStruct, row.Elem())
		mp.Store(pkKey, row.Interface())
	}
	db.tableData[tableName] = mp
}

func (db *DB) LoadCache() {
	if db.tableEnableCache == false {
		return
	}
	for tableName, tableStruct := range db.tableStruct {
		if tableStruct.EnableCache == false {
			continue
		}
		db.loadTableCache(tableName)
	}
}

func (db *DB) Table(name string) *Query {
	q := &Query{
		DB:          db,
		table:       name,
		fields:      []string{},
		whereArgs:   []interface{}{},
		primaryArgs: []interface{}{},
		updateArgs:  []interface{}{},
		orderBy:     M{},
	}
	return q
}

func (q *Query) Bind(ifc interface{}, enableCache bool) {
	tableName := q.table
	t := reflect.TypeOf(ifc)
	if t.Kind() == reflect.Struct {
		t = reflect.New(t).Type()
	}
	tableStruct, ok := q.tableStruct[tableName]
	if !ok {
		tableStruct := NewTableStruct(q.DB, tableName, t)
		tableStruct.EnableCache = enableCache
		q.tableStruct[tableName] = tableStruct
	} else {
		tableStruct.EnableCache = enableCache
	}
}

func (q *Query) LoadCache() {
	if q.tableEnableCache == false {
		return
	}
	tableName := q.table
	tableStruct := q.tableStruct[tableName]
	if tableStruct.EnableCache == false {
		return
	}
	q.loadTableCache(q.table)
}

func (q *Query) AllFromCache() *syncmap.Map {
	v, ok := q.tableData[q.table]
	if ok {
		return v
	} else {
		return nil
	}
}

func (q *Query) Fields(fields ...string) *Query {
	q.fields = fields
	return q
}

/*
	dbx.M{{"uid", 1}, {"gid", -1}}
*/
//func (q *Query) OrderBy(m M) *Query {
//	q.orderBy = m
//	return q
//}
//
//func args_time_format(args []interface{}) {
//	for k, v := range args {
//		if _, ok := v.(time.Time); ok {
//			args[k] = v.(time.Time).Format("2006-01-02 15:04:05")
//		}
//	}
//}
//
//func FormatTime(t time.Time) string {
//	return t.Format("2006-01-02 15:04:05")
//}

func (q *Query) Where(str string, args ...interface{}) *Query {
	//args_time_format(args)
	if q.where == "" {
		q.where = str
	} else {
		q.where += " AND " + str
	}
	q.whereArgs = append(q.whereArgs, args...)
	return q
}

func (q *Query) And(str string, args ...interface{}) *Query {
	return q.Where(str, args...)
}

func (q *Query) Or(str string, args ...interface{}) *Query {
	if q.where == "" {
		q.where = str
	} else {
		q.where += " OR " + str
	}
	q.whereArgs = append(q.whereArgs, args...)
	return q
}

func (q *Query) WhereM(m M) *Query {
	q.whereM = append(q.whereM, m...)
	/*
		l := len(m)
		whereCols := make([]string, l)
		args := make([]interface{}, l)
		for i, mp := range m {
			args[i] = mp.Value
			whereCols[i] = mp.Key
		}
		q.whereArgs = args
		q.where = arr_to_sql_add(whereCols, "=?", " AND ")
	*/
	return q
}

func (q *Query) WherePK(args ...interface{}) *Query {
	q.primaryArgs = args
	str := get_key_str_by_args(args...)
	q.primaryKeyStr = str
	return q
}

// 合并 where 条件
//func (q *Query) wherePKToKey() (string) {
//	str := ""
//	for _, v := range args {
//		str = fmt.Sprintf("%v-", v)
//	}
//	str = strings.TrimRight(str, "-")
//	q.primaryKeyStr = str
//}

func (q *Query) whereToSQL(tableStruct *TableStruct) (where string, args []interface{}, allowFiltering string) {

	// 主键优先级最高，独占
	if len(q.primaryArgs) > 0 {
		where = " WHERE " + arr_to_sql_add(tableStruct.PrimaryKey, "=?", " AND ", q.isCQL)
		args = q.primaryArgs
		return
	}

	// 合并所有的 where + whereM 条件
	where, args = q.whereToSQLDo()

	// Cassandra 非主键，需要增加关键字！
	if q.DriverType == DRIVER_CQL && len(q.primaryArgs) == 0 && where != "" {
		//ALLOW FILTERING
		allowFiltering = " ALLOW FILTERING"
	}
	return
}

func (q *Query) whereToSQLDo() (where string, args []interface{}) {
	// 合并所有的 where + whereM 条件
	where = q.where
	args = q.whereArgs
	if len(q.whereM) > 0 {
		colNames, args2 := q.whereM.toKeysValues()
		whereAdd := arr_to_sql_add(colNames, "=?", " AND ", q.isCQL)
		if where == "" {
			where = whereAdd
		} else {
			where += " AND " + whereAdd
		}
		args = append(args, args2...)
	}

	if where != "" {
		where = " WHERE " + where
	}
	return
}

func (q *Query) Sort(colName string, order int) *Query {
	q.orderBy = append(q.orderBy, Map{colName, order})
	return q
}

func (q *Query) SortM(m M) *Query {
	for _, v := range m {
		q.orderBy = append(q.orderBy, Map{v.Key, v.Value})
	}
	return q
}

/*
	Limit(10)
	Limit(0, 10) // Cassandra 不支持
	// Cassandra 只能"下一页"的方式进行翻页，默认按照主键排序
	SELECT * FROM user WHERE token(uid)>token(300) LIMIT 1;
	SELECT * FROM user WHERE token(id) > token(xx-xx-xx-xx-xx) AND regdate >= '2019-01-01' LIMIT 10  ALLOW FILTERING;
*/
func (q *Query) Limit(limitStart int64, limitEnds ...int64) *Query {
	limitEnd := int64(0)
	if len(limitEnds) > 0 {
		limitEnd = limitEnds[0]
	}
	q.limitStart = limitStart
	q.limitEnd = limitEnd
	return q
}

func (q *Query) orderByToSQL() string {
	sqlAdd := ""
	for _, m := range q.orderBy {
		k := m.Key
		v, ok := m.Value.(int)
		if !ok {
			sqlAdd += fmt.Sprintf("%v %v AND", k, "ASC")
			continue
		}
		if v == 1 {
			sqlAdd += fmt.Sprintf("%v %v AND", k, "ASC")
		} else if v == -1 {
			sqlAdd += fmt.Sprintf("%v %v AND", k, "DESC")
		}
	}
	return strings.TrimRight(sqlAdd, " AND")
}

// 将当前条件转化为 SQL 语句
func (q *Query) toSQL(tableStruct *TableStruct, action int, rvalues ...reflect.Value) (sql1 string, args []interface{}) {
	fields := "*"
	where := ""
	orderBy := ""
	limit := ""
	if len(q.fields) > 0 {
		fields = strings.Join(q.fields, ",")
	}

	var allowFiltering string
	where, args, allowFiltering = q.whereToSQL(tableStruct)

	if len(q.orderBy) > 0 {
		orderBy = " ORDER BY " + q.orderByToSQL()
	}
	if q.limitStart != 0 || q.limitEnd != 0 {
		if q.limitEnd == 0 {
			limit = fmt.Sprintf(" LIMIT %v", q.limitStart)
		} else {
			limit = fmt.Sprintf(" LIMIT %v,%v", q.limitStart, q.limitEnd)
		}
	}
	switch action {
	case ACTION_SELECT_ONE:
		limit = " LIMIT 1"
		sql1 = fmt.Sprintf("SELECT %v FROM %v%v%v%v%v", fields, q.table, where, orderBy, limit, allowFiltering)
	case ACTION_SELECT_ALL:
		sql1 = fmt.Sprintf("SELECT %v FROM %v%v%v%v%v", fields, q.table, where, orderBy, limit, allowFiltering)
	case ACTION_UPDATE:
		if q.DriverType == DRIVER_MYSQL {
			limit = " LIMIT 1"
		}

		var updateSets []interface{}
		updateSets, pkArgs, _ := struct_value_to_args(tableStruct, rvalues[0], true, true, q.isCQL)

		// todo: 去掉主键的更新
		colNames := array_sub(tableStruct.ColFieldMap.colArr, tableStruct.PrimaryKey)
		updateFields := arr_to_sql_add(colNames, "=?", ",", q.isCQL)
		if where == "" {
			where = " WHERE " + arr_to_sql_add(tableStruct.PrimaryKey, "=?", " AND ", q.isCQL)
			args = append(args, pkArgs...)
		}
		sql1 = fmt.Sprintf("UPDATE %v SET %v%v%v", q.table, updateFields, where, limit)
		args = append(updateSets, args...)
	case ACTION_UPDATE_M:
		if q.DriverType == DRIVER_SQLITE {
			limit = ""
		}
		colNames := arr_to_sql_add_update(q.updateFields, q.updateOps, q.isCQL)
		sql1 = fmt.Sprintf("UPDATE %v SET %v%v%v", q.table, colNames, where, limit) // UPDATE 不支持 ALLOW FILTERING
		args = append(q.updateArgs, args...)
	case ACTION_DELETE:
		if q.DriverType == DRIVER_SQLITE {
			limit = ""
		}
		sql1 = fmt.Sprintf("DELETE FROM %v%v%v%v", q.table, where, limit, allowFiltering)
	case ACTION_INSERT:
		uncludes := []string{tableStruct.AutoIncrement}
		colNames := array_sub(tableStruct.ColFieldMap.colArr, uncludes)
		fields := arr_to_sql_add(colNames, "", ",", q.isCQL)
		values := strings.TrimRight(strings.Repeat("?,", len(colNames)), ",")
		sql1 = fmt.Sprintf("INSERT INTO %v (%v) VALUES (%v)", q.table, fields, values)
		args, _, _ = struct_value_to_args(tableStruct, rvalues[0], true, false, q.isCQL)
	case ACTION_INSERT_IGNORE:
		// copy from ACTION_INSERT
		uncludes := []string{tableStruct.AutoIncrement}
		colNames := array_sub(tableStruct.ColFieldMap.colArr, uncludes)
		fields := arr_to_sql_add(colNames, "", ",", q.isCQL)
		values := strings.TrimRight(strings.Repeat("?,", len(colNames)), ",")
		if q.DriverType == DRIVER_MYSQL {
			sql1 = fmt.Sprintf("INSERT IGNORE INTO %v (%v) VALUES (%v)", q.table, fields, values)
		} else if q.DriverType == DRIVER_SQLITE {
			sql1 = fmt.Sprintf("INSERT OR IGNORE INTO %v (%v) VALUES (%v)", q.table, fields, values)
		}
		args, _, _ = struct_value_to_args(tableStruct, rvalues[0], true, false, q.isCQL)
		// copy end
	case ACTION_REPLACE:
		tableStruct := q.tableStruct[q.table]
		fields := arr_to_sql_add(tableStruct.ColFieldMap.colArr, "", ",", q.isCQL)
		values := strings.TrimRight(strings.Repeat("?,", len(tableStruct.ColFieldMap.colArr)), ",")
		sql1 = fmt.Sprintf("REPLACE INTO %v (%v) VALUES (%v)", q.table, fields, values)
		args, _, _ = struct_value_to_args(tableStruct, rvalues[0], false, false, q.isCQL)
	case ACTION_COUNT:
		sql1 = fmt.Sprintf("SELECT COUNT(*) FROM %v%v%v", fields, q.table, where, allowFiltering)
	case ACTION_SUM:
	}
	return
}

func (q *Query) One(arrIfc interface{}) (err error) {
	defer dbxErrorDefer(&err, q)
	arrType := reflect.TypeOf(arrIfc)
	arrValue := reflect.ValueOf(arrIfc)
	if arrType.Kind() != reflect.Ptr {
		errStr := fmt.Sprintf("must pass a struct pointer: %v", arrType.Kind())
		//panic(dbxErrorNew(errStr))
		q.Panic(errStr)
	}

	arr := arrType.Elem() // 求一级指针，&arr -> arr
	if arr.Kind() != reflect.Struct {
		errStr := fmt.Sprintf("must pass a struct pointer: %v", arr.Kind())
		//panic(dbxErrorNew(errStr))
		q.Panic(errStr)
	}

	// 如果没有 Bind() ，这里就会执行下去，从缓存里读表结构，不用每次都反射，提高效率
	tableStruct := q.getTableStruct(arrType)

	// 判断是否开启了缓存
	if q.tableEnableCache && tableStruct.EnableCache && len(q.primaryArgs) > 0 {
		if len(q.primaryKeyStr) != 0 {
			mp, ok := q.tableData[q.table]
			if !ok {
				errStr := fmt.Sprintf("q.tableData[q.table]: key %v does not exists.", q.table)
				q.ErrorLog(errStr)
				return errors.New(errStr)
			}
			ifc, ok := mp.Load(q.primaryKeyStr)
			if ok {
				arrValue.Elem().Set(reflect.ValueOf(ifc).Elem())
				return nil
			} else {
				// 只要开启 cache，必须终止！提高速度！
				return ErrNoRows
			}
		} else {
			return ErrNoRows
		}
	}

	sql1, args := q.toSQL(tableStruct, ACTION_SELECT_ONE)
	arrValue, err = q.get_row_by_sql(tableStruct, sql1, args...)
	return
}

func (q *Query) SQLQuery(sql1 string, args ... interface{}) (rows *sql.Rows, err error) {
	var stmt *sql.Stmt
	stmt, err = q.Prepare(sql1)
	if err != nil {
		q.ErrorSQL(err.Error(), sql1, args...)
		return
	}
	defer stmt.Close()
	rows, err = stmt.Query(args...)
	q.LogSQL(sql1, args...)
	if err != nil {
		q.ErrorSQL(err.Error(), sql1, args...)
	}
	return
}

func (q *Query) CQLQuery(sql1 string, args ... interface{}) (rows *gocql.Iter, err error) {
	rows = q.CQLSession.Query(sql1, args...).Iter()
	if rows == nil {
		q.ErrorSQL(err.Error(), sql1, args...)
		return
	}
	q.LogSQL(sql1, args...)
	//defer iter.Close()
	return
}

func (q *Query) QueryRowScanX(sql1 string, args ... interface{}) (n int64, err error) {
	if q.DriverType == DRIVER_CQL {
		var rows *gocql.Iter
		rows, err = q.CQLQuery(sql1, args...)
		if err != nil || rows == nil {
			return
		}
		rows.Scan(&n)
		return
	} else {
		var n2 sql.NullInt64
		err = q.QueryRow(sql1, args...).Scan(&n2)
		q.LogSQL(sql1, args...)
		if err != nil {
			q.ErrorSQL(err.Error(), sql1, args...)
			return
		}
		n = n2.Int64
		return
	}
}

func (q *Query) All(arrListIfc interface{}) (err error) {
	defer dbxErrorDefer(&err, q)
	arrListType := reflect.TypeOf(arrListIfc)
	if arrListType.Kind() != reflect.Ptr {
		q.Panic("must pass a slice pointer: %v", arrListType.Kind())
	}

	arrlist := arrListType.Elem() // 求一级指针，&arrlist -> arrlist
	if arrlist.Kind() != reflect.Slice {
		q.Panic("must pass a slice pointer: %v", arrListType.Kind())
	}

	arrType := arrlist.Elem() // &struct
	arrIsPtr := (arrType.Kind() == reflect.Ptr)

	arrListValue := reflect.ValueOf(arrListIfc).Elem()
	// 如果没有 Bind() ，这里就会执行下去，从缓存里读表结构，不用每次都反射，提高效率
	tableStruct := q.getTableStruct(arrType)

	// 判断是否为 whereM
	sql1, args := q.toSQL(tableStruct, ACTION_SELECT_ALL)
	if !q.isCQL {
		var rows *sql.Rows
		rows, err = q.SQLQuery(sql1, args...)
		if err != nil || rows == nil {
			return
		}
		defer rows.Close()
		err = rows_to_arr_list(arrListValue, rows, tableStruct, arrIsPtr) // 这个错误要保留
	} else {
		var rows *gocql.Iter
		rows, err = q.CQLQuery(sql1, args...)
		if err != nil || rows == nil {
			return
		}
		defer rows.Close()
		err = cql_rows_to_arr_list(&arrListValue, rows, tableStruct, arrIsPtr) // 这个错误要保留
	}
	return
}

// Cassandra Count() 可能会超时，可以通过 COPY tablename TO '/dev/null' 来查看行数
func (q *Query) Count() (n int64, err error) {
	defer dbxErrorDefer(&err, q)

	// 判断 WHERE 条件是否为空
	if q.tableEnableCache {
		tableStruct := q.getTableStruct()
		if tableStruct.EnableCache && q.where == "" && len(q.whereM) == 0 {
			return q.tableData[q.table].Len(), nil
		}
	}
	q.Fields("COUNT(*)")
	sql1, args := q.toSQL(nil, ACTION_SELECT_ONE)
	n, err = q.QueryRowScanX(sql1, args...)
	return
}

// 针对某一列
func (q *Query) Sum(colName string) (n int64, err error) {
	defer dbxErrorDefer(&err, q)
	q.Fields("SUM(" + colName + ")")
	sql1, args := q.toSQL(nil, ACTION_SELECT_ONE)
	n, err = q.QueryRowScanX(sql1, args...)
	return
}

// 针对某一列
func (q *Query) Max(colName string) (n int64, err error) {
	defer dbxErrorDefer(&err, q)
	q.Fields("MAX(" + colName + ")")
	sql1, args := q.toSQL(nil, ACTION_SELECT_ONE)
	n, err = q.QueryRowScanX(sql1, args...)
	return
}

// 针对某一列
func (q *Query) Min(colName string) (n int64, err error) {
	defer dbxErrorDefer(&err, q)
	q.Fields("MIN(" + colName + ")")
	sql1, args := q.toSQL(nil, ACTION_SELECT_ONE)
	n, err = q.QueryRowScanX(sql1, args...)
	return
}

func (q *Query) Truncate() (err error) {
	defer dbxErrorDefer(&err, q)

	// 判断 WHERE 条件是否为空
	if q.tableEnableCache {
		q.tableData[q.table] = new(syncmap.Map)
	}

	sql1 := ""
	if q.DriverType == DRIVER_SQLITE {
		sql1 = "DELETE FROM " + q.table
	} else {
		sql1 = "TRUNCATE " + q.table
	}
	_, err = q.Exec(sql1)
	if err != nil {
		q.ErrorSQL(err.Error(), sql1)
	}
	// 清理缓存
	q.LoadCache()
	return
}

// arrType 必须为 &struct
func (q *Query) getTableStruct(arrTypes ...reflect.Type) (tableStruct *TableStruct) {
	if len(arrTypes) == 0 {
		tableStruct, ok := q.tableStruct[q.table]
		if !ok {
			return nil
			//panic(dbxErrorNew("q.tableStruct[q.table] does not exists:" + q.table))
		} else {
			return tableStruct
		}
	}
	arrType := arrTypes[0]
	if arrType.Kind() != reflect.Ptr {
		arrType = reflect.New(arrType).Type()
		//panic(dbxErrorNew("getTableStruct(arrType) expect type of &struct."))
	}

	var ok bool
	if q.tableStruct == nil && q.DB == nil {
		q.Panic("database link may be not initialized, q.DB == nil.")
		return
	}
	if q.tableStruct != nil {
		tableStruct, ok = q.tableStruct[q.table]
	}
	if !ok {
		tableStruct = NewTableStruct(q.DB, q.table, arrType)
		q.tableStruct[q.table] = tableStruct
	}
	return
}

// ifc 最好为 &struct
func (q *Query) Insert(ifc interface{}) (insertId int64, err error) {
	return q.insert_replace(ifc, false, false)
}

// ifc 最好为 &struct
func (q *Query) Replace(ifc interface{}) (insertId int64, err error) {
	if q.DriverType == DRIVER_CQL {
		tableStruct := q.getTableStruct()
		_, pkArgs, _ := struct_value_to_args(tableStruct, reflect.ValueOf(ifc), false, false, q.isCQL)
		_, err = q.WherePK(pkArgs...).Delete()
		if err != nil {
			return
		}
		q.insert_replace(ifc, false, false)
	} else {
		q.insert_replace(ifc, true, false)
	}
	return
}

// ifc 最好为 &struct
func (q *Query) InsertIgnore(ifc interface{}) (insertId int64, err error) {
	return q.insert_replace(ifc, false, true)
}

// 需要处理 cql uuid
func (q *Query) insert_replace(ifc interface{}, isReplace bool, ignore bool) (insertId int64, err error) {
	if q.readOnly {
		return
	}

	defer dbxErrorDefer(&err, q)

	ifc2 := ifc
	arrType := reflect.TypeOf(ifc)
	arrValue := reflect.ValueOf(ifc)
	ifcValueP := arrValue
	arrTypeElem := arrType
	if arrType.Kind() == reflect.Ptr {
		arrTypeElem = arrType.Elem()
	} else {
		ifcValueP = reflect.New(reflect.TypeOf(ifc))
		ifcValueP.Elem().Set(arrValue)
		ifc2 = ifcValueP.Interface()
	}

	if arrValue.Kind() == reflect.Ptr {
		arrValue = arrValue.Elem()
	}

	if arrTypeElem.Kind() != reflect.Struct {
		q.Panic("must pass a struct or struct pointer: %v", arrTypeElem.Kind())
	}

	// 如果没有 Bind() ，这里就会执行下去，从缓存里读表结构，不用每次都反射，提高效率
	var tableStruct *TableStruct
	tableStruct = q.getTableStruct(arrType)

	action := ACTION_INSERT
	if ignore {
		action = ACTION_INSERT_IGNORE
	} else if isReplace {
		action = ACTION_REPLACE
	}

	var sql1 string
	var args []interface{}
	sql1, args = q.toSQL(tableStruct, action, arrValue)

	// gocql.RandomUUID()

	if ignore && !q.isCQL {
		_, err = q.DB.Exec(sql1, args...)
		q.LogSQL(sql1, args...)
		if err != nil {
			errStr := err.Error()
			errStrLower := strings.ToLower(errStr)
			if !strings.Contains(errStrLower, "unique") && !strings.Contains(errStrLower, "duplicate") {
				q.ErrorSQL(errStr, sql1, args...)
			}
		}
		return
	}

	insertId, err = q.Exec(sql1, args...)
	if err != nil {
		return
	}

	// cache
	if !ignore && q.tableEnableCache && tableStruct.EnableCache {
		mp, ok := q.tableData[q.table]
		if !ok {
			errStr := fmt.Sprintf("q.tableData[q.table]: key %v does not exists.", q.table)
			q.ErrorLog(errStr)
			err = errors.New(errStr)
			return
		}

		// update auto_increment value
		if tableStruct.AutoIncrement != "" && !isReplace {
			n, ok := tableStruct.ColFieldMap.colMap[tableStruct.AutoIncrement]
			pos := tableStruct.ColFieldMap.cols[n].FieldPos
			if !ok {
				errStr := fmt.Sprintf("auto_increment not found: %v.%v", q.table, tableStruct.AutoIncrement)
				q.ErrorLog(errStr)
				err = errors.New(errStr)
				return
			}
			colValue := get_reflect_value_from_pos(ifcValueP, pos)

			set_value_to_ifc(colValue, insertId)

			//ifc2 = ifcValueP.Interface()
		}
		pkkey := get_pk_keys(tableStruct, arrValue)
		mp.Store(pkkey, ifc2)
	}

	return
}

// 根据主键更新一条数据
func (q *Query) Update(ifc interface{}) (affectedRows int64, err error) {
	if q.readOnly {
		return
	}

	defer dbxErrorDefer(&err, q)

	ifc2 := ifc
	arrType := reflect.TypeOf(ifc)
	arrValue := reflect.ValueOf(ifc)
	ifcValueP := arrValue
	arrTypeElem := arrType
	if arrType.Kind() == reflect.Ptr {
		arrTypeElem = arrType.Elem()
	} else {
		ifcValueP = reflect.New(reflect.TypeOf(ifc))
		ifcValueP.Elem().Set(arrValue)
		ifc2 = ifcValueP.Interface()
	}

	if arrValue.Kind() == reflect.Ptr {
		arrValue = arrValue.Elem()
	}

	if arrTypeElem.Kind() != reflect.Struct {
		errStr := fmt.Sprintf("must pass a struct or struct pointer: %v", arrTypeElem.Kind())
		q.ErrorLog(errStr)
		err = errors.New(errStr)
		return
	}

	// 如果没有 Bind() ，这里就会执行下去，从缓存里读表结构，不用每次都反射，提高效率
	var tableStruct *TableStruct
	tableStruct = q.getTableStruct(arrType)

	var sql1 string
	var args []interface{}
	sql1, args = q.toSQL(tableStruct, ACTION_UPDATE, arrValue)

	// todo: cql 不返回受到影响的行数
	affectedRows, err = q.Exec(sql1, args...)
	if err != nil {
		return
	}

	// cache
	if q.tableEnableCache && tableStruct.EnableCache {
		// 判断是否通过主键更新，如果是主键则只更新
		pkkey := get_pk_keys(tableStruct, arrValue)
		// todo: 修正为主键的值？还是报错？
		mp, ok := q.tableData[q.table]
		if !ok {
			errStr := fmt.Sprintf("q.tableData[q.table]: key %v does not exists.", q.table)
			q.ErrorLog(errStr)
			err = errors.New(errStr)
			return
		}
		//v, ok := mp.Load("1")
		//mp.Range(func(k interface{}, v interface{}) bool {
		//	fmt.Printf("%v: %v\n", k, v)
		//	return true
		//})
		//fmt.Printf("Len: %v, 1: %v", mp.Len(), v)
		mp.Store(pkkey, ifc2)
	}

	return
}

//// 根据主键更新最简单
//if isPK {
//	// 直接获取原来的值
//	if cacheOn {
//		// 获取一条数据，然后更新 cache
//		mp, ok := q.tableData[q.table];
//		if !ok {
//			errStr := fmt.Sprintf("q.tableData[q.table]: key %v does not exists.", q.table)
//			q.ErrorLog(errStr)
//			err = errors.New(errStr)
//			return
//		}
//		poses := make([][]int, len(updateFields))
//		for k, colName := range updateFields {
//			n, ok := tableStruct.ColFieldMap.colMap[colName]
//			if !ok {
//				q.ErrorLog("UpdateM() colNmae does not exists: " + colName)
//				continue
//			}
//			col := tableStruct.ColFieldMap.cols[n]
//			poses[k] = col.FieldPos
//		}
//
//		keystr := get_key_str_by_args(q.primaryArgs...)
//		oldRow, ok := mp.Load(keystr)
//		if ok && oldRow != nil {
//			for j, _ := range updateFields {
//				pos := poses[j]
//				var oldV reflect.Value
//				oldV = get_reflect_value_from_pos(reflect.ValueOf(oldRow).Elem(), pos)
//				if updateOps[j] == "=" {
//					set_value_to_ifc(oldV, updateArgs[j])
//				} else {
//					set_value_to_ifc_int(oldV, updateOps[j], updateArgs[j])
//				}
//				//oldV.Set(reflect.ValueOf(updateArgs[j]))
//			}
//		}
//		if err != nil {
//			return
//		}
//	}
//	sql1, args := q.toSQL(tableStruct, ACTION_UPDATE_M)
//	_, err = q.Exec(sql1, args...)
//	return
//} else {

func (q *Query) UpdateM(m M) (affectedRows int64, err error) {
	if q.readOnly {
		return
	}

	defer dbxErrorDefer(&err, q)

	tableStruct := q.getTableStruct()

	// 更新相关的信息
	updateFields := make([]string, len(m))
	updateOps := make([]string, len(m))
	updateArgs := make([]interface{}, len(m))
	euqalOpcode := true
	for i, m := range m {
		if in_array(m.Key, tableStruct.PrimaryKey) {
			//return 0, errors.New("you can't update primary key, you can remove it first.")
			continue
		}
		opcode := m.Key[len(m.Key)-1:]
		if opcode == "+" || opcode == "-" || opcode == "*" || opcode == "%" || opcode == "=" {
			updateOps[i] = opcode
			updateFields[i] = m.Key[0 : len(m.Key)-1]
			euqalOpcode = false
		} else {
			updateOps[i] = "="
			updateFields[i] = m.Key
		}
		updateArgs[i] = m.Value
	}
	q.updateFields = updateFields
	q.updateOps = updateOps
	q.updateArgs = updateArgs
	//pkColNames := tableStruct.PrimaryKey

	// 8 种组合逻辑判断
	//isPK := len(q.primaryKeyStr) > 0
	cacheOn := q.tableEnableCache && tableStruct.EnableCache
	isCQL := q.isCQL

	var mp *syncmap.Map
	var listValue reflect.Value
	poses := make([][]int, len(updateFields))
	if cacheOn || isCQL {
		where2, args2, allowFiltering := q.whereToSQL(tableStruct)
		fields2 := arr_to_sql_add(tableStruct.PrimaryKey, "", ",", q.isCQL)
		sql2 := fmt.Sprintf("SELECT %v FROM %v%v%v", fields2, q.table, where2, allowFiltering)
		listValue, err = q.get_list_by_sql(sql2, args2...)
		if err != nil {
			return
		}
		listValue = listValue.Elem()
		// fmt.Printf("listValue len: %v\n", listValue.Len())
		var ok bool
		mp, ok = q.tableData[q.table];
		if !ok {
			errStr := fmt.Sprintf("q.tableData[q.table]: key %v does not exists.", q.table)
			q.ErrorLog(errStr)
			err = errors.New(errStr)
			return
		}
		for k, colName := range updateFields {
			n, ok := tableStruct.ColFieldMap.colMap[colName]
			if !ok {
				q.ErrorLog("UpdateM() colNmae does not exists: " + colName)
				continue
			}
			col := tableStruct.ColFieldMap.cols[n]
			poses[k] = col.FieldPos
		}
	}
	if cacheOn {
		updateSets := arr_to_sql_add(updateFields, "=?", ",", q.isCQL)
		for i := 0; i < listValue.Len(); i++ {
			row := listValue.Index(i) // 只有主键的数据
			rowElem := row.Elem()
			pkKey := get_pk_keys(tableStruct, rowElem)
			pkValues := get_pk_values(tableStruct, rowElem, q.isCQL)

			// 遍历 M，挨个更新字段
			old, ok := mp.Load(pkKey)
			if !ok {
				continue
				// todo: caceh 与 db 数据不一致，应该修补，但是数据不完整，跳过
				// mp[pkKey] = row.Interface()
			} else {
				// 更新有限的字段
				//fmt.Printf("old: %#v\n", old)
				updateNewArgs := make([]interface{}, 0)
				for j, _ := range updateFields {
					pos := poses[j]
					var oldV reflect.Value // 更新旧值，从 map 里面反射过来
					oldV = get_reflect_value_from_pos(reflect.ValueOf(old).Elem(), pos)

					if updateOps[j] == "=" {
						set_value_to_ifc(oldV, updateArgs[j])
					} else {
						// Cassandra 不支持！非 =
						set_value_to_ifc_int(oldV, updateOps[j], updateArgs[j])
						// 写入 CQL
						if isCQL && !euqalOpcode {
							updateNewArgs = append(updateNewArgs, oldV.Interface())
						}
					}
					//oldV.Set(reflect.ValueOf(updateArgs[j]))
				}
				if isCQL && !euqalOpcode {
					// 按照行更新
					where := arr_to_sql_add(tableStruct.PrimaryKey, "=?", " AND ", q.isCQL)
					sql3 := fmt.Sprintf("UPDATE %v SET %v WHERE %v", q.table, updateSets, where)
					updateNewArgs = append(updateNewArgs, pkValues...)
					affectedRows, err = q.Exec(sql3, updateNewArgs...)
				}
			}
		}
	}
	if isCQL {
		// 如果是 CQL，按照行更新 Database
		if euqalOpcode {
			for i := 0; i < listValue.Len(); i++ {
				row := listValue.Index(i) // 只有主键的数据
				pkValues := get_pk_values(tableStruct, row.Elem(), q.isCQL)
				q.WherePK(pkValues...) // todo: 不是很优雅，如果需要再次使用可以重复调用
				sql1, args := q.toSQL(tableStruct, ACTION_UPDATE_M)
				affectedRows, err = q.Exec(sql1, args...)
			}
		}
	} else {
		// 如果不是，则批量更新 Database
		sql1, args := q.toSQL(tableStruct, ACTION_UPDATE_M)
		affectedRows, err = q.Exec(sql1, args...)
	}
	return
}



func (db *DB) Exec(sql1 string, args ...interface{}) (n int64, err error) {
	if db.DriverType == DRIVER_CQL {
		err = db.CQLSession.Query(sql1, args...).Exec()
		db.LogSQL(sql1, args...)
		if err != nil {
			db.ErrorSQL(err.Error(), sql1, args...)
		}
		return
	} else {
		var result sql.Result
		result, err = db.DB.Exec(sql1, args...)
		db.LogSQL(sql1, args...)
		if err != nil {
			db.ErrorSQL(err.Error(), sql1, args...)
			return
		}
		prefix := strings.ToUpper(sql1[0:6])
		if prefix == "INSERT" {
			n, err = result.LastInsertId()
			if err != nil {
				db.ErrorSQL(err.Error(), sql1, args...)
				return
			}
		} else if prefix == "UPDATE" || prefix == "DELETE" {
			n, err = result.RowsAffected()
			if err != nil {
				db.ErrorSQL(err.Error(), sql1, args...)
				return
			}
		}
		return
	}
}

// ispk, cacheok iscql
func (q *Query) Delete() (n int64, err error) {
	if q.readOnly {
		return
	}
	defer dbxErrorDefer(&err, q)

	tableStruct := q.getTableStruct()

	isPK := len(q.primaryKeyStr) > 0
	cacheOn := q.tableEnableCache && tableStruct.EnableCache
	isCQL := q.isCQL

	// 根据 WHERE 条件更新，三种情况：
	/*
		1. where 为空
		2. where 为主键
		3. where 为复杂条件
	 */
	where2, args2, allowFiltering := q.whereToSQL(tableStruct)
	mp, ok := q.tableData[q.table]
	if !ok {
		errStr := fmt.Sprintf("q.tableData[q.table]: key %v does not exists.", q.table)
		q.ErrorLog(errStr)
		err = errors.New(errStr)
		return
	}
	if where2 == "" {
		q.Truncate()
		return
	}

	// 只更新一条
	if isPK {
		if cacheOn {
			mp.Delete(q.primaryKeyStr)
		}
		sql1, args := q.toSQL(tableStruct, ACTION_DELETE)
		n, err = q.Exec(sql1, args...)
		return
	} else {
		// 更新多条
		// 需要先查询: cacheOn || isCQL
		var listValue reflect.Value
		if cacheOn || isCQL {
			pkColNames := tableStruct.PrimaryKey
			fields2 := arr_to_sql_add(append(pkColNames), "", ",", q.isCQL)
			sql2 := fmt.Sprintf("SELECT %v FROM %v%v%v", fields2, q.table, where2, allowFiltering)
			listValue, err = q.get_list_by_sql(sql2, args2...)
			listValue = listValue.Elem()
		}
		// 更新缓存
		if cacheOn {
			for i := 0; i < listValue.Len(); i++ {
				row := listValue.Index(i)
				pkKey := get_pk_keys(tableStruct, row.Elem())
				mp.Delete(pkKey)
			}
		}
		// cql 需要按照条删除！
		if isCQL {
			for i := 0; i < listValue.Len(); i++ {
				row := listValue.Index(i)
				pkValues := get_pk_values(tableStruct, row.Elem(), q.isCQL)
				q.WherePK(pkValues...)
				sql1, args := q.toSQL(tableStruct, ACTION_DELETE)
				_, err = q.Exec(sql1, args...)
			}
		} else {
			sql1, args := q.toSQL(tableStruct, ACTION_DELETE)
			n, err = q.Exec(sql1, args...)
		}
	}

	return
}

// 获取多行
func (q *Query) get_list_by_sql(sql2 string, args2... interface{}) (listValue reflect.Value, err error) {
	tableStruct := q.getTableStruct()
	if q.isCQL {
		var rows *gocql.Iter
		rows, err = q.CQLQuery(sql2, args2...)
		if err != nil || rows == nil {
			return
		}
		defer rows.Close()

		ifc := reflect_make_slice_pointer(tableStruct.Type)
		listValue = reflect.ValueOf(ifc)
		err = cql_rows_to_arr_list(&listValue, rows, tableStruct, true) // 保留错误
	} else {
		var rows *sql.Rows
		rows, err = q.SQLQuery(sql2, args2...)
		if err != nil || rows == nil {
			return
		}
		defer rows.Close()

		ifc := reflect_make_slice_pointer(tableStruct.Type)
		listValue = reflect.ValueOf(ifc)
		err = rows_to_arr_list(listValue, rows, tableStruct, true) // 保留错误
	}
	return
}

// 获取一行
func (q *Query) get_row_by_sql(tableStruct *TableStruct, sql1 string, args... interface{}) (arrValue reflect.Value, err error) {
	var columns []string
	if q.DriverType == DRIVER_CQL {
		var rows *gocql.Iter
		rows, err = q.CQLQuery(sql1, args...)
		if err != nil || rows == nil {
			return
		}

		// 数据库返回的列，需要和表结构进行对应
		defer rows.Close()
		columns = cql_columns(rows.Columns())
		values := make([]interface{}, len(columns))

		posMap := map[int][]int{}
		for k, colName := range columns {
			n, ok := tableStruct.ColFieldMap.colMap[colName]
			if !ok {
				continue
			}
			col := tableStruct.ColFieldMap.cols[n]
			posMap[k] = col.FieldPos
			values[k] = reflect.New(col.FieldStruct.Type).Interface()
		}

		if b := rows.Scan(values...); !b {
			err = sql.ErrNoRows
			return
		}
		// 对应到相应的列
		for k, _ := range columns {
			pos, ok := posMap[k]
			if !ok {
				continue
			}
			arrValue = reflect.ValueOf(values[k]).Elem()
			ifc := arrValue.Interface()
			col := get_reflect_value_from_pos(arrValue, pos) // 需要设置的字段
			set_value_to_ifc(col, ifc)
		}
		return

	} else {
		var rows *sql.Rows
		rows, err = q.SQLQuery(sql1, args...)
		if err != nil || rows == nil {
			return
		}
		defer rows.Close()
		columns, err = rows.Columns()
		values := make([]interface{}, len(columns))
		if err != nil {
			q.ErrorSQL(err.Error(), sql1, args...)
			return
		}

		posMap := map[int][]int{}
		for k, colName := range columns {
			n, ok := tableStruct.ColFieldMap.colMap[colName]
			if !ok {
				continue
			}
			col := tableStruct.ColFieldMap.cols[n]
			posMap[k] = col.FieldPos
			values[k] = reflect.New(col.FieldStruct.Type).Interface()
		}

		//values := make([]interface{}, len(columns))
		//for i := range values {
		//	values[i] = new(interface{})
		//}

		if !rows.Next() {
			err = sql.ErrNoRows
			return
		}
		err = rows.Scan(values...)
		if err != nil {
			q.ErrorSQL(err.Error(), sql1, args...)
			return
		}
		// 对应到相应的列
		for k, _ := range columns {
			pos, ok := posMap[k]
			if !ok {
				continue
			}

			arrValue = reflect.ValueOf(values[k]).Elem()
			ifc := arrValue.Interface()
			col := get_reflect_value_from_pos(arrValue, pos) // 需要设置的字段
			set_value_to_ifc(col, ifc)

			////ifc_pos_to_value(values[k], pos, arrValue)
			//ifc := *(values[k].(*interface{})) // db 里面取出来的数据
			////valueV := reflect.ValueOf(value)
			////valueKind := valueV.Kind()
			//col := get_reflect_value_from_pos(arrValue.Elem(), pos) // 需要设置的字段
			//
			//set_value_to_ifc(col, ifc)

		}

		err = rows.Err()
		if err != nil {
			q.ErrorSQL(err.Error(), sql1, args...)
			return
		}
	}
	return
}

func (q *Query) get_row_by_pk(tableStruct *TableStruct, args... interface{}) (arrValue reflect.Value, err error) {
	where := " WHERE " + arr_to_sql_add(tableStruct.PrimaryKey, "=?", " AND ", q.isCQL)
	sql1 := fmt.Sprintf("SELECT * FROM %v WHERE %v", q.table, where)
	arrValue, err = q.get_row_by_sql(tableStruct, sql1, args...)
	return
}

func (db *DB) DebugCache() {
	for tableName, mp := range db.tableData {
		fmt.Printf("=================== %v ==================\n", tableName)
		mp.Range(func(k, v interface{}) bool {
			fmt.Printf("%v: %+v\n", k, v)
			return true
		})
		fmt.Printf("=====================================\n", tableName)
	}
}
