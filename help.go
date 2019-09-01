package dbx

import (
	"database/sql"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unsafe"
)

func sqlite_get_create_table_sql(db *DB, tableName string) (string) {
	row := db.QueryRow(`select sql from sqlite_master where type="table" and name="` + tableName + `"`)

	var str string
	err := row.Scan(&str)
	if err != nil {
		panic(dbxErrorNew(err.Error()))
	}
	return str
}

func mysql_get_create_table_sql(db *DB, tableName string) (string) {
	row := db.QueryRow("show create table `" + tableName + "`")

	var tableName1, createTable string
	err := row.Scan(&tableName1, &createTable)
	if err != nil {
		// tableName 可能不存在
		panic(dbxErrorNew(err.Error()))
	}
	return createTable
}

// 判断两个 pk 是否相等
func pk_is_equa(pk1 []string, pk2 []string) bool {
	len1, len2 := len(pk1), len(pk2)
	if len1 != len2 {
		return false
	}
	if len1 == 0 {
		return true
	}
	for i := 0; i < len1; i++ {
		if pk1[i] != pk2[i] {
			return false
		}
	}
	return true
}

/*
	// 支持两种结构:
	create table xxx(
		id int primary key auto_increment,
		gid int,
		name text
	)
	create table xxx(
		id int auto_increment,
		gid int,
		name text,
		primary key(id, gid)
	)
*/
func get_pk_from_sql(sql string) []string {
	reg := regexp.MustCompile(`(?i)(?:primary\s+key|unique).*?\((.*?)\)`)
	arr := reg.FindStringSubmatch(sql)
	if len(arr) < 2 {
		reg = regexp.MustCompile(`(?i)(\w+).*(?:primary key|unique)`)
		arr := reg.FindStringSubmatch(sql)
		if len(arr) == 2 {
			return []string{arr[1]}
		} else {
			return nil
		}
	}
	arr2 := strings.Split(arr[1], ",")
	for k, v := range arr2 {
		arr2[k] = strings.Replace(strings.TrimSpace(v), "`", "", -1)
	}
	return arr2
}

func get_auto_increment_from_sql(sql string) string {
	reg := regexp.MustCompile(`(?i)(\w+).*(?:autoincrement|auto_increment)`)
	arr := reg.FindStringSubmatch(sql)
	if len(arr) == 2 {
		return arr[1]
	} else {
		return ""
	}
}

func get_table_info(db *DB, talbeName string) (pk []string, auto_increment string) {
	create_table_sql := ""
	if db.DriverType == DRIVER_MYSQL {
		create_table_sql = mysql_get_create_table_sql(db, talbeName)
	} else if db.DriverType == DRIVER_SQLITE {
		create_table_sql = sqlite_get_create_table_sql(db, talbeName)
	}
	pk = get_pk_from_sql(create_table_sql)
	auto_increment = get_auto_increment_from_sql(create_table_sql)
	return
}

// t 兼容 struct / &struct
func struct_fields_range_do(colFieldMap *ColFieldMap, t2 reflect.Type, pos1 []int) {
	t := t2
	if t.Kind() != reflect.Struct {
		if t.Kind() == reflect.Ptr {
			t = t.Elem()
		}
		if t.Kind() != reflect.Struct {
			panic(dbxErrorNew("type of first argument must be sturct."))
		}
	}

	n := t.NumField()
	for i := 0; i < n; i++ {
		col := new(Col)
		pos2 := make([]int, len(pos1)+1)
		copy(pos2, pos1)
		pos2[len(pos2)-1] = i

		field := t.Field(i)
		fieldType := field.Type
		if fieldType.Kind() == reflect.Ptr {
			fieldType = fieldType.Elem()
		}
		tagName, ok := field.Tag.Lookup("db")
		col.ColName = tagName
		col.FieldName = field.Name
		col.FieldPos = pos2
		col.FieldStruct = field

		if !ok && field.Anonymous == false {
			colFieldMap.Add(col)
			continue
		}
		if field.Anonymous == true {
			struct_fields_range_do(colFieldMap, fieldType, pos2)
		} else {
			colFieldMap.Add(col)
		}
	}
	return
}

//func struct_field_type_get(p reflect.Type, pos []int) reflect.Type {
//	var p2 reflect.Type
//	p2 = p
//	for _, i := range pos {
//		p2 = p2.Field(i).Type
//	}
//	return p2
//}

func struct_field_value_get(p reflect.Value, pos []int) reflect.Value {
	p2 := p
	for _, i := range pos {
		p2 = p2.Field(i)
	}
	return p2
}

// 只返回 keys 里面不为空的
func struct_values(keys []string, ifc interface{}) ([]interface{}) {
	var struct_values_do func(v reflect.Value) ([]interface{})
	i := 0
	keysLen := len(keys)
	struct_values_do = func(v reflect.Value) ([]interface{}) {
		if v.Kind() == reflect.Ptr {
			v = v.Elem()
		}
		if v.Kind() != reflect.Struct {
			panic(dbxErrorNew("type of first argument must be sturct."))
		}
		values := []interface{}{}
		n := v.NumField()
		for j := 0; j < n; j++ {
			if i >= keysLen {
				break
			}
			key := keys[i]
			if key == "" {
				i++
				continue
			}
			field := v.Field(j)
			if field.CanInterface() { //判断是否为可导出字段
				//判断是否是嵌套结构
				fieldType := field.Type()
				if fieldType.Kind() == reflect.Ptr && fieldType.Elem().Kind() == reflect.Struct {
					field = field.Elem()
				}
				if fieldType.Kind() == reflect.Struct {
					values2 := struct_values_do(field)
					values = append(values, values2...)
					continue
				} else {
					values = append(values, field.Interface())
				}
			}
			i++
		}
		return values
	}
	v := reflect.ValueOf(ifc)
	return struct_values_do(v)
}

func array_unique(arr []string) []string {
	r := []string{}
	for _, v := range arr {
		if v == "" {
			continue
		}
		r = append(r, v)
	}
	return r
}

func reflect_make_slice_pointer(ifc interface{}) interface{} {
	ifcType, ok := ifc.(reflect.Type)
	if !ok {
		ifcType = reflect.TypeOf(ifc)
	}
	arr2 := reflect.MakeSlice(reflect.SliceOf(ifcType), 0, 0)
	arr3 := reflect.New(arr2.Type())
	arr3.Elem().Set(arr2)
	return arr3.Interface()
}

// 第一个参数约定为：struct, 不能为 &struct
func get_reflect_value_from_pos(col reflect.Value, pos []int) (reflect.Value) {
	//fmt.Printf("col: %+v\n", col)
	var field reflect.Value
	if col.Kind() == reflect.Ptr {
		field = col.Elem()
	} else {
		field = col
	}
	for _, i := range pos {
		field = field.Field(i)
		//fmt.Printf("field: %+v\n", field)
		fieldType := field.Type()
		fieldKind := fieldType.Kind()
		if fieldKind == reflect.Ptr {
			if field.IsNil() {
				//fmt.Printf("field is nil: %+v, %+v\n", field, fieldType)
				newField := reflect.New(fieldType.Elem())
				field.Set(newField)
				//fmt.Printf("newField: %+v, %+v\n", field, field.Type())
				//os.Exit(0)
			}
			fieldElem := field.Elem()
			//if fieldElem.Type().Kind() == reflect.Struct {
			field = fieldElem

			//}
		}
	}
	// fmt.Printf("return field: %+v, %+v\n", field, field.Type())
	return field
}

// 第2个参数约定为：struct, 不能为 &struct
func get_pk_key(tableStruct *TableStruct, row reflect.Value) string {
	pkKeyName := make([]interface{}, 0)
	pkStr := ""
	for _, pos := range tableStruct.PrimaryKeyPos {
		row2 := get_reflect_value_from_pos(row, pos)
		pkKeyName = append(pkKeyName, row2.Interface())
		pkStr += "%v-"
	}
	pkStr = strings.TrimRight(pkStr, "-")
	pkKey := fmt.Sprintf(pkStr, pkKeyName...)
	return pkKey
}

func arr_to_sql_add(arr []string, sep1 string, sep2 string) string {
	sqlAdd := ""
	for _, v := range arr {
		sqlAdd += fmt.Sprintf("`%v`%v%v", v, sep1, sep2)
	}
	sqlAdd = strings.TrimRight(sqlAdd, sep2)
	return sqlAdd
}

func rows_to_arr_list(destp reflect.Value, rows *sql.Rows, tableStruct *TableStruct, arrIsPtr bool) (err error) {
	var dest reflect.Value
	// 最终返回的数组
	// 因为我们约定了 TableStruct.Type 为 &struct 类型，所以这里很方便
	//var dest reflect.Value
	if arrIsPtr {
		dest = reflect.MakeSlice(reflect.SliceOf(tableStruct.Type), 0, 0)
	} else {
		dest = reflect.MakeSlice(reflect.SliceOf(tableStruct.Type.Elem()), 0, 0)
	}

	// 数据库返回的列，需要和表结构进行对应
	var columns []string
	columns, err = rows.Columns()
	if err != nil {
		return
	}
	posMap := map[int][]int{}
	for k, colName := range columns {
		n, ok := tableStruct.ColFieldMap.colMap[colName]
		if !ok {
			//posMap[k] = nil // 标志不存在！
			continue
		}
		col := tableStruct.ColFieldMap.cols[n]
		posMap[k] = col.FieldPos
	}

	values := make([]interface{}, len(columns))
	for i := range values {
		// 每一列对应的类型
		values[i] = new(interface{})
	}

	totalRows := 0
	for rows.Next() {
		err = rows.Scan(values...)
		if err != nil {
			return
		}
		// 对应到相应的列
		row := reflect.New(tableStruct.Type.Elem())
		rowElem := row.Elem()
		for k, _ := range columns {
			pos, ok := posMap[k]
			// 如果不存在，则跳过
			if !ok {
				continue
			}

			ifc_pos_to_value(values[k], pos, row)

			/*
				value := *(values[k].(*interface{}))
				//value := values[k] // 返回的结果是一个 (*interface{}) 类型

				row2 := get_reflect_value_from_pos(rowElem, pos)
				switch value.(type) {
				case []uint8: {
					str := uint8_to_string(value.([]uint8))
					row2.Set(reflect.ValueOf(str))
				}
				default:
					row2.Set(reflect.ValueOf(value))
				}
			*/

			// 判断类型是否为 []uint8
			//valueValue := reflect.ValueOf(value)
			//valueType := valueValue.Type()
			//
			//// 判断目标是否为 *interface
			////switch expr {
			////	case *interface{}:
			////}
			//// https://github.com/go-sql-driver/mysql/issues/407
			//// mysql has two protocols
			//if valueType.Kind() == reflect.Slice && valueType.Elem().Kind() == reflect.Uint8 {
			//	str := uint8_to_string(value.([]uint8))
			//	row2.Set(reflect.ValueOf(str))
			//} else {
			//	row2.Set(valueValue)
			//}

		}
		if arrIsPtr {
			dest = reflect.Append(dest, row) // reflect.Indirect
		} else {
			dest = reflect.Append(dest, rowElem)
		}
		totalRows++
	}
	if totalRows == 0 {
		return sql.ErrNoRows
	}

	// 约定了必须传地址，所以这里可以直接设置值
	//arrListValue := reflect.ValueOf(arrListIfc)
	//if arrListValue.Type().Kind() == reflect.Ptr {
	//	arrListValue.Elem().Set(dest)
	//} else {
	//	arrListValue.Set(dest)
	//}

	//dest2 = dest.Interface()

	// 赋值: destp = dest
	// todo: 约定都为指针 &arrList， 需要 destp.Elem()
	if destp.Kind() == reflect.Ptr {
		destp.Elem().Set(dest)
	} else {
		destp.Set(dest)
	}

	err = rows.Err()
	if err != nil {
		return
	}
	return
}

// uncludePK 是否排除主键
func struct_value_to_args(tableStruct *TableStruct, value reflect.Value, uncludeAutoIncrement bool, uncludePK bool) ([]interface{}, []interface{}, interface{}) {
	args := make([]interface{}, 0)
	pkArgs := make([]interface{}, 0)
	var autoIncrementArg interface{}
	for _, colName := range tableStruct.ColFieldMap.colArr {
		flag1 := 0
		if uncludeAutoIncrement && colName == tableStruct.AutoIncrement {
			flag1 = 1
		}
		if uncludePK {
			if in_array(colName, tableStruct.PrimaryKey) {
				flag1 = 2
			}
		}
		n := tableStruct.ColFieldMap.colMap[colName]
		pos := tableStruct.ColFieldMap.cols[n].FieldPos
		v := get_reflect_value_from_pos(value, pos)
		vi := v.Interface()
		vtime, ok := vi.(time.Time)
		var tmp interface{}
		if ok {
			tmp = vtime.Format("2006-01-02 15:04:05")
		} else {
			tmp = vi
		}
		if flag1 == 0 {
			args = append(args, tmp)
		} else if flag1 == 1 {
			autoIncrementArg = tmp
		} else if flag1 == 2 {
			pkArgs = append(pkArgs, tmp)
		}
	}
	return args, pkArgs, autoIncrementArg
}

func ifc_pos_to_value(fromIfc interface{}, fromPos []int, retValue reflect.Value) error {
	value := *(fromIfc.(*interface{})) // db 里面取出来的数据
	//valueV := reflect.ValueOf(value)
	//valueKind := valueV.Kind()

	col := get_reflect_value_from_pos(retValue.Elem(), fromPos)

	set_value_to_ifc(col, value)

	//
	//colKind := col.Kind()
	//
	//// 判断类型是否为 []uint8
	//if valueKind == colKind {
	//	col.Set(valueV)
	//} else {
	//	switch value.(type) {
	//	case []uint8:
	//		str := uint8_to_string(value.([]uint8))
	//		col.Set(reflect.ValueOf(str))
	//	case int8, int16, int32, int64:
	//		vuint64 := uint64(valueV.Int())
	//		switch colKind {
	//		case reflect.Uint64:
	//			col.Set(reflect.ValueOf(vuint64))
	//		case reflect.Uint8:
	//			col.Set(reflect.ValueOf(uint8(vuint64)))
	//		case reflect.Uint16:
	//			col.Set(reflect.ValueOf(uint16(vuint64)))
	//		case reflect.Uint32:
	//			col.Set(reflect.ValueOf(uint32(vuint64)))
	//		}
	//	case uint8, uint16, uint32, uint64:
	//		vint64 := int64(valueV.Uint())
	//		switch colKind {
	//		case reflect.Uint64:
	//			col.Set(reflect.ValueOf(vint64))
	//		case reflect.Uint8:
	//			col.Set(reflect.ValueOf(int8(vint64)))
	//		case reflect.Uint16:
	//			col.Set(reflect.ValueOf(int16(vint64)))
	//		case reflect.Uint32:
	//			col.Set(reflect.ValueOf(int32(vint64)))
	//		}
	//	default:
	//		// 类型错误
	//		panic(dbxErrorNew("type convert error: %v -> %v", valueKind, colKind))
	//		//col.Set(reflect.ValueOf(value))
	//	}
	//}
	return nil
}

func uint8_to_string(bytes []uint8) string {
	p := unsafe.Pointer(&bytes)
	str := *(*string)(p) //cast it to a string pointer and assign the value of this pointer
	return str
}

// 格式任意转换
//func type_convert(kind reflect.Kind, ifc interface{}) (ifc2 interface{}) {
//	ifcType := reflect.TypeOf(ifc)
//	ifcKind := ifcType.Kind()
//	if kind == ifcKind {
//		return ifc
//	}
//	switch kind {
//	case reflect.String:
//		return fmt.Sprintf("%v", ifc)
//	case reflect.Int:
//		switch ifcKind {
//		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
//
//		}
//	}
//
//	return nil
//}
//
//func set_value_to_ifc(value reflect.Value, ifc interface{}) {
//	//fmt.Printf("%v, %v\n", value.String(), value.Kind())
//	valKind := value.Kind()
//	ifcValue := reflect.ValueOf(ifc)
//	ifcKind := ifcValue.Kind()
//	if valKind == ifcKind {
//		value.Set(ifcValue)
//		return
//	}
//	switch valKind {
//	case reflect.String:
//
//
//	}
//
//	switch ifc.(type) {
//	case []uint8:
//		str := uint8_to_string(ifc.([]uint8))
//		value.Set(reflect.ValueOf(str))
//	case int, int8, int16, int32, int64:
//		vuint64 := uint64(ifcValue.Int())
//		switch valKind {
//		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
//			value.SetInt(int64(ifcValue.Int()))
//
//		case reflect.Uint:
//			value.Set(reflect.ValueOf(uint(vuint64)))
//		case reflect.Uint64:
//			value.Set(reflect.ValueOf(vuint64))
//		case reflect.Uint8:
//			value.Set(reflect.ValueOf(uint8(vuint64)))
//		case reflect.Uint16:
//			value.Set(reflect.ValueOf(uint16(vuint64)))
//		case reflect.Uint32:
//			value.Set(reflect.ValueOf(uint32(vuint64)))
//		default:
//			str := fmt.Sprintf("%v -> %v convert error!\n", valKind, ifcKind)
//			panic(str)
//		}
//	case uint8, uint16, uint32, uint64:
//		vint64 := int64(ifcValue.Uint())
//		switch valKind {
//		case reflect.Unt, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
//			value.SetUint(int64(ifcValue.Int()))
//		case reflect.Uint:
//			value.Set(reflect.ValueOf(int(vint64)))
//		case reflect.Uint64:
//			value.Set(reflect.ValueOf(vint64))
//		case reflect.Uint8:
//			value.Set(reflect.ValueOf(int8(vint64)))
//		case reflect.Uint16:
//			value.Set(reflect.ValueOf(int16(vint64)))
//		case reflect.Uint32:
//			value.Set(reflect.ValueOf(int32(vint64)))
//			str := fmt.Sprintf("%v -> %v convert error!\n", valKind, ifcKind)
//			panic(str)
//		}
//	default:
//		str := fmt.Sprintf("%v -> %v convert error!\n", valKind, ifcKind)
//		panic(str)
//		//fmt.Printf("ifc type: %#v\n", reflect.TypeOf(ifc).Kind().String())
//		// 类型错误
//		//panic(dbxErrorNew("type convert error: %v -> %v", valueKind, colKind))
//		//col.Set(reflect.ValueOf(value))
//	}
//}

//func struct_value_to_pk_args(tableStruct *TableStruct, value reflect.Value) ([]interface{}) {
//	args := make([]interface{}, 0)
//	for _, pos := range tableStruct.PrimaryKeyPos {
//		v, _ := get_reflect_value_from_pos(value, pos)
//		vi := v.Interface()
//		vtime, ok := vi.(time.Time)
//		if ok {
//			args = append(args, vtime.Format("2006-01-02 15:04:05"))
//		} else {
//			args = append(args, vi)
//		}
//	}
//	return args
//}

// 用第一个数组减去第二个数组
func array_sub(arr1 []string, arr2 []string) []string {
	arr := []string{}
	for _, v := range arr1 {
		if in_array(v, arr2) {
			continue
		} else {
			arr = append(arr, v)
		}
	}
	return arr
}

func in_array(v string, arr []string) bool {
	for _, v2 := range arr {
		if v == v2 {
			return true
		}
	}
	return false
}

func set_value_to_ifc(dv reflect.Value, src interface{}) {
	dk := dv.Kind()
	if dv.Kind() == reflect.Ptr {
		dv = dv.Elem()
		dk = dv.Kind()
	}

	sv := reflect.ValueOf(src)
	st := sv.Type()
	//sk := sv.Kind()
	dt := dv.Type()

	//if _, ok := src.(time.Time);  ok {
	//
	//}

	switch src.(type) {
	case []byte:
		if dk == reflect.String {
			dv.Set(reflect.ValueOf(uint8_to_string(src.([]uint8))))
			return
		} else if dk == reflect.Float64 || dk == reflect.Float32 {
			str := uint8_to_string(src.([]uint8))
			f, err := strconv.ParseFloat(str, 64)
			if err != nil {
				panic(dbxErrorNew("convert failed: %v -> %v, error: %v", st, dt, err.Error()))
			}
			if dk == reflect.Float64 {
				dv.Set(reflect.ValueOf(f))
			} else {
				// convert float64 -> float32
				dv.Set(sv.Convert(reflect.TypeOf(dt)))
			}
			return
		}
	case string:
		if dk != reflect.String {
			dv.Set(reflect.ValueOf(fmt.Sprintf("%v", src)))
		}
	}

	switch dk {
	case reflect.String:
		dv.Set(reflect.ValueOf(fmt.Sprintf("%v", src)))
		return
	}

	//fmt.Printf("sk: %v, dk: %v, dt: %v\n", sk, dk, dt)
	//if dv.Kind() == sv.Kind() && (st.ConvertibleTo(dt) || dt.ConvertibleTo(st)) {
	if st.ConvertibleTo(dt) {
		dv.Set(sv.Convert(dt))
		return
	}

	panic(dbxErrorNew("convert failed: %v -> %v", st, dt))

}

//func uint8_to_string(bs []uint8) string {
//	ba := []byte{}
//	for _, b := range bs {
//		ba = append(ba, byte(b))
//	}
//	return string(ba)
//}
