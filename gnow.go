package gnow
// #include <nowdb/nowclient.h>
// #include <stdlib.h>
import "C"

import(
	"fmt"
	"os"
	"time"
	"unsafe"
)

var global_is_initialised bool

func init() {
	global_is_initialised = (C.nowdb_client_init() != 0)
}

func Leave() {
	if global_is_initialised {
		C.nowdb_client_close()
	}
}

const (
	OK = 0
	_eof = 8
)

const (
	status  = 0x21
	report  = 0x22
	row     = 0x23
	cursor  = 0x24
)

const (
	InvalidT = -1
	StatusT = 1
	CursorT = 2
)

const (
	NOTHING = 0
	TEXT = 1
	DATE = 2
	TIME = 3
	FLOAT= 4
	INT  = 5
	UINT = 6
	BOOL = 9
)

var EOF = eof()
var NULL = null()

type ClientError struct {
	what string
}

func newClientError(s string) (e ClientError) {
	e.what = s
	return
}

func (e ClientError) Error() string {
	return e.what
}

func eof() ClientError{
	return newClientError("end-of-file")
}

type TypeError struct {
	what string
}

func newTypeError(s string) (e TypeError) {
	e.what = s
	return
}

func (e TypeError) Error() string {
	return e.what
}

func null() TypeError{
	return newTypeError("NULL")
}

type ServerError struct {
	what string
}

func newServerError(s string) (e ServerError) {
	e.what = s
	return
}

func (e ServerError) Error() string {
	return e.what
}

const npersec = 1000000000

func Now2Go(n int64) time.Time {
	s := n / npersec
	ns := n - s * npersec
	return time.Unix(s,ns).UTC()
}

func Go2Now(t time.Time) int64 {
	return t.UnixNano()
}

type Connection struct {
   cc C.nowdb_con_t
}

func Connect(server string, port string, usr string, pwd string) (*Connection, error) {
	var cc C.nowdb_con_t

	if !global_is_initialised {
		return nil, newClientError("Client is not initialised")
	}

	rc := C.nowdb_connect(&cc, C.CString(server), C.CString(port), nil, nil, 0)
	if rc != OK {
		fmt.Fprintf(os.Stderr, "cannot connect: %d\n", rc)
		m := fmt.Sprintf("%d", rc) // explain!
		return nil, newServerError(m)
	}

	c := new(Connection)
	c.cc = cc

	return c, nil
}

func (c *Connection) Close() error {
	if c.cc == nil {
		return nil
	}
	rc := C.nowdb_connection_close(c.cc)
	if rc != OK {
		C.nowdb_connection_destroy(c.cc)
		c.cc = nil
		fmt.Fprintf(os.Stderr, "cannot connect: %d\n", rc)
		m := fmt.Sprintf("%d", rc) // explain!
		return newServerError(m)
	}
	c.cc = nil
	return nil
}

type Result struct {
	cs C.nowdb_result_t
	t  int
}

func (r *Result) TellType() int {
	t := C.nowdb_result_type(r.cs)
	switch(t) {
	case status: fallthrough
	case report: return StatusT
	case row: fallthrough
	case cursor: return CursorT
	default: return -1
	}
}

func (r *Result) OK() bool {
	return (C.nowdb_result_errcode(r.cs) == OK)
}

func (r *Result) Error() string {
	return fmt.Sprintf("%d: %s", C.nowdb_result_errcode(r.cs),
	                             C.nowdb_result_details(r.cs))
}

func r2err(r C.nowdb_result_t) ServerError {
	return newServerError(fmt.Sprintf("%d: %s",
		int(C.nowdb_result_errcode(r)),
		C.GoString(C.nowdb_result_details(r))))
}

func (c *Connection) Execute(stmt string) (*Result, error) {
	var cr C.nowdb_result_t

	rc := C.nowdb_exec_statement(c.cc, C.CString(stmt), &cr)
	if rc != OK {
		fmt.Fprintf(os.Stderr, "cannot execute: %d\n", rc)
		m := fmt.Sprintf("%d", rc) // explain!
		return nil, newServerError(m)
	}

	r := new(Result)
	r.cs = cr

	r.t = int(C.nowdb_result_type(cr))

	if int(C.nowdb_result_status(cr)) != OK {
		err := r2err(cr)
		r.Destroy()
		return nil, err
	}
	return r, nil
}

func (c *Connection) Use(db string) error {
	stmt := fmt.Sprintf("use %s", db)
	r, err := c.Execute(stmt)
	if err != nil {
		return err
	}
	if r != nil {
		r.Destroy()
	}
	return nil
}

func (r *Result) Destroy() {
	if r.cs == nil {
		return
	}
	C.nowdb_result_destroy(r.cs)
	r.cs = nil
}

type Cursor struct {
	cc C.nowdb_cursor_t
	row C.nowdb_row_t
	first bool
}

func cur2res(c C.nowdb_cursor_t) C.nowdb_result_t {
	return C.nowdb_result_t(unsafe.Pointer(c))
}

func (r *Result) Open() (*Cursor, error) {
	if r.t != cursor && r.t != row {
		return nil, newClientError("not a cursor")
	}

	c := new(Cursor)
	c.first = true

	if r.t == cursor {
		rc := C.nowdb_cursor_open(r.cs, &c.cc)
		if rc != OK {
			return nil, newClientError(fmt.Sprintf("%d", rc))
		}
		c.row = C.nowdb_cursor_row(c.cc)
	} else {
		c.row = C.nowdb_row_t(unsafe.Pointer(r.cs))
	}

	r.cs = nil

	return c, nil
}

func (c *Cursor) Close() {
	if c.row != nil {
		if c.cc == nil {
			C.nowdb_result_destroy(unsafe.Pointer(c.row))
		}
		c.row = nil
	}
	if c.cc != nil {
		rc := C.nowdb_cursor_close(c.cc)
		if rc != OK {
			C.nowdb_result_destroy(unsafe.Pointer(c.cc))
		}
		c.cc = nil
	}
}

type Row struct {
	cr C.nowdb_row_t
}

func makeRow(c *Cursor) (*Row, error) {
	r := new(Row)
	r.cr = c.row
	return r, nil
}

func (c *Cursor) Fetch() (*Row, error) {
	if c.row != nil {
		if c.first {
			c.first = false
			return makeRow(c)
		}
		rc := C.nowdb_row_next(c.row)
		if rc != OK {
			if c.cc == nil {
				C.nowdb_result_destroy(unsafe.Pointer(c.row))
			}
			c.row = nil
		} else {
			return makeRow(c)
		}
	}
	if c.cc != nil {
		rc := C.nowdb_cursor_fetch(c.cc)
		if rc == OK {
			rc = C.nowdb_result_errcode(unsafe.Pointer(c.cc))
		}
		if rc != OK {
			if rc == _eof {
				return nil, EOF
			}
			return nil, r2err(cur2res(c.cc))
		}
		c.row = C.nowdb_cursor_row(c.cc)
		return makeRow(c)
	}
	return nil, EOF
}

func (r *Row) Field(idx int) (int, interface{}) {
	var t C.int
	v := C.nowdb_row_field(r.cr, C.int(idx), &t)
	switch(t) {
	case NOTHING: return NOTHING, nil
	case TEXT: return int(t), C.GoString((*C.char)(v))
	case DATE: fallthrough
	case TIME: fallthrough
	case INT:  return int(t), *(*int64)(v)
	case UINT: return int(t), *(*uint64)(v)
	case FLOAT: return int(t), *(*float64)(v)
	case BOOL:
		x := *(*C.char)(v)
		if x == 0 {
			return int(t), false
		} else {
			return int(t), true
		}

	default: return NOTHING, nil
	}
}

func (r *Row) String(idx int) (string, error) {
	t, v := r.Field(idx)
	if t == NOTHING {
		return "", NULL
	}
	if t != TEXT {
		return "", newTypeError("not a string")
	}
	return v.(string), nil
}

func (r *Row) intValue(idx int, isTime bool) (int64, error) {
	t, v := r.Field(idx)
	if t == NOTHING {
		return 0, NULL
	}
	if isTime && t != TIME && t != DATE && t != INT {
		return 0, newTypeError("not time value")
	}
	if !isTime && t != INT {
		return 0, newTypeError("not an int value")
	}
	return v.(int64), nil
}

func (r *Row) Time(idx int) (int64, error) {
	return r.intValue(idx, true)
}

func (r *Row) Int(idx int) (int64, error) {
	return r.intValue(idx, false)
}

func (r *Row) UInt(idx int) (uint64, error) {
	t, v := r.Field(idx)
	if t == NOTHING {
		return 0, NULL
	}
	if t != UINT {
		return 0, newTypeError("not an uint value")
	}
	return v.(uint64), nil
}

func (r *Row) Float(idx int) (float64, error) {
	t, v := r.Field(idx)
	if t == NOTHING {
		return 0, NULL
	}
	if t != FLOAT {
		return 0, newTypeError("not a float value")
	}
	return v.(float64), nil
}

func (r *Row) Bool(idx int) (bool, error) {
	t, v := r.Field(idx)
	if t == NOTHING {
		return false, NULL
	}
	if t != BOOL {
		return false, newTypeError("not a bool value")
	}
	return v.(bool), nil
}
