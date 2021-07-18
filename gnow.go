// gnow is a simple nowdb client.
//
// A typical workflow is as follows:
//
// 	func myquery(srv string, port string, usr string, pwd string) {
// 		c, err := Connect(srv, port, usr, pwd)
//     		if err != nil {
//			// error handling
//		}
//		defer c.Close()
//		err = c.Use("mydb")
//		if err != nil {
//     	 		// error handling
//		}
//		res, err = c.Execute("select count(*) from mytable")
//		if err != nil {
//     		 	// error handling
//		}
//		cur, err := res.Open()
//		if err != nil {
//     		 	// error handling
//		}
//		defer cur.Close()
//		for row, cerr := cur.Fetch(); cerr != nil; row, cerr = cur.Fetch() {
//			fmt.Printf("Count: %d\n", row.UInt(0))
//		}
//	}
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

// Leave shall be called immediately before program exit.
// It frees resources allocated by the C library on initialisation.
func Leave() {
	if global_is_initialised {
		C.nowdb_client_close()
	}
}

const (
	// success status code
	OK = 0
	_eof = 8
)

const (
	status  = 0x21
	report  = 0x22
	row     = 0x23
	cursor  = 0x24
)

// Return Type indicators
const (
	// Invalid or unknown return type
	InvalidT = -1
	// Status return type (ok/not ok)
	StatusT = 1
	// Cursor return type
	CursorT = 2
)

// Data type indicators
const (
	// NULL
	NOTHING = 0
	// Text type
	TEXT = 1
	// Time type
	DATE = 2
	// Time type
	TIME = 3
	// Float type
	FLOAT= 4
	// Int type
	INT  = 5
	// Uint type
	UINT = 6
	// Bool type
	BOOL = 9
)

// EOF error
var EOF = eof()

// NULL error
var NULL = null()

// Generic Error Type
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

// Type error
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

// Error type for server-side errors
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

// Now2Go converts a nowdb time value
// to a go time.Time object.
func Now2Go(n int64) time.Time {
	s := n / npersec
	ns := n - s * npersec
	return time.Unix(s,ns).UTC()
}

// Go2Now converts a go time.Time object
// to a nowdb time value
func Go2Now(t time.Time) int64 {
	return t.UnixNano()
}

// Connection type
type Connection struct {
   cc C.nowdb_con_t
}

// Connect creates a connection to the database server.
// It expects the server name and port and
// a user name and password.
// It returns a new Connection object or
// error on failure.
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

// Close closes the connection.
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

// Result is a polymorphic type to
// abstract data results.
// It is either a Status (ok or not ok)
// or a Cursor.
type Result struct {
	cs C.nowdb_result_t
	t  int
}

// Tell type returns the result type indicator
// of this result object.
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

// OK returns true if the status is ok
// and false otherwise.
func (r *Result) OK() bool {
	return (C.nowdb_result_errcode(r.cs) == OK)
}

// Error returns an error reflecting the status
// of the result.
func (r *Result) Error() string {
	if C.nowdb_result_details(r.cs) == nil {
		return fmt.Sprintf("%d", int(C.nowdb_result_errcode(r.cs)))
	}
	return fmt.Sprintf("%d: %s", int(C.nowdb_result_errcode(r.cs)),
		             C.GoString(C.nowdb_result_details(r.cs)))
}

// Errcode returns the numerical error code related to this result
func (r *Result) Errcode() int {
	return int(C.nowdb_result_errcode(r.cs))
}

// transform a result into a server error
func r2err(r C.nowdb_result_t) ServerError {
	return newServerError(fmt.Sprintf("%d: %s",
		int(C.nowdb_result_errcode(r)),
		C.GoString(C.nowdb_result_details(r))))
}

// Execute sends a SQL statement to the database.
// It retuns a result or an error.
// That means: Result is always ok;
// if there was an error result will be nil.
func (c *Connection) Execute(stmt string) (*Result, error) {
	var cr C.nowdb_result_t

	rc := C.nowdb_exec_statement(c.cc, C.CString(stmt), &cr)
	if rc != OK || cr == nil {
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

// Use defines the database to be used
// in all subsequent statements.
// Use must be called before any other statement.
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

// Destroy releases all resources
// allocated by the C library for
// this result. I must be called
// to avoid memory leaks in the
// C library.
// If the result is a cursor
// and the cursor was opened
// and closed, it is not necessary
// to call Destroy on the result
// (but it does not harm to call
// Destroy addionally).
func (r *Result) Destroy() {
	if r.cs == nil {
		return
	}
	C.nowdb_result_destroy(r.cs)
	r.cs = nil
}

// Cursor is an iterator over
// a resultset. It is created
// from an existing result.
// It inherits all resources
// from that result.
// Closing a cursor releases
// all resources (server- and
// client-side) assigned to it.
type Cursor struct {
	cc C.nowdb_cursor_t
	row C.nowdb_row_t
	first bool
}

func cur2res(c C.nowdb_cursor_t) C.nowdb_result_t {
	return C.nowdb_result_t(unsafe.Pointer(c))
}

// Open creates a cursor from a result.
// The cursor inherits all
// resources from the result.
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

// Close releases all resources assigned to the cursor
// and the result form which it was opened.
// A cursor shall be closed to avoid memory leaks
// in the C library and resources in the server
// (which, otherwise, would be pending until the end
// of the session).
// When the cursor was closed, it is not necessary
// to destroy the corresponding result
// (but there is also no harm in destroying
// the result additionally).
func (c *Cursor) Close() {
	if c.row != nil {
		if c.cc == nil {
			C.nowdb_result_destroy(C.nowdb_result_t(unsafe.Pointer(c.row)))
		}
		c.row = nil
	}
	if c.cc != nil {
		rc := C.nowdb_cursor_close(c.cc)
		if rc != OK {
			C.nowdb_result_destroy(C.nowdb_result_t(unsafe.Pointer(c.cc)))
		}
		c.cc = nil
	}
}

// The Row type represents one row in a resultset.
type Row struct {
	cr C.nowdb_row_t
}

func makeRow(c *Cursor) (*Row, error) {
	r := new(Row)
	r.cr = c.row
	return r, nil
}

// Fetch returns one row of the result set or error
// (but never both). 
func (c *Cursor) Fetch() (*Row, error) {
	if c.row != nil {
		if c.first {
			c.first = false
			return makeRow(c)
		}
		rc := C.nowdb_row_next(c.row)
		if rc != OK {
			if c.cc == nil {
				C.nowdb_result_destroy(C.nowdb_result_t(unsafe.Pointer(c.row)))
			}
			c.row = nil
		} else {
			return makeRow(c)
		}
	}
	if c.cc != nil {
		rc := C.nowdb_cursor_fetch(c.cc)
		if rc == OK {
			rc = C.nowdb_result_errcode(C.nowdb_result_t(unsafe.Pointer(c.cc)))
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

// Count returns the number of fields in the row.
func (r *Row) Count() int {
	if r.cr == nil {
		return 0
	}
	return int(C.nowdb_row_count(r.cr))
}

// Field returns the type indicator and
// the content of the field with index idx
// starting to count from 0.
// If idx is out of range, NOTHING is returned.
func (r *Row) Field(idx int) (int, interface{}) {
	var t C.int
	if r.cr == nil {
		return NOTHING, nil
	}
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

// String returns the field with index 'idx'
// as string. If that field is not a string,
// a type error is returned.
// If idx is out of range, NOTHING is returned.
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
		return 0, newTypeError("not a time value")
	}
	if !isTime && t != INT {
		return 0, newTypeError("not an int value")
	}
	return v.(int64), nil
}

// Time returns the field with index 'idx'
// as time value. If that field is not a time value,
// a type error is returned.
// If idx is out of range, NOTHING is returned.
func (r *Row) Time(idx int) (int64, error) {
	return r.intValue(idx, true)
}

// Int returns the field with index 'idx'
// as int value. If that field is not an int,
// a type error is returned.
// If idx is out of range, NOTHING is returned.
func (r *Row) Int(idx int) (int64, error) {
	return r.intValue(idx, false)
}

// UInt returns the field with index 'idx'
// as uint value. If that field is not a uint,
// a type error is returned.
// If idx is out of range, NOTHING is returned.
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

// Float returns the field with index 'idx'
// as float value. If that field is not a float,
// a type error is returned.
// If idx is out of range, NOTHING is returned.
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

// Bool returns the field with index 'idx'
// as bool value. If that field is not a bool,
// a type error is returned.
// If idx is out of range, NOTHING is returned.
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
