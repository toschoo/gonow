package gnow
// #include <nowdb/nowclient.h>
// #include <stdlib.h>
import "C"

import(
	"fmt"
	"os"
	"unsafe"
)

const (
	OK = 0
	_EOF = 8
)

const (
	ACK = 0x4f
	NOK = 0x4e
)

const (
	nothing = 0
	status  = 0x21
	report  = 0x22
	row     = 0x23
	cursor  = 0x24
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

type EOFError struct {
	what string
}

func eof() *EOFError {
	return new(EOFError)
}

func (e EOFError) Error() string {
	return "end-of-file"
}

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

var global_is_initialised bool

func init() {
	global_is_initialised = (C.nowdb_client_init() != 0)
}

func Leave() {
	if global_is_initialised {
		C.nowdb_client_close()
	}
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

func r2err(c int, msg string) ServerError {
	return newServerError(fmt.Sprintf("%d: %s", c, msg))
}

func (c *Connection) Execute(stmt string) (*Result, error) {
	var cr C.nowdb_result_t

	fmt.Printf("executing '%s'\n", stmt)
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
		err := r2err(int(C.nowdb_result_errcode(cr)), C.GoString(C.nowdb_result_details(cr)))
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
			if rc == _EOF {
				return nil, EOF
			}
			return nil, r2err(int(C.nowdb_result_errcode(unsafe.Pointer(c.cc))),
			           C.GoString(C.nowdb_result_details(unsafe.Pointer(c.cc))))
		}
		c.row = C.nowdb_cursor_row(c.cc)
		return makeRow(c)
	}
	return nil, EOF
}

func (r *Row) Field(idx int) interface{} {
	var t C.int
	v := C.nowdb_row_field(r.cr, C.int(idx), &t)
	switch(t) {
		case TEXT: return C.GoString((*C.char)(unsafe.Pointer(v)))
		default: return unsafe.Pointer(v)
	}
}

func (r *Row) String(idx int) (string, error) {
	var t C.int
	v := C.nowdb_row_field(r.cr, C.int(idx), &t)
	if t != TEXT {
		return "", newClientError("not a string")
	}
	return C.GoString((*C.char)(unsafe.Pointer(v))), nil
}
