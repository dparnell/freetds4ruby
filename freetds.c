#include "ruby.h"

#include "ctpublic.h"

static VALUE rb_FreeTDS;
static VALUE rb_Driver;
static VALUE rb_Connection;
static VALUE rb_Statement;

static VALUE rb_DateTime;

typedef struct _tds_connection {
	CS_CONTEXT *context;
	CS_CONNECTION *connection;
} TDS_Connection;

typedef struct _ex_column_data
{
  CS_INT        datatype;
  CS_CHAR         *value;
  CS_INT        valuelen;
  CS_SMALLINT  indicator;
} EX_COLUMN_DATA;

/*** helper functions ***/

static char* value_to_cstr(VALUE value) {
	VALUE str;
	char* result = NULL;
	int max;
	
	if(RTEST(value)) {
		str = value;
		if( TYPE(str) != T_STRING ) {
			str = rb_str_to_str(str);
		}
		str = StringValue(value);
		max = RSTRING(str)->len;
		result = malloc(max+1);
		bzero(result, max+1);
		strncpy(result, STR2CSTR(str), max);		
	}
	
	return result;
}

static VALUE getConstant(const char *name, VALUE module)
{
   VALUE owner = module,
         constants,
         string,
         exists,
         entry;

   /* Check that we've got somewhere to look. */
   if(owner == Qnil)
   {
      owner = rb_cModule;
   }
   constants = rb_funcall(owner, rb_intern("constants"), 0),
   string    = rb_str_new2(name),
   exists    = rb_funcall(constants, rb_intern("include?"), 1, string);

   if(exists != Qfalse)
   {
      ID    id     = rb_intern(name);
      VALUE symbol = ID2SYM(id);

      entry = rb_funcall(owner, rb_intern("const_get"), 1, symbol);
   }

   return(entry);
}

static VALUE getClass(const char *name)
{
   VALUE klass = getConstant(name, Qnil);

   if(klass != Qnil)
   {
      VALUE type = rb_funcall(klass, rb_intern("class"), 0);

      if(type != rb_cClass)
      {
         klass = Qnil;
      }
   }

   return(klass);
}

/*** end of helper functions ***/

static void free_tds_connection(void *p) {
	free(p);
}

static VALUE alloc_tds_connection(VALUE klass) {
	TDS_Connection* conn;
	VALUE result;
	
	conn = malloc(sizeof(TDS_Connection));
	bzero(conn, sizeof(TDS_Connection));
	
	result = Data_Wrap_Struct(klass, 0, free_tds_connection, conn);
	
	return result;
}
/*
static int connection_handle_message(TDSCONTEXT * context, TDSSOCKET * tds, TDSMESSAGE * msg)
{
	VALUE self = (VALUE)tds_get_parent(tds);

	if(RTEST(self)) {
		if (msg->msg_level == 0) {
			VALUE messages = rb_iv_get(self, "@messages");
			rb_ary_push(messages, rb_str_new2(msg->message));
			return 0;
		}

//		printf("got message %d - %d - %s\n", msg->msg_level, msg->msg_number, msg->message);
		
		if (msg->msg_number != 5701 && msg->msg_number != 5703 && msg->msg_number != 20018) {
			TDS_Connection* conn;		
			VALUE errors = rb_iv_get(self, "@errors");
			VALUE err = rb_hash_new();
				
			rb_hash_aset(err, rb_str_new2("error"), INT2FIX(msg->msg_number));
			rb_hash_aset(err, rb_str_new2("level"), INT2FIX(msg->msg_level));
			rb_hash_aset(err, rb_str_new2("state"), INT2FIX(msg->msg_state));
			rb_hash_aset(err, rb_str_new2("server"), rb_str_new2(msg->server));
			rb_hash_aset(err, rb_str_new2("line"), INT2FIX(msg->line_number));
			rb_hash_aset(err, rb_str_new2("message"), rb_str_new2(msg->message));
		
			rb_ary_push(errors, err);
		}
	}
	return 0;
}
*/

static VALUE connection_Initialize(VALUE self, VALUE connection_hash) {
	TDS_Connection* conn;
	
	char *hostname = NULL;
	char *servername = NULL;
	char *username = NULL;
	char *password = NULL;
	char *confile = NULL;
	char *charset = NULL;
	int port = 0;
	VALUE temp;
	VALUE errors;
	CS_RETCODE ret;
	
	Data_Get_Struct(self, TDS_Connection, conn);
	cs_ctx_alloc(CS_VERSION_100, &conn->context);
	ct_init(conn->context, CS_VERSION_100);
	ct_con_alloc(conn->context, &conn->connection);
	
//	conn->context->msg_handler = connection_handle_message;
//	conn->context->err_handler = connection_handle_message;
	
	/* now let's get the connection parameters */
	temp = rb_hash_aref(connection_hash, ID2SYM(rb_intern("hostname")));
	hostname = value_to_cstr(temp);

	temp = rb_hash_aref(connection_hash, ID2SYM(rb_intern("port")));
	if(RTEST(temp)) {
		port = FIX2INT(temp);
	} else {
		port = 1433;
	}

	temp = rb_hash_aref(connection_hash, ID2SYM(rb_intern("username")));
	username = value_to_cstr(temp);
	
	temp = rb_hash_aref(connection_hash, ID2SYM(rb_intern("password")));
	password = value_to_cstr(temp);

	temp = rb_hash_aref(connection_hash, ID2SYM(rb_intern("servername")));
	servername = value_to_cstr(temp);

	temp = rb_hash_aref(connection_hash, ID2SYM(rb_intern("charset")));
	charset = value_to_cstr(temp);

	if(charset==NULL) {
		charset = strdup("ISO-8859-1");
	}
	
	/* validate parameters */
	if (!servername && !hostname) {
		rb_raise(rb_eArgError, "Either servername or hostname must be specified");
		return Qnil;
	}
	
	if (hostname && !port) {
		rb_raise(rb_eArgError, "No port specified");
		return Qnil;
	}

	if (!username) {
		rb_raise(rb_eArgError, "No username specified");
		return Qnil;
	}

	if (!password) {
		password = strdup("");
	}	
	
//	printf("*** servername='%s', username='%s' password='%s'\n", servername, username, password);
	
	ct_con_props(conn->connection, CS_SET, CS_USERNAME, username, CS_NULLTERM, NULL);
	ct_con_props(conn->connection, CS_SET, CS_PASSWORD, password, CS_NULLTERM, NULL);
	
	if (hostname) {
		ct_con_props(conn->connection, CS_SET, CS_HOSTNAME, hostname, CS_NULLTERM, NULL);
		ct_con_props(conn->connection, CS_SET, CS_PORT, (char*)&port, CS_NULLTERM, NULL);
	}	

	/* Try to open a connection */
   	ret = ct_connect(conn->connection, servername, CS_NULLTERM);
	
	/* free up all the memory */
	if (hostname)
		free(hostname);
	if (username)
		free(username);
	if (password)
		free(password);
	if (servername)
		free(servername);

	if(ret!=CS_SUCCEED) {
		rb_raise(rb_eIOError, "Connection failed");
		return Qnil;
	}

	rb_iv_set(self, "@messages", rb_ary_new());
	errors = rb_ary_new();
	rb_iv_set(self, "@errors", errors);
	
	return Qnil;
}

static VALUE connection_Statement(VALUE self, VALUE query) {
	TDS_Connection* conn;
	
	Data_Get_Struct(self, TDS_Connection, conn);

	if(conn->connection) {
		VALUE statement = rb_class_new_instance(0, NULL, rb_Statement);
	
		rb_iv_set(statement, "@connection", self);
		rb_iv_set(statement, "@query", query);
	
		return statement;
	} 
	
	rb_raise(rb_eEOFError, "The connection is closed");

	return Qnil;
}

static VALUE connection_Close(VALUE self) {
	TDS_Connection* conn;
	
	Data_Get_Struct(self, TDS_Connection, conn);

	ct_close(conn->connection, CS_FORCE_CLOSE);
	ct_exit(conn->context, CS_FORCE_EXIT);

	conn->connection = NULL;
	conn->context = NULL;

	return Qnil;
}

static char* column_type_name(CS_DATAFMT column) {
	char * column_type;
	
	switch (column.datatype) {
	case CS_TINYINT_TYPE:
		column_type = "tinyint";
		break;
	case CS_BIT_TYPE:
		column_type = "bit";
		break;
	case CS_SMALLINT_TYPE:
		column_type = "smallint";
		break;
	case CS_INT_TYPE:
		column_type = "int";
		break;
	case CS_DATETIME_TYPE:
		column_type = "datetime";
		break;
	case CS_DATETIME4_TYPE:
		column_type = "smalldatetime";
		break;
	case CS_REAL_TYPE:
		column_type = "real";
		break;
	case CS_MONEY_TYPE:
		column_type = "money";
		break;
	case CS_MONEY4_TYPE:
		column_type = "smallmoney";
		break;
	case CS_FLOAT_TYPE:
		column_type = "float";
		break;

	case CS_DECIMAL_TYPE:
		column_type = "decimal";
		break;
	case CS_NUMERIC_TYPE:
		column_type = "numeric";
		break;

	case CS_VARCHAR_TYPE:
		column_type = "varchar";
		break;		
	case CS_CHAR_TYPE:
		column_type = "char";
		break;
		
	case CS_VARBINARY_TYPE:
		column_type = "varbinary";
		break;
	case CS_BINARY_TYPE:
		column_type = "binary";
		break;
	case CS_TEXT_TYPE:
		column_type = "text";
		break;
	case CS_IMAGE_TYPE:
		column_type = "image";
		break;
	case CS_UNICHAR_TYPE:
		column_type = "nvarchar";
		break;
	// case XSYBNCHAR:
	// 	column_type = "nchar";
	// 	break;
	// case SYBNTEXT:
	// 	column_type = "ntext";
	// 	break;
	// case SYBUNIQUE:
	// 	column_type = "uniqueidentifier";
	// 	break;
	default:
//		printf("here - %d\n", column->column_type);
		return NULL;
	}
	
	return column_type;
}

static VALUE statement_Execute(VALUE self) {
	int i;
	CS_DATAFMT col;
	CS_DATAFMT *cols;
	EX_COLUMN_DATA *col_data;
	CS_INT rc;
	CS_INT resulttype;
	CS_INT num_cols;
	CS_INT col_len;
	CS_INT row_count = 0;
	CS_INT rows_read;

	struct timeval start, stop;
	int print_rows = 1;
	char message[128];
	char* buf;
	CS_DATEREC date_rec;
	char output[30];
	CS_INT output_len;
	int tempInt;
	double tempDouble;
	
	TDS_Connection* conn;
	CS_COMMAND * cmd;
	
	VALUE connection;
	VALUE query;
	VALUE columns;
	VALUE rows;
	VALUE status;
	VALUE errors;
	
	VALUE date_parts[8];
	
	VALUE column;
	VALUE row;
	
	VALUE column_name = rb_str_new2("name");
	VALUE column_type = rb_str_new2("type");
	VALUE column_size = rb_str_new2("size");
	VALUE column_scale = rb_str_new2("scale");
	VALUE column_precision = rb_str_new2("precision");
	
	VALUE column_value;
	
	connection = rb_iv_get(self, "@connection");
	query = rb_iv_get(self, "@query");
	
	columns = rb_ary_new();
	rb_iv_set(self, "@columns", columns);

	rows = rb_ary_new();
	rb_iv_set(self, "@rows", rows);
	
	Data_Get_Struct(connection, TDS_Connection, conn);
	buf = value_to_cstr(query);

	rb_iv_set(self, "@status", Qnil);
	
	rb_iv_set(self, "@messages", rb_ary_new());
	errors = rb_ary_new();
	rb_iv_set(self, "@errors", errors);

	ct_cmd_alloc(conn->connection, &cmd);
	ct_command(cmd, CS_LANG_CMD, buf, CS_NULLTERM, CS_UNUSED);
	ct_send(cmd);

	// TODO:
	// - We should have an array of malloc'd cols
	// - Then we bind / fetch to those
	// - Finish conversions...
	
	while ((rc = ct_results(cmd, &resulttype)) == CS_SUCCEED) {
		switch (resulttype) {
		case CS_ROW_RESULT:
			rc = ct_res_info(cmd, CS_NUMDATA, &num_cols, sizeof(num_cols), &col_len);
			if (rc != CS_SUCCEED)
			{
				fprintf(stderr, "ct_res_info() failed\n");
				return 1;
			}

			col_data = (EX_COLUMN_DATA *)malloc(num_cols * sizeof (EX_COLUMN_DATA));
			if (col_data == NULL)
			 {
			    fprintf(stderr, "ex_fetch_data: malloc() failed");
			    return CS_MEM_ERROR;
			 }
			 cols = (CS_DATAFMT *)malloc(num_cols * sizeof (CS_DATAFMT));
			 if (cols == NULL)
			 {
			    fprintf(stderr, "ex_fetch_data: malloc() failed");
			    free(col_data);
			    return CS_MEM_ERROR;
			 }
			
			
			// Get column information
			for (i = 0; i < num_cols; i++) {
				rc = ct_describe(cmd, (i+1), &cols[i]);

				column_value = rb_hash_new();
				rb_hash_aset(column_value, column_name, rb_str_new2(cols[i].name));
				rb_hash_aset(column_value, column_type, rb_str_new2(column_type_name(cols[i])));
				rb_hash_aset(column_value, column_size, INT2FIX(cols[i].maxlength));
				rb_hash_aset(column_value, column_scale, INT2FIX(cols[i].scale));
				rb_hash_aset(column_value, column_precision, INT2FIX(cols[i].precision));

				rb_ary_push(columns, column_value);
				
				// TODO: need a better solution to avoid malloc'ing 2GB text columns
				if (cols[i].maxlength > 100000) {
					cols[i].maxlength = 100000;
				}
				col_data[i].value = (CS_CHAR *)malloc(cols[i].maxlength);
				if (col_data[i].value == NULL)
				{
					fprintf(stderr, "col_data malloc() failed");
					return CS_MEM_ERROR;
				}
				
				rc = ct_bind(cmd, (i + 1), &cols[i], col_data[i].value, &col_data[i].valuelen, &col_data[i].indicator);
				if (rc != CS_SUCCEED)
				{
					fprintf(stderr, "ct_bind() failed");
					break;
				}
			}
			
			// Fetch data
			while (((rc = ct_fetch(cmd, CS_UNUSED, CS_UNUSED, CS_UNUSED, &rows_read)) == CS_SUCCEED) || (rc == CS_ROW_FAIL)) {
				row_count = row_count + rows_read;

				row = rb_hash_new();
				rb_ary_push(rows, row);
				
				// Create Ruby objects
				for (i = 0; i < num_cols; i++) {
					if (col_data[i].indicator == -1) {
						rb_hash_aset(row, rb_str_new2(cols[i].name), Qnil);
						continue;
					}
					switch (cols[i].datatype) {
					case CS_TINYINT_TYPE:
					case CS_BIT_TYPE:
						col.datatype  = CS_INT_TYPE;
						col.locale    = NULL;
						cs_convert(conn->context, &cols[i], col_data[i].value, &col, &tempInt, &output_len);
						if(tempInt == 1) {
							rb_hash_aset(row, rb_str_new2(cols[i].name), Qtrue);
						} else {
							rb_hash_aset(row, rb_str_new2(cols[i].name), Qfalse);
						}
						break;
					case CS_INT_TYPE:
					case CS_SMALLINT_TYPE:
						col.datatype  = CS_INT_TYPE;
						col.locale    = NULL;
						cs_convert(conn->context, &cols[i], col_data[i].value, &col, &tempInt, &output_len);
						rb_hash_aset(row, rb_str_new2(cols[i].name), INT2FIX(tempInt));
						break;
					
					case CS_DATETIME_TYPE:
					case CS_DATETIME4_TYPE:
						col.maxlength = 30;
						col.datatype  = CS_CHAR_TYPE;
						col.format    = CS_FMT_NULLTERM;
						col.locale    = NULL;
						
						cs_convert(conn->context, &cols[i], col_data[i].value, &col, output, &output_len);
						rb_hash_aset(row, rb_str_new2(cols[i].name), rb_funcall(rb_DateTime, rb_intern("parse"), 1, rb_str_new2(output)));
						break;
					
					case CS_REAL_TYPE:
					case CS_FLOAT_TYPE:
					case CS_MONEY_TYPE:
					case CS_MONEY4_TYPE: 
					case CS_NUMERIC_TYPE:
					case CS_DECIMAL_TYPE:
						col.datatype  = CS_FLOAT_TYPE;
						col.locale    = NULL;
						// not too sure about this one.  Should it be a double or a float?
						cs_convert(conn->context, &cols[i], col_data[i].value, &col, &tempDouble, &output_len);
						rb_hash_aset(row, rb_str_new2(cols[i].name), rb_float_new(tempDouble));
						break;
					
					case CS_CHAR_TYPE:
					case CS_LONGCHAR_TYPE:
					case CS_TEXT_TYPE:
					case CS_VARCHAR_TYPE:
					case CS_UNICHAR_TYPE:
					case CS_UNIQUE_TYPE: // @todo should this one be handled differently?
						
						// if (tds_convert(conn->context, ctype, (TDS_CHAR *) src, srclen, SYBVARCHAR, &dres) < 0)
						// 	continue;
						// 	
						rb_hash_aset(row, rb_str_new2(cols[i].name), rb_str_new2(col_data[i].value));
						// 
						// free(dres.c);
					
						break;
					
					case CS_BINARY_TYPE:
					case CS_LONGBINARY_TYPE:
					case CS_VARBINARY_TYPE:
					case CS_IMAGE_TYPE:
						// rb_hash_aset(row, rb_str_new2(tds->res_info->columns[i]->column_name), rb_str_new((char *) ((TDSBLOB *) src)->textvalue, tds->res_info->columns[i]->column_cur_size));						
						rb_hash_aset(row, rb_str_new2(cols[i].name), Qnil);
						break;

					default:
						printf("here - %d\n", cols[i].datatype);
					}

				
				}
			}
			
			if( rc != CS_END_DATA )
			{
			    fprintf(stderr, "ct_fetch failed");
			}
			
			break;
		case CS_CMD_SUCCEED:
			rb_iv_set(self, "@status", Qnil);
			break;
		case CS_CMD_FAIL:
			rb_raise(rb_eIOError, "");
			// rb_iv_set(self, "@status", INT2FIX(0));
			break;
		case CS_CMD_DONE:
			rb_iv_set(self, "@status", Qnil);
			break;
		default:
			fprintf(stderr, "ct_results returned unexpected result type: %d\n", resulttype);
			break;
		}
	}
	
	// tds_set_parent(conn->tds, NULL);

	// VALUE err = rb_funcall(errors, rb_intern("first"), 0);
	// if(RTEST(err)) {
	// 	char* error_message = value_to_cstr(rb_hash_aref(err, rb_str_new2("message")));
	// 	rb_raise(rb_eIOError, error_message);
	// }

	
	return Qnil;	
}

static VALUE statement_Columns(VALUE self) {
	return rb_iv_get(self, "@columns");
}

static VALUE statement_Rows(VALUE self) {
	return rb_iv_get(self, "@rows");
}

static VALUE statement_Status(VALUE self) {
	return rb_iv_get(self, "@status");
}

static VALUE statement_Messages(VALUE self) {
	return rb_iv_get(self, "@messages");
}

static VALUE statement_Errors(VALUE self) {
	return rb_iv_get(self, "@errors");
}

static VALUE statement_Drop(VALUE self) {
	// TODO: Let's free our memory here...
	return Qnil;
}
static VALUE driver_Connect(VALUE self, VALUE connection_hash ) {
	return rb_class_new_instance(1, &connection_hash, rb_Connection);
}

void Init_freetds() {
	
	rb_require("date");	
	rb_DateTime = getClass("DateTime");
	
	// initialize the tds library	
	rb_FreeTDS = rb_define_module ("FreeTDS");

	rb_Driver = rb_define_class_under(rb_FreeTDS, "Driver", rb_cObject);
	rb_define_method(rb_Driver, "connect", driver_Connect, 1);
	
	rb_Connection = rb_define_class_under(rb_FreeTDS, "Connection", rb_cObject);
	rb_define_alloc_func(rb_Connection, alloc_tds_connection);
	rb_define_method(rb_Connection, "initialize", connection_Initialize, 1);
	rb_define_method(rb_Connection, "statement", connection_Statement, 1);
	rb_define_method(rb_Connection, "close", connection_Close, 0);
	
	rb_Statement = rb_define_class_under(rb_FreeTDS, "Statement", rb_cObject);
	rb_define_method(rb_Statement, "execute", statement_Execute, 0);
	rb_define_method(rb_Statement, "columns", statement_Columns, 0);
	rb_define_method(rb_Statement, "rows", statement_Rows, 0);
	rb_define_method(rb_Statement, "status", statement_Status, 0);
	rb_define_method(rb_Statement, "messages", statement_Messages, 0);
	rb_define_method(rb_Statement, "errors", statement_Errors, 0);
	rb_define_method(rb_Statement, "drop", statement_Drop, 0);
}