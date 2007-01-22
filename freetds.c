#include "ruby.h"

#include "tds.h"
#include "tdsconvert.h"

static VALUE rb_FreeTDS;
static VALUE rb_Driver;
static VALUE rb_Connection;
static VALUE rb_Statement;

static VALUE rb_DateTime;

typedef struct _tds_connection {
	TDSSOCKET *tds;
	TDSLOGIN *login;
	TDSCONTEXT *context;
	TDSCONNECTION *connection;
} TDS_Connection;

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

static int connection_handle_message(TDSCONTEXT * context, TDSSOCKET * tds, TDSMESSAGE * msg)
{
	VALUE self = (VALUE)tds_get_parent(tds);

	if(RTEST(self)) {
		if (msg->msg_number == 0) {
			VALUE messages = rb_iv_get(self, "@messages");
			rb_ary_push(messages, rb_str_new2(msg->message));
			return 0;
		}

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
	
	Data_Get_Struct(self, TDS_Connection, conn);

	conn->login = tds_alloc_login();
	conn->context = tds_alloc_context();
	if (conn->context->locale && !conn->context->locale->date_fmt) {
		/* set default in case there's no locale file */
		conn->context->locale->date_fmt = strdup("%b %e %Y %I:%M%p");
	}
	
	conn->context->msg_handler = connection_handle_message;
	conn->context->err_handler = connection_handle_message;
	
	/* now let's get the connection parameters */
	temp = rb_hash_aref(connection_hash, rb_str_new2("hostname"));
	hostname = value_to_cstr(temp);

	temp = rb_hash_aref(connection_hash, rb_str_new2("port"));
	if(RTEST(temp)) {
		port = FIX2INT(temp);
	}

	temp = rb_hash_aref(connection_hash, rb_str_new2("username"));
	username = value_to_cstr(temp);
	
	temp = rb_hash_aref(connection_hash, rb_str_new2("password"));
	password = value_to_cstr(temp);

	temp = rb_hash_aref(connection_hash, rb_str_new2("servername"));
	servername = value_to_cstr(temp);

	temp = rb_hash_aref(connection_hash, rb_str_new2("charset"));
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
	
	if (servername) {
		tds_set_user(conn->login, username);
		tds_set_app(conn->login, "TSQL");
		tds_set_library(conn->login, "TDS-Library");
		tds_set_server(conn->login, servername);
		tds_set_client_charset(conn->login, charset);
		tds_set_language(conn->login, "us_english");
		tds_set_passwd(conn->login, password);
		if (confile) {
			tds_set_interfaces_file_loc(confile);
		}
		/* else we specified hostname/port */
	} else {
		tds_set_user(conn->login, username);
		tds_set_app(conn->login, "TSQL");
		tds_set_library(conn->login, "TDS-Library");
		tds_set_server(conn->login, hostname);
		tds_set_port(conn->login, port);
		tds_set_client_charset(conn->login, charset);
		tds_set_language(conn->login, "us_english");
		tds_set_passwd(conn->login, password);
	}	
	
	/* free up all the memory */
	if (hostname)
		free(hostname);
	if (username)
		free(username);
	if (password)
		free(password);
	if (servername)
		free(servername);
	if (charset)
		free(charset);

	rb_iv_set(self, "@messages", rb_ary_new());
	errors = rb_ary_new();
	rb_iv_set(self, "@errors", errors);

	/* Try to open a connection */
	conn->tds = tds_alloc_socket(conn->context, 512);
	tds_set_parent(conn->tds, (void*)self);
	conn->connection = tds_read_config_info(NULL, conn->login, conn->context->locale);
	if (!conn->connection || tds_connect(conn->tds, conn->connection) == TDS_FAIL) {
		tds_free_connection(conn->connection);
		
		VALUE err = rb_funcall(errors, rb_intern("first"), 0);
		if(RTEST(err)) {
			char* error_message = value_to_cstr(rb_hash_aref(err, rb_str_new2("message")));
			rb_raise(rb_eIOError, error_message);
			
			return Qnil;
		}
		
		rb_raise(rb_eIOError, "Connection failed");
		return Qnil;
	}
	tds_free_connection(conn->connection);
	
	return Qnil;
}

static VALUE connection_Statement(VALUE self, VALUE query) {
	TDS_Connection* conn;
	
	Data_Get_Struct(self, TDS_Connection, conn);
	
	if(conn->tds) {
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
	
	tds_free_socket(conn->tds);
	tds_free_login(conn->login);
	tds_free_context(conn->context);	
	
	conn->tds = NULL;
	conn->login = NULL;
	conn->context = NULL;
}

static char* column_type_name(TDSCOLUMN* column) {
	char *column_type = NULL;

	switch (column->column_type) {
	case SYBINT1:
		column_type = "tinyint";
		break;
	case SYBBIT:
		column_type = "bit";
		break;
	case SYBINT2:
		column_type = "smallint";
		break;
	case SYBINT4:
		column_type = "int";
		break;
	case SYBINT8:
		column_type = "bigint";
		break;
	case SYBDATETIME:
		column_type = "datetime";
		break;
	case SYBDATETIME4:
		column_type = "smalldatetime";
		break;
	case SYBREAL:
		column_type = "real";
		break;
	case SYBMONEY:
		column_type = "money";
		break;
	case SYBMONEY4:
		column_type = "smallmoney";
		break;
	case SYBFLT8:
		column_type = "float";
		break;

	case SYBINTN:
		switch (column->column_size) {
		case 1:
			column_type = "tinyint";
			break;
		case 2:
			column_type = "smallint";
			break;
		case 4:
			column_type = "int";
			break;
		case 8:
			column_type = "bigint";
			break;
		}
		break;

	case SYBBITN:
		column_type = "bit";
		break;
	case SYBFLTN:
		switch (column->column_size) {
		case 4:
			column_type = "real";
			break;
		case 8:
			column_type = "float";
			break;
		}
		break;
	case SYBMONEYN:
		switch (column->column_size) {
		case 4:
			column_type = "smallmoney";
			break;
		case 8:
			column_type = "money";
			break;
		}
		break;
	case SYBDATETIMN:
		switch (column->column_size) {
		case 4:
			column_type = "smalldatetime";
			break;
		case 8:
			column_type = "datetime";
			break;
		}
		break;
	case SYBDECIMAL:
		column_type = "decimal";
		break;
	case SYBNUMERIC:
		column_type = "numeric";
		break;

	case SYBVARCHAR:
		column_type = "varchar";
		break;		
	case SYBCHAR:
		column_type = "char";
		break;
		
	case XSYBVARBINARY:
		column_type = "varbinary";
		break;
	case XSYBVARCHAR:
		column_type = "varchar";
		break;
	case XSYBBINARY:
		column_type = "binary";
		break;
	case XSYBCHAR:
		column_type = "char";
		break;
	case SYBTEXT:
		column_type = "text";
		break;
	case SYBIMAGE:
		column_type = "image";
		break;
	case XSYBNVARCHAR:
		column_type = "nvarchar";
		break;
	case XSYBNCHAR:
		column_type = "nchar";
		break;
	case SYBNTEXT:
		column_type = "ntext";
		break;
	case SYBUNIQUE:
		column_type = "uniqueidentifier";
		break;
	default:
		printf("here - %d\n", column->column_type);
		return NULL;
	}
	
	return column_type;
}

static VALUE statement_Execute(VALUE self) {
	int rc, i;
	TDSCOLUMN *col;
	int ctype;
	CONV_RESULT dres;
	unsigned char *src;
	TDS_INT srclen;
	TDS_INT rowtype;
	TDS_INT resulttype;
	TDS_INT computeid;
	struct timeval start, stop;
	int print_rows = 1;
	char message[128];
	char* buf;
	TDSDATEREC date_rec;
	
	TDS_Connection* conn;
	TDSSOCKET * tds;
	
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
	
	tds_set_parent(conn->tds, (void*)self);
	
	tds = conn->tds;
	rc = tds_submit_query(tds, buf);
	if (rc != TDS_SUCCEED) {
		fprintf(stderr, "tds_submit_query() failed\n");
		return 1;
	}

	while ((rc = tds_process_result_tokens(tds, &resulttype, NULL)) == TDS_SUCCEED) {
		switch (resulttype) {
		case TDS_ROWFMT_RESULT:
			if (tds->res_info) {
				for (i = 0; i < tds->res_info->num_cols; i++) {
					column = rb_hash_new();
					rb_ary_push(columns, column);
					
					rb_hash_aset(column, column_name, rb_str_new2(tds->res_info->columns[i]->column_name));
					rb_hash_aset(column, column_type, rb_str_new2(column_type_name(tds->res_info->columns[i])));
					rb_hash_aset(column, column_size, INT2FIX(tds->res_info->columns[i]->column_size));
					rb_hash_aset(column, column_scale, INT2FIX(tds->res_info->columns[i]->column_scale));
					rb_hash_aset(column, column_precision, INT2FIX(tds->res_info->columns[i]->column_prec));					
				}
			}
			break;
		case TDS_ROW_RESULT:
			while ((rc = tds_process_row_tokens(tds, &rowtype, &computeid)) == TDS_SUCCEED) {				
				if (!tds->res_info)
					continue;

				row = rb_hash_new();
				rb_ary_push(rows, row);
				
				for (i = 0; i < tds->res_info->num_cols; i++) {
					if (tds_get_null(tds->res_info->current_row, i)) {
						rb_hash_aset(row, rb_str_new2(tds->res_info->columns[i]->column_name), Qnil);
						continue;
					}
					col = tds->res_info->columns[i];
					ctype = tds_get_conversion_type(col->column_type, col->column_size);

					src = &(tds->res_info->current_row[col->column_offset]);
					srclen = col->column_cur_size;

					switch (col->column_type) {
					case SYBBIT:
					case SYBBITN:
						tds_convert(tds->tds_ctx, ctype, (TDS_CHAR *) src, srclen, SYBINT1, &dres);
						if(dres.ti) {
							rb_hash_aset(row, rb_str_new2(tds->res_info->columns[i]->column_name), Qtrue);
						} else {
							rb_hash_aset(row, rb_str_new2(tds->res_info->columns[i]->column_name), Qfalse);							
						}
						break;
					case SYBINT1:
					case SYBINT2:
					case SYBINT4:
					case SYBINT8:
					case SYBINTN:
						tds_convert(tds->tds_ctx, ctype, (TDS_CHAR *) src, srclen, SYBINT8, &dres);
						rb_hash_aset(row, rb_str_new2(tds->res_info->columns[i]->column_name), LONG2NUM(dres.bi));												
						break;
						
					case SYBDATETIME:
					case SYBDATETIME4:
					case SYBDATETIMN:
						if(tds_datecrack(SYBDATETIME, src, &date_rec)==TDS_SUCCEED) {				

							if(date_rec.year && date_rec.month && date_rec.day) {
								date_parts[0] = INT2FIX(date_rec.year);
								date_parts[1] = INT2FIX(date_rec.month);
								date_parts[2] = INT2FIX(date_rec.day);
								date_parts[3] = INT2FIX(date_rec.hour);
								date_parts[4] = INT2FIX(date_rec.minute);
								date_parts[5] = INT2FIX(date_rec.second);
								
								//printf("**%d/%d/%d %d:%d:%d\n", date_rec.year, date_rec.month, date_rec.day, date_rec.hour, date_rec.minute, date_rec.second);
								column_value = rb_funcall2(rb_DateTime, rb_intern("civil"), 6, &date_parts[0]);
							} else {
								column_value = Qnil;
							}
							
							rb_hash_aset(row, rb_str_new2(tds->res_info->columns[i]->column_name), column_value);
						} else {
							rb_hash_aset(row, rb_str_new2(tds->res_info->columns[i]->column_name), Qnil);
						}
						break;
						
					case SYBREAL:
					case SYBMONEY:
					case SYBMONEY4:
					case SYBFLT8:
					case SYBFLTN:
					case SYBMONEYN:
					case SYBDECIMAL:
					case SYBNUMERIC:
						tds_convert(tds->tds_ctx, ctype, (TDS_CHAR *) src, srclen, SYBFLT8, &dres);
						rb_hash_aset(row, rb_str_new2(tds->res_info->columns[i]->column_name), rb_float_new(dres.f));						
						break;
						
					case SYBVARCHAR:
					case SYBCHAR:
					case XSYBVARCHAR:
					case XSYBCHAR:
					case SYBTEXT:
					case XSYBNVARCHAR:
					case XSYBNCHAR:
					case SYBNTEXT:
					
					case SYBUNIQUE: // @todo should this one be handled differently?

						if (tds_convert(tds->tds_ctx, ctype, (TDS_CHAR *) src, srclen, SYBVARCHAR, &dres) < 0)
							continue;
							
						rb_hash_aset(row, rb_str_new2(tds->res_info->columns[i]->column_name), rb_str_new2(dres.c));

						free(dres.c);
						
						break;
						
					case XSYBVARBINARY:
					case XSYBBINARY:
					case SYBIMAGE:
						rb_hash_aset(row, rb_str_new2(tds->res_info->columns[i]->column_name), rb_str_new((char *) ((TDSBLOB *) src)->textvalue, tds->res_info->columns[i]->column_cur_size));						
						break;

//					default:
//						printf("here - %d\n", column->column_type);
					}

					
				}

			}
			break;
		case TDS_STATUS_RESULT:
			rb_iv_set(self, "@status", INT2FIX(tds->ret_status));
			
			break;
		default:
			break;
		}
	}
	
	tds_set_parent(conn->tds, NULL);

	VALUE err = rb_funcall(errors, rb_intern("first"), 0);
	if(RTEST(err)) {
		char* error_message = value_to_cstr(rb_hash_aref(err, rb_str_new2("message")));
		rb_raise(rb_eIOError, error_message);
	}
	
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
}