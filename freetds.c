#include "ruby.h"

#include "tds.h"
#include "tdsconvert.h"

static VALUE rb_FreeTDS;
static VALUE rb_Driver;
static VALUE rb_Connection;

typedef struct _tds_connection {
	TDSSOCKET *tds;
	TDSLOGIN *login;
	TDSCONTEXT *context;
	TDSCONNECTION *connection;
} TDS_Connection;

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
	if (msg->msg_number == 0) {
		fprintf(stderr, "%s\n", msg->message);
		return 0;
	}

	if (msg->msg_number != 5701 && msg->msg_number != 5703
	    && msg->msg_number != 20018) {
		fprintf(stderr, "Msg %d, Level %d, State %d, Server %s, Line %d\n%s\n",
			msg->msg_number, msg->msg_level, msg->msg_state, msg->server, msg->line_number, msg->message);
	}

	return 0;
}

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
		result = malloc(max);
		strncpy(result, STR2CSTR(str), max);
	}
	
	return result;
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
		return self;
	}
	if (hostname && !port) {
		rb_raise(rb_eArgError, "No port specified");
		return self;
	}
	if (!username) {
		rb_raise(rb_eArgError, "No username specified");
		return self;
	}
	if (!password) {
		password = strdup("");
	}	
	
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

	/* Try to open a connection */
	conn->tds = tds_alloc_socket(conn->context, 512);
	tds_set_parent(conn->tds, NULL);
	conn->connection = tds_read_config_info(NULL, conn->login, conn->context->locale);
	if (!conn->connection || tds_connect(conn->tds, conn->connection) == TDS_FAIL) {
		tds_free_connection(conn->connection);
		rb_raise(rb_eException, "Connection failed");
		return self;
	}
	tds_free_connection(conn->connection);
	
	return self;
}

static VALUE connection_Execute(VALUE self, VALUE query) {
	int rows = 0;
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
	
	TDS_Connection* conn;
	TDSSOCKET * tds;
	
	Data_Get_Struct(self, TDS_Connection, conn);
	buf = value_to_cstr(query);
	
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
					fprintf(stdout, "%s\t", tds->res_info->columns[i]->column_name);
				}
				fprintf(stdout, "\n");
			}
			break;
		case TDS_ROW_RESULT:
			rows = 0;
			while ((rc = tds_process_row_tokens(tds, &rowtype, &computeid)) == TDS_SUCCEED) {
				rows++;

				if (!tds->res_info)
					continue;

				for (i = 0; i < tds->res_info->num_cols; i++) {
					if (tds_get_null(tds->res_info->current_row, i)) {
						if (print_rows)
							fprintf(stdout, "NULL\t");
						continue;
					}
					col = tds->res_info->columns[i];
					ctype = tds_get_conversion_type(col->column_type, col->column_size);

					src = &(tds->res_info->current_row[col->column_offset]);
					if (is_blob_type(col->column_type))
						src = (unsigned char *) ((TDSBLOB *) src)->textvalue;
					srclen = col->column_cur_size;


					if (tds_convert(tds->tds_ctx, ctype, (TDS_CHAR *) src, srclen, SYBVARCHAR, &dres) < 0)
						continue;
					if (print_rows)
						fprintf(stdout, "%s\t", dres.c);
					free(dres.c);
				}
				if (print_rows)
					fprintf(stdout, "\n");

			}
			break;
		case TDS_STATUS_RESULT:
			printf("(return status = %d)\n", tds->ret_status);
			break;
		default:
			break;
		}
	}
	
	return Qnil;	
}

static VALUE driver_Connect(VALUE self, VALUE connection_hash ) {
	return rb_class_new_instance(1, &connection_hash, rb_Connection);
}

void Init_freetds() {
	
	// initialize the tds library
	
	rb_FreeTDS = rb_define_module ("FreeTDS");

	rb_Driver = rb_define_class_under(rb_FreeTDS, "Driver", rb_cObject);
	rb_define_method(rb_Driver, "connect", driver_Connect, 1);
	
	rb_Connection = rb_define_class_under(rb_FreeTDS, "Connection", rb_cObject);
	rb_define_alloc_func(rb_Connection, alloc_tds_connection);
	rb_define_method(rb_Connection, "initialize", connection_Initialize, 1);
	rb_define_method(rb_Connection, "execute", connection_Execute, 1);
	
}