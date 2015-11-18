import sqlite3 as _sqlite

from brbn import *
from faller import *
from pencil import *

_log = logger("haymaker")
_strings = StringCatalog(__file__)

class Application(BrbnApplication):
    def receive_request(self, request):
        request.database_connection = _sqlite.connect("data/data.sqlite") # XXX

        try:
            return self.do_receive_request(request)
        finally:
            request.database_connection.close()

        return request.respond_not_found()

    def do_receive_request(self, request):
        if request.path_info in ("/", "/index.html"):
            return self.send_message_index(request)

        if request.path_info == "/message.html":
            return self.send_message_view(request)
        
        return self.send_file(request)

    def send_message_index(self, request):
        cursor = request.database_connection.cursor()
        statement = "select * from messages limit 200"

        cursor.execute(statement)

        records = cursor.fetchall()
        rows = list()

        for record in records:
            message = Message.from_database_record(record)
            message_href = "/message.html?id={}".format(url_escape(message.id))
            
            cols = [
                html_a(xml_escape(message.subject), message_href),
                xml_escape(message.from_),
                xml_escape(message.date),
            ]

            rows.append(cols)

        title = "Message Index"
        path_navigation = [(title, "/")]
        body = html_table(rows, False)
        content = html_page(title, path_navigation, body)

        return request.respond_ok(content, "text/html")

    def send_message_view(self, request):
        id = request.parameters["id"][0]

        cursor = request.database_connection.cursor()
        message = Message.for_id(cursor, id)

        title = "Message {}".format(message.id)
        message_href = "/message.html?id={}".format(url_escape(message.id))
        path_navigation = [("Message Index", "/"), (title, message_href)]
        body = _strings["message_view"].format(message=message)
        content = html_page(title, path_navigation, body)
        
        return request.respond_ok(content, "text/html")
    
app = Application()

class MessageDatabase:
    def __init__(self, path):
        self.path = path

    def init(self):
        columns = list()

        for name in Message.fields:
            field_type = Message.field_types.get(name, str)
            column_type = "text"

            if field_type == int:
                column_type = "integer"

            column = "{} {}".format(name, column_type)

            columns.append(column)

        statements = list()
            
        ddl = "create table messages ({});".format(", ".join(columns))
        statements.append(ddl)

        ddl = "create index messages_id_idx on messages (id);"
        statements.append(ddl)
        
        conn = _sqlite.connect(self.path)
        cursor = conn.cursor()

        try:
            for statement in statements:
                cursor.execute(statement)
        finally:
            cursor.close()
            conn.close()

class Message:
    fields = [
        "id",
        "in_reply_to_id",
        "from_",
        "list_id",
        "date",
        "subject",
        "content_type",
    ]
    
    field_types = {
    }

    field_mbox_keys = {
        "id": "Message-ID",
        "in_reply_to_id": "In-Reply-To",
        "from_": "From",
        "list_id": "List-Id",
        "date": "Date",
        "subject": "Subject",
        "content_type": "Content",
    }

    def __init__(self):
        for name in self.fields:
            setattr(self, name, None)

        self.payload = None

    @classmethod
    def from_mbox_message(cls, mbox_message):
        message = cls()

        for name in cls.fields:
            mbox_key = cls.field_mbox_keys[name]
            value = mbox_message.get(mbox_key)
            field_type = cls.field_types.get(name, str)

            if value is not None:
                value = field_type(value)

            setattr(message, name, value)

        return message

    @classmethod
    def from_database_record(cls, record):
        message = Message()
        
        for i, name in enumerate(cls.fields):
            value = record[i]
            field_type = cls.field_types.get(name, str)

            if value is not None:
                value = field_type(value)

            setattr(message, name, value)

        return message

    @classmethod
    def for_id(cls, cursor, id_):
        sql = "select * from messages where id = ?"

        cursor.execute(sql, [id_])

        record = cursor.fetchone()

        return Message.from_database_record(record)
    
    def save(self, cursor):
        columns = ", ".join(self.fields)
        values = ", ".join("?" * len(self.fields))
        args = [getattr(self, x) for x in fields]

        dml = "insert into messages ({}) values ({})".format(columns, values)

        cursor.execute(dml, args)

def html_page(title, path_navigation, body):
    links = [html_a(xml_escape(text), href) for text, href in path_navigation]
    path_navigation = html_ul(links, id="-path-navigation")

    return _strings["page_template"].format(**locals())
