import sqlite3 as _sqlite

from brbn import *
from pencil import *

class _Application(BrbnApplication):
    def receive_request(self, request):
        request.database_connection = _sqlite.connect("data/data.sqlite") # XXX

        try:
            if request.path_info in ("/", "/index.html"):
                return self.send_message_list(request)
            if request.path_info == "/message.html":
                return self.send_message_view(request)
        finally:
            request.database_connection.close()

        return request.respond_not_found()
        
    def send_message_list(self, request):
        cursor = request.database_connection.cursor()
        statement = "select * from messages limit 200"
        content = list()

        cursor.execute(statement)

        records = cursor.fetchall()
        rows = list()

        for record in records:
            message = Message.from_database_record(record)

            cols = [
                xml_escape(message.from_),
                xml_escape(shorten(message.subject, 50)),
                xml_escape(message.date),
            ]

            rows.append(cols)
        
        content = html_table(rows, False)

        # XXX page template
        
        return request.respond_ok(content, "text/html")

    def send_message_view(self, request):
        id_ = request.parameters["id"][0]

        return request.respond_ok(id_, "text/plain")
    
app = _Application()

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
    def from_database_record(cls, record, message=None):
        if message is None:
            message = cls()

        for i, name in enumerate(cls.fields):
            value = record[i]
            field_type = cls.field_types.get(name, str)

            if value is not None:
                value = field_type(value)

            setattr(message, name, value)

        return message

    def load(self, cursor, id_):
        sql = "select * from messages where id = ?"

        cursor.execute(sql, [id_])

        record = cursor.fetchone()

        Message.from_database_record(record, self)
    
    def save(self, cursor):
        columns = ", ".join(self.fields)
        values = ", ".join("?" * len(self.fields))
        args = [getattr(self, x) for x in fields]

        dml = "insert into messages ({}) values ({})".format(columns, values)

        cursor.execute(dml, args)
