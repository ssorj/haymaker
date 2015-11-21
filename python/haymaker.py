#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
# 
#   http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

import os as _os
import sqlite3 as _sqlite
import time as _time
import email.utils as _email

from datetime import datetime as _datetime

from brbn import *
from faller import *
from pencil import *

_log = logger("haymaker")
_strings = StringCatalog(__file__)

class Application(BrbnApplication):
    def __init__(self, home_dir):
        super().__init__(home_dir)

        title = "Haymaker"
        href = "/index.html"
        self.message_index = Page(self, None, title, href)

        title = "Message '{}'"
        href = "/message.html?id={}"
        self.message_view = Page(self, self.message_index, title, href)
    
    def receive_request(self, request):
        path = _os.path.join(self.home_dir, "data", "data.sqlite")
        request.database_connection = _sqlite.connect(path)

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

        default_query = ["from_address = 'gsim@redhat.com'"]
        query = request.parameters.get("query", default_query)[0]

        sql = "select * from messages where {}".format(query)
        sql = "{} order by date desc limit 1000".format(sql)
        
        cursor.execute(sql)

        records = cursor.fetchall()
        rows = list()

        for record in records:
            message = Message.from_database_record(record)
            message_href = self.message_view.href.format(url_escape(message.id))
            
            cols = [
                html_a(xml_escape(message.subject), message_href),
                xml_escape(message.from_address),
                message.authored_lines,
                xml_escape(str(_email.formatdate(message.date)[:-6])),
            ]

            rows.append(cols)

        fields = html_ul(Message.fields, class_="four-column")
        messages = html_table(rows, False, class_="messages")
        body = _strings["message_index"].format(**locals())
        content = self.message_index.render(None, None, body)

        return request.respond_ok(content, "text/html")

    def send_message_view(self, request):
        id = request.parameters["id"][0]

        cursor = request.database_connection.cursor()
        message = Message.for_id(cursor, id)

        if message is None:
            return request.respond_not_found()

        rmessage = Message.for_id(cursor, message.in_reply_to_id)
        in_reply_to_link = ""
        
        if rmessage is not None:
            href = self.message_view.href.format(rmessage.id)
            in_reply_to_link = html_a(rmessage.id, href)

        values = {
            "id": xml_escape(message.id),
            "in_reply_to_link": in_reply_to_link,
            "list_id": xml_escape(message.list_id),
            "from": xml_escape("{} <{}>".format(message.from_name, message.from_address)),
            "date": xml_escape(_email.formatdate(message.date)),
            "subject": xml_escape(message.subject),
            "message_content": xml_escape(message.content),
        }
        
        body = _strings["message_view"].format(**values)
        content = self.message_view.render(message.subject, message.id, body)

        return request.respond_ok(content, "text/html")

class Page:
    def __init__(self, app, parent, title, href):
        self.app = app
        self.parent = parent
        self.title = title
        self.href = href
    
    def render(self, object_name, object_id, body):
        title = self.title.format(xml_escape(object_name))
        href = self.href.format(object_id)

        items = list()
        
        if self.parent is not None:
            items.append((self.parent.title, self.parent.href))
        
        items.append((title, href))

        links = [html_a(text, href) for text, href in items]
        path_navigation = html_ul(links, id="-path-navigation")

        return _strings["page_template"].format(**locals())

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
        "from_name",
        "from_address",
        "list_id",
        "date",
        "subject",
        "content_type",
        "content",
        "authored_lines",
    ]
    
    field_types = {
        "date": int,
        "authored_lines": int,
    }

    field_mbox_keys = {
        "id": "Message-ID",
        "in_reply_to_id": "In-Reply-To",
        "list_id": "List-Id",
        "subject": "Subject",
        "content_type": "Content",
    }

    def __init__(self):
        for name in self.fields:
            setattr(self, name, None)

    @classmethod
    def from_mbox_message(cls, mbox_message):
        message = cls()

        for name in cls.field_mbox_keys:
            mbox_key = cls.field_mbox_keys[name]
            value = mbox_message.get(mbox_key)
            field_type = cls.field_types.get(name, str)

            if value is not None:
                value = field_type(value)

            setattr(message, name, value)

        name, address = _email.parseaddr(mbox_message["From"])

        message.from_name = name
        message.from_address = address

        tup = _email.parsedate(mbox_message["Date"])
        message.date = _time.mktime(tup)
        
        if mbox_message.is_multipart():
            for part in mbox_message.walk():
                if part.get_content_type() == "text/plain":
                    message.content = part.get_payload()
                    break
                    
        elif mbox_message.get_content_type() == "text/plain":
            message.content = mbox_message.get_payload()

        count = 0

        if message.content is not None:
            for line in message.content.splitlines():
                line = line.strip()

                if line.startswith(">"):
                    continue

                if line == "":
                    continue

                count += 1

        message.authored_lines = count
            
        return message

    @classmethod
    def from_database_record(cls, record):
        message = cls()
        
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

        if record is None:
            return

        return Message.from_database_record(record)
    
    def save(self, cursor):
        columns = ", ".join(self.fields)
        values = ", ".join("?" * len(self.fields))
        args = [getattr(self, x) for x in self.fields]

        dml = "insert into messages ({}) values ({})".format(columns, values)

        cursor.execute(dml, args)

    def __repr__(self):
        return format_repr(self, self.id)
