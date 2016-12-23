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

import brbn
import email.utils as _email
import json as _json
import logging as _logging
import os as _os
import quopri as _quopri
import re as _re
import sqlite3 as _sqlite
import time as _time
import textwrap as _textwrap

from datetime import datetime as _datetime
from pencil import *

_log = _logging.getLogger("haystack")
_strings = StringCatalog(__file__)
_topics = _json.loads(_strings["topics"])

class Haystack(brbn.Application):
    def __init__(self, home_dir):
        super().__init__(home_dir)

        path = _os.path.join(self.home, "data", "data.sqlite")
        self.database = Database(path)

        self.root_resource = _IndexPage(self)
        self.search_page = _SearchPage(self)
        self.thread_page = _ThreadPage(self)
        self.message_page = _MessagePage(self)
        
    def receive_request(self, request):
        request.database_connection = self.database.connect()

        try:
            return super().receive_request(request)
        finally:
            request.database_connection.close()

class _IndexPage(brbn.Page):
    def __init__(self, app):
        super().__init__(app, "/", _strings["index_page_body"])
    
    def get_title(self, request):
        return "Haystack"

    @brbn.xml
    def render_topics(self, request):
        items = list()
        
        for topic in _topics:
            href = self.app.search_page.get_href(request, query=topic)
            text = xml_escape(topic)

            items.append(html_a(text, href))

        return html_ul(items, class_="four-column")

class _SearchPage(brbn.Page):
    def __init__(self, app):
        super().__init__(app, "/search.html", _strings["search_page_body"])

    def get_title(self, request):
        query = request.get("query")
        return "Search '{}'".format(query)
    
    def render_query(self, request):
        return request.get("query")

    @brbn.xml
    def render_threads(self, request):
        query = request.get("query")

        sql = ("select * from messages where id in "
               "(select distinct thread_id from messages_fts "
               " where messages_fts match ? limit 1000) "
               "order by date desc")

        escaped_query = query.replace("\"", "\"\"")
        
        records = self.app.database.query(request, sql, escaped_query)
        message = Message()
        rows = list()

        for record in records:
            message.load_from_record(record)

            thread_link = self.app.thread_page.get_object_link(request, message)

            row = [
                thread_link,
                xml_escape(message.from_address),
                message.authored_words,
                xml_escape(str(_email.formatdate(message.date)[:-6])),
            ]

            rows.append(row)

        return html_table(rows, False, class_="messages four")

class _ThreadPage(brbn.ObjectPage):
    def __init__(self, app):
        super().__init__(app, "/thread.html", _strings["thread_page_body"])

    def get_object(self, request):
        id = request.get("id")
        return self.app.database.get(request, Message, id)

    def get_object_name(self, request, obj):
        return "Thread '{}'".format(obj.subject)
    
    def process(self, request):
        sql = ("select * from messages "
               "where thread_id = ? "
               "order by thread_position, date asc "
               "limit 1000")

        records = self.app.database.query(request, sql, request.object.id)

        request.messages = list()
        request.messages_by_id = dict()

        for record in records:
            message = Message()
            message.load_from_record(record)

            request.messages.append(message)
            request.messages_by_id[message.id] = message
        
    def render_title(self, request):
        return request.object.subject
        
    @brbn.xml
    def render_index(self, request):
        rows = list()
        
        for i, message in enumerate(request.messages):
            date = _time.strftime("%d %b %Y", _time.gmtime(message.date))
            number = i + 1
            title = self.get_message_title(request, message, number)

            row = [
                html_a(xml_escape(title), "#{}".format(number)),
                xml_escape(date),
                message.authored_words,
            ]

            rows.append(row)

        return html_table(rows, False, class_="messages")

    @brbn.xml
    def render_messages(self, request):
        out = list()
        
        for i, message in enumerate(request.messages):
            number = i + 1
            title = self.get_message_title(request, message, number)

            out.append(html_elem("h2", title, id=str(number)))
            out.append(html_elem("pre", xml_escape(message.content)))

        return "\n".join(out)

    def get_message_title(self, request, message, number):
        title = "{}. {}".format(number, message.from_name)

        if message.in_reply_to_id is not None:
            rmessage = request.messages_by_id.get(message.in_reply_to_id)

            if rmessage is not None:
                rperson = rmessage.from_name
                title = "{} replying to {}".format(title, rperson)

        return title

class _MessagePage(brbn.ObjectPage):
    def __init__(self, app):
        super().__init__(app, "/message", _strings["message_page_body"])

    def get_object(self, request):
        id = request.get("id")
        return self.app.database.get(request, Message, id)

    def get_object_name(self, request, obj):
        return "Message '{}'".format(obj.subject)

    def render_title(self, request):
        return self.render_subject(request)

    @brbn.xml
    def render_thread_link(self, request):
        thread = None
        thread_id = request.object.thread_id
        thread_link = xml_escape(thread_id)

        if thread_id is not None:
            try:
                thread = self.app.database.get(request, Message, thread_id)
            except ObjectNotFound:
                pass

            if thread is not None:
                thread_link = self.app.thread_page.get_object_link(request, thread)

        return thread_link
    
    @brbn.xml
    def render_in_reply_to_link(self, request):
        rmessage = None
        rmessage_id = request.object.in_reply_to_id
        rmessage_link = nvl(xml_escape(rmessage_id), "[None]")

        if rmessage_id is not None:
            try:
                rmessage = self.database.get(request, Message, rmessage_id)
            except ObjectNotFound:
                pass

            if rmessage is not None:
                rmessage_link = self.app.message_page.get_object_link(request, rmessage)

        return rmessage_link

    @brbn.xml
    def render_headers(self, request):
        message = request.object
        from_field = "{} <{}>".format(message.from_name, message.from_address)

        items = (
            ("ID", xml_escape(message.id)),
            ("List", xml_escape(message.list_id)),
            ("From", xml_escape(from_field)),
            ("Date", xml_escape(_email.formatdate(message.date))),
            ("Subject", xml_escape(message.subject)),
        )

        return html_table(items, False, True, class_="headers")
    
    @brbn.xml
    def render_content(self, request):
        message = request.object
        content = ""

        if message.content is not None:
            lines = list()

            for line in message.content.splitlines():
                line = line.strip()

                if line.startswith(">"):
                    m = _re.match("^[> ]+", line)
                    prefix = "\n{}".format(m.group(0))

                    line = prefix.join(_textwrap.wrap(line, 80))
                    line = html_span(xml_escape(line), class_="quoted")
                else:
                    line = "\n".join(_textwrap.wrap(line, 80))
                    line = xml_escape(line)

                lines.append(line)

            content = "\n".join(lines)

        return content
        
class Database:
    def __init__(self, path):
        self.path = path

        _log.info("Using database at {}".format(self.path))
        
    def connect(self):
        # XXX thread local connections
        return _sqlite.connect(self.path)

    def create_schema(self):
        columns = list()

        for name in Message.fields:
            field_type = Message.field_types.get(name, str)
            column_type = "text"

            if field_type == int:
                column_type = "integer"

            column = "{} {}".format(name, column_type)

            columns.append(column)

        statements = list()

        columns = ", ".join(columns)
        ddl = "create table messages ({});".format(columns)
        statements.append(ddl)

        ddl = "create index messages_id_idx on messages (id);"
        statements.append(ddl)

        columns = ", ".join(Message.fts_fields)
        ddl = ("create virtual table messages_fts using fts4 "
               "({}, notindexed=id, notindexed=thread_id, tokenize=porter)"
               "".format(columns))

        statements.append(ddl)

        conn = self.connect()
        cursor = conn.cursor()

        try:
            for statement in statements:
                cursor.execute(statement)
        finally:
            conn.close()

    def optimize(self):
        conn = self.connect()
        cursor = conn.cursor()

        ddl = "insert into messages_fts (messages_fts) values ('optimize')"

        try:
            cursor.execute(ddl)
        finally:
            conn.close()

    def cursor(self, request):
        return request.database_connection.cursor()

    def query(self, request, sql, *args):
        cursor = self.cursor(request)

        try:
            cursor.execute(sql, args)
            return cursor.fetchall()
        finally:
            cursor.close()

    def get(self, request, cls, id):
        _log.debug("Getting {} with ID {}".format(cls.__name__, id))

        assert issubclass(cls, _DatabaseObject), cls
        assert id is not None

        sql = "select * from {} where id = ?".format(cls.table)
        cursor = self.cursor(request)

        try:
            cursor.execute(sql, [id])
            record = cursor.fetchone()
        finally:
            cursor.close()

        if record is None:
            raise ObjectNotFound()

        obj = cls()
        obj.load_from_record(record)

        return obj

class ObjectNotFound(Exception):
    pass

class _DatabaseObject:
    table = None
    
    def __init__(self, id, name, parent=None):
        self.id = id
        self._name = name
        self.parent = parent

    @property
    def name(self):
        return self._name

    def __repr__(self):
        return format_repr(self, self.id)

class Message(_DatabaseObject):
    table = "messages"

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
        "authored_content",
        "authored_words",
        "thread_id",
        "thread_position",
    ]

    field_types = {
        "date": int,
        "authored_words": int,
        "thread_position": int,
    }

    field_mbox_keys = {
        "id": "Message-ID",
        "in_reply_to_id": "In-Reply-To",
        "list_id": "List-Id",
        "subject": "Subject",
        "content_type": "Content-Type",
    }

    fts_fields = [
        "id",
        "thread_id",
        "subject",
        "authored_content",
    ]

    def __init__(self):
        super().__init__(None, None)

        for name in self.fields:
            setattr(self, name, None)

    @property
    def name(self):
        return self.subject

    def load_from_mbox_message(self, mbox_message):
        for name in self.field_mbox_keys:
            mbox_key = self.field_mbox_keys[name]
            value = mbox_message.get(mbox_key)
            field_type = self.field_types.get(name, str)

            if value is not None:
                value = field_type(value)

            setattr(self, name, value)

        name, address = _email.parseaddr(mbox_message["From"])

        self.from_name = name
        self.from_address = address

        tup = _email.parsedate(mbox_message["Date"])
        self.date = _time.mktime(tup)

        content = _get_mbox_content(mbox_message)

        assert content is not None

        self.content = content
        self.authored_content = _get_authored_content(self.content)
        self.authored_words = len(self.authored_content.split())

    def load_from_record(self, record):
        for i, name in enumerate(self.fields):
            value = record[i]
            field_type = self.field_types.get(name, str)

            if value is not None:
                value = field_type(value)

            setattr(self, name, value)

    def save(self, cursor):
        columns = ", ".join(self.fields)
        values = ", ".join("?" * len(self.fields))
        args = [getattr(self, x) for x in self.fields]

        dml = "insert into messages ({}) values ({})".format(columns, values)

        cursor.execute(dml, args)

        columns = ", ".join(self.fts_fields)
        values = ", ".join("?" * len(self.fts_fields))
        args = [getattr(self, x) for x in self.fts_fields]

        dml = "insert into messages_fts ({}) values ({})".format(columns, values)

        cursor.execute(dml, args)

def _get_mbox_content(mbox_message):
    content_type = None
    content_encoding = None
    content = None

    if mbox_message.is_multipart():
        for part in mbox_message.walk():
            if part.get_content_type() == "text/plain":
                content_type = "text/plain"
                content_encoding = part["Content-Transfer-Encoding"]
                content = part.get_payload()

    if content_type is None:
        content_type = mbox_message.get_content_type()
        content_encoding = mbox_message["Content-Transfer-Encoding"]
        content = mbox_message.get_payload()

    assert content_type is not None
    assert content is not None

    if content_encoding == "quoted-printable":
        content = _quopri.decodestring(content)
        content = content.decode("utf-8", errors="replace")

    if content_type == "text/html":
        content = strip_tags(content)

    return content

def _get_authored_content(content):
    lines = list()

    for line in content.splitlines():
        line = line.strip()

        if line.startswith(">"):
            continue

        lines.append(line)

    return "\n".join(lines)
