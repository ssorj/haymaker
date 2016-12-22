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

import email.utils as _email
import json as _json
import os as _os
import quopri as _quopri
import re as _re
import sqlite3 as _sqlite
import time as _time
import textwrap as _textwrap

from datetime import datetime as _datetime

from brbn import *
from faller import *
from pencil import *

_log = logger("haystack")
_strings = StringCatalog(__file__)
_topics = _json.loads(_strings["topics"])

class Application(BrbnApplication):
    def __init__(self, home_dir):
        super().__init__(home_dir)

        add_logged_module("haystack")

        setup_console_logging("info")

        path = _os.path.join(self.home_dir, "data", "data.sqlite")
        self.database = MessageDatabase(path)

        title = "Haystack"
        href = "/index.html"
        func = self.send_index
        self.index_page = BrbnPage(self, None, title, href, func)

        title = "Message '{}'"
        href = "/message.html?id={}"
        func = self.send_message
        self.message_page = BrbnPage(self, self.index_page, title, href, func)

        title = "Search '{}'"
        href = "/search.html?query={}"
        func = self.send_search
        self.search_page = BrbnPage(self, self.index_page, title, href, func)

        title = "Sender '{}'"
        href = "/sender.html?address={}"
        func = self.send_sender
        self.sender_page = BrbnPage(self, self.index_page, title, href, func)

        title = "Thread '{}'"
        href = "/thread.html?id={}"
        func = self.send_thread
        self.thread_page = BrbnPage(self, self.index_page, title, href, func)

    def receive_request(self, request):
        request.database_connection = self.database.connect()

        try:
            return self.send_response(request)
        finally:
            request.database_connection.close()

        return request.respond_not_found()

    def send_index(self, request):
        sql = ("select from_address from messages "
               "group by from_address having count(id) > 200 "
               "order by from_address collate nocase")

        records = self.database.query(request, sql)
        items = list()

        for record in records:
            address = record[0]
            href = self.sender_page.get_href(key=address)
            text = xml_escape(address)

            items.append(html_a(text, href))

        senders = html_ul(items, class_="three-column")

        items = list()

        for topic in _topics:
            href = self.search_page.get_href(key=topic)
            text = xml_escape(topic)

            items.append(html_a(text, href))

        topics = html_ul(items, class_="four-column")

        values = {
            "senders": senders,
            "topics": topics,
        }

        content = _strings["index"].format(**values)

        return self.index_page.send_response(request, content)

    def send_message(self, request):
        id = request.get("id")

        try:
            message = self.database.get(request, Message, id)
        except ObjectNotFound as e:
            return request.respond_not_found()

        in_reply_to = None
        in_reply_to_id = message.in_reply_to_id
        in_reply_to_link = in_reply_to_id

        if in_reply_to_id is not None:
            try:
                in_reply_to = self.database.get(request, Message, in_reply_to_id)
            except ObjectNotFound:
                pass

            if in_reply_to is not None:
                in_reply_to_link = self.message_page.render_brief_link(in_reply_to)

        thread = None
        thread_id = message.thread_id
        thread_link = thread_id

        if thread_id is not None:
            try:
                thread = self.database.get(request, Message, thread_id)
            except ObjectNotFound:
                pass

            if thread is not None:
                thread_link = self.thread_page.render_brief_link(thread)

        from_field = "{} <{}>".format(message.from_name, message.from_address)

        message_content = ""

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

            message_content = "\n".join(lines)

        values = {
            "id": xml_escape(message.id),
            "in_reply_to_link": in_reply_to_link,
            "thread_link": thread_link,
            "list_id": xml_escape(message.list_id),
            "from": xml_escape(from_field),
            "date": xml_escape(_email.formatdate(message.date)),
            "subject": xml_escape(message.subject),
            "message_content": message_content,
        }

        content = _strings["message"].format(**values)

        return self.message_page.send_response(request, content, message)

    def send_search(self, request):
        query = request.get("query")
        obj = Object(query, query)

        sql = ("select * from messages where id in "
               "(select distinct thread_id from messages_fts "
               " where messages_fts match ? limit 1000) "
               "order by date desc")

        records = self.database.query(request, sql, query)
        message = Message()
        rows = list()

        for record in records:
            message.load_from_record(record)
            thread_link = self.thread_page.render_brief_link(message)

            row = [
                thread_link,
                xml_escape(message.from_address),
                message.authored_words,
                xml_escape(str(_email.formatdate(message.date)[:-6])),
            ]

            rows.append(row)

        values = {
            "query": xml_escape(query),
            "messages": html_table(rows, False, class_="messages four"),
        }

        content = _strings["search"].format(**values)

        return self.search_page.send_response(request, content, obj)

    def send_sender(self, request):
        address = request.get("address")
        obj = Object(address, address)

        sql = ("select * from messages where from_address = ? "
               "order by date desc limit 1000")

        records = self.database.query(request, sql, address)
        message = Message()
        rows = list()

        for record in records:
            message.load_from_record(record)
            message_link = self.message_page.render_brief_link(message)

            row = [
                message_link,
                message.authored_words,
                xml_escape(str(_email.formatdate(message.date)[:-6])),
            ]

            rows.append(row)

        values = {
            "address": xml_escape(address),
            "messages": html_table(rows, False, class_="messages"),
        }

        content = _strings["sender"].format(**values)

        return self.sender_page.send_response(request, content, obj)

    def send_thread(self, request):
        id = request.get("id")

        try:
            head = self.database.get(request, Message, id)
        except ObjectNotFound as e:
            return request.respond_not_found()

        sql = ("select * from messages "
               "where thread_id = ? "
               "order by thread_position, date asc "
               "limit 1000")

        records = self.database.query(request, sql, id)
        messages = list()
        messages_by_id = dict()

        for record in records:
            message = Message()
            message.load_from_record(record)

            messages.append(message)
            messages_by_id[message.id] = message

        thread_index_rows = list()
        thread_content = list()

        for i, message in enumerate(messages):
            message_date = _time.strftime("%d %b %Y", _time.gmtime(message.date))
            message_href = self.message_page.get_href(message)
            message_number = i + 1
            message_title = "{}. {}".format(message_number, message.from_name)

            if message.in_reply_to_id is not None:
                rmessage = messages_by_id.get(message.in_reply_to_id)

                if rmessage is not None:
                    rperson = rmessage.from_name
                    message_title = "{} replying to {}".format(message_title, rperson)

            row = [
                html_a(xml_escape(message_title), "#{}".format(message_number)),
                xml_escape(message_date),
                message.authored_words,
                html_a("Message", message_href),
            ]

            thread_index_rows.append(row)

            thread_content.append(html_elem("h2", message_title, id=str(message_number)))
            thread_content.append(html_elem("pre", message.content))

        thread_index = html_table(thread_index_rows, False, class_="messages four")
        thread_content = "\n".join(thread_content)

        values = {
            "subject": xml_escape(head.subject),
            "thread_index": thread_index,
            "thread_content": thread_content,
        }

        content = _strings["thread"].format(**values)

        return self.thread_page.send_response(request, content, head)

class MessageDatabase:
    def __init__(self, path):
        self.path = path

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

        assert issubclass(cls, DatabaseObject), cls
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

class Object:
    def __init__(self, id, name, parent=None):
        self.id = id
        self._name = name
        self.parent = parent

    @property
    def name(self):
        return self._name

    def __repr__(self):
        return format_repr(self, self.id)

class DatabaseObject(Object):
    table = None

class Message(DatabaseObject):
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
