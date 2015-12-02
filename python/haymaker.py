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

_log = logger("haymaker")
_strings = StringCatalog(__file__)

_topics = [
    "acknowledgement",
    "access control",
    "acl",
    "activemq",
    "address",
    "alternate exchange",
    "amqp 0-10",
    "amqp 1.0",
    "amqp",
    "apache",
    "api reference",
    "api",
    "at least once delivery",
    "authentication",
    "binding",
    "broker",
    "c++ broker",
    "c++ client",
    "c++",
    "celery",
    "celluloid",
    "centos",
    "certificate",
    "cli",
    "client certificate",
    "client server",
    "client side persistence",
    "client",
    "clr",
    "cluster",
    "cmake",
    "codec",
    "command",
    "committer",
    "communication",
    "compilation",
    "compression",
    "configuration",
    "conformance",
    "connection option",
    "connection",
    "consumer",
    "credit",
    "deadlock",
    "debian",
    "delayed delivery",
    "delivery",
    "destination",
    "disconnected operation",
    "discovery",
    "dispatch",
    "dlq",
    "documentation",
    "download",
    "durable subscription",
    "dynamic node",
    "dynamic",
    "endpoint",
    "engine",
    "exactly once delivery",
    "example",
    "exchange",
    "failover",
    "federation",
    "fedora",
    "flow control",
    "go",
    "gsoc",
    "gssapi",
    "ha",
    "header",
    "heartbeats",
    "incubator",
    "integration",
    "interop",
    "io",
    "ipv6",
    "java broker",
    "java client",
    "java",
    "javascript",
    "jca",
    "jira",
    "jms",
    "jmx",
    "jvm",
    "kerberos",
    "large message",
    "last value queue",
    "ldap",
    "leak",
    "library",
    "link recovery",
    "link",
    "linux",
    "listener",
    "logging",
    "mailing list",
    "management",
    "maven",
    "memory",
    "message body",
    "message groups",
    "message header",
    "message",
    "messaging api",
    "messenger",
    "mqtt",
    "msg",
    "net",
    "node",
    "openwire",
    "os x",
    "patch",
    "peer to peer",
    "performance",
    "perl",
    "perl",
    "persistence",
    "php",
    "plugin",
    "policy",
    "priority queue",
    "producer",
    "profiling",
    "project",
    "protocol detection",
    "protocol",
    "proton",
    "proton-c",
    "proton-j",
    "publish subscribe",
    "python",
    "qip",
    "qmf",
    "qpid-config",
    "qpid-cpp",
    "qpid::messaging",
    "qpidc",
    "qpidd",
    "queue",
    "rabbitmq",
    "rdma",
    "reactor",
    "receiver",
    "reconnect",
    "release",
    "request response",
    "review",
    "rhel",
    "ring queue",
    "router",
    "ruby",
    "rust",
    "sasl",
    "sctp",
    "selectors",
    "sender",
    "serializable destination",
    "server",
    "session",
    "settlement",
    "shared subscription",
    "solaris",
    "ssl",
    "stomp",
    "store",
    "subscription",
    "subversion",
    "svn",
    "test failure",
    "test",
    "testing",
    "thread",
    "timeout",
    "topic",
    "transaction",
    "transport",
    "message ttl",
    "ubuntu",
    "udp",
    "url",
    "vert.x",
    "visual studio",
    "vote",
    "web",
    "website",
    "websockets",
    "wiki",
    "windows",
    "wiring",
]

class Application(BrbnApplication):
    def __init__(self, home_dir):
        super().__init__(home_dir)

        add_logged_module("haymaker")

        setup_console_logging("info")
        
        path = _os.path.join(self.home_dir, "data", "data.sqlite")
        self.database = MessageDatabase(path)
        
        title = "Haymaker"
        href = "/index.html"
        self.index = Page(self, None, "index", title, href)

        title = "Message '{}'"
        href = "/message.html?id={}"
        self.message = Page(self, self.index, "message", title, href)

        title = "Search"
        href = "/search.html?query={}"
        self.search = Page(self, self.index, "search", title, href)

        title = "Sender '{}'"
        href = "/sender.html?id={}"
        self.sender = Page(self, self.index, "sender", title, href)
    
    def receive_request(self, request):
        request.database_connection = self.database.connect()
 
        try:
            return self.dispatch_request(request)
        finally:
            request.database_connection.close()

        return request.respond_not_found()

    def dispatch_request(self, request):
        if request.path_info in ("/", "/index.html"):
            return self.send_index(request)

        if request.path_info == "/message.html":
            return self.send_message(request)

        if request.path_info == "/search.html":
            return self.send_search(request)

        if request.path_info == "/sender.html":
            return self.send_sender(request)
        
        return self.send_file(request)

    def send_index(self, request):
        sql = ("select from_address from messages "
               "group by from_address having count(id) > 100 "
               "order by from_address collate nocase")

        records = self.database.query(request, sql)
        items = list()

        for record in records:
            address = record[0]
            href = "/sender.html?id={}".format(address)

            items.append(html_a(xml_escape(address), href))

        senders = html_ul(items, class_="three-column")

        items = list()
        
        for topic in _topics:
            href = "/search.html?query={}".format(url_escape(topic))
            items.append(html_a(xml_escape(topic), href))

        topics = html_ul(items, class_="four-column")
                         
        values = {
            "senders": senders,
            "topics": topics,
        }
        
        return self.index.respond(request, None, values)
        
    def send_message(self, request):
        id = request.parameters["id"][0]

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
                href = self.message.get_href(in_reply_to)
                in_reply_to_link = html_a(xml_escape(in_reply_to_id), href)

        from_field = "{} <{}>".format(message.from_name, message.from_address)

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
        
        values = {
            "id": xml_escape(message.id),
            "in_reply_to_link": in_reply_to_link,
            "list_id": xml_escape(message.list_id),
            "from": xml_escape(from_field),
            "date": xml_escape(_email.formatdate(message.date)),
            "subject": xml_escape(message.subject),
            "message_content": content,
        }

        return self.message.respond(request, message, values)

    def send_search(self, request):
        query = request.parameters.get("query", [""])[0]

        sql = ("select * from messages where id in "
               "(select id from messages_fts "
               " where messages_fts match ? limit 1000)"
               "order by date desc")

        records = self.database.query(request, sql, query)
        rows = list()

        for record in records:
            message = Message.from_database_record(record)
            message_href = self.message.get_href(message)
            
            cols = [
                html_a(xml_escape(message.subject), message_href),
                xml_escape(message.from_address),
                message.authored_words,
                xml_escape(str(_email.formatdate(message.date)[:-6])),
            ]

            rows.append(cols)

        values = {
            "query": xml_escape(query),
            "messages": html_table(rows, False, class_="messages four"),
        }

        return self.search.respond(request, None, values)

    def send_sender(self, request):
        address = request.parameters["id"][0]
        obj = Object(address, address)

        sql = ("select * from messages where from_address = ? "
               "order by date desc limit 1000")

        records = self.database.query(request, sql, address)
        rows = list()

        for record in records:
            message = Message.from_database_record(record)
            message_href = self.message.get_href(message)
            
            cols = [
                html_a(xml_escape(message.subject), message_href),
                message.authored_words,
                xml_escape(str(_email.formatdate(message.date)[:-6])),
            ]

            rows.append(cols)

        values = {
            "address": xml_escape(address),
            "messages": html_table(rows, False, class_="messages"),
        }

        return self.sender.respond(request, obj, values)
    
class Page:
    def __init__(self, app, parent, template, title, href):
        self.app = app
        self.parent = parent
        self.template = template
        self.title = title
        self.href = href

    def get_title(self, obj=None):
        if obj is None:
            return self.title

        return self.title.format(xml_escape(obj.name))
        
    def get_href(self, obj=None, id=None):
        if obj is None:
            return self.href

        return self.href.format(url_escape(obj.id))
        
    def render_link(self, obj=None):
        title = self.get_title(obj)
        href = self.get_href(obj)

        return html_a(title, href)
        
    def render(self, content, obj=None):
        title = self.get_title(obj)
        
        links = list()
        page = self
        obj = obj

        while page is not None:
            links.append(page.render_link(obj))

            page = page.parent

            if obj is not None:
                obj = obj.parent

        links.reverse()

        values = {
            "title": title,
            "path_navigation": html_ul(links, id="-path-navigation"),
            "content": content,
        }

        return _strings["page"].format(**values)

    def respond(self, request, obj=None, values={}):
        content = _strings[self.template]
        content = content.format(**values)
        content = self.render(content, obj)

        return request.respond_ok(content, "text/html")

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
               "({}, notindexed=id, tokenize=porter)"
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

        ddl = "insert into messages_fts (messages_fts)"
        ddl = "{} values ('optimize')".format(ddl)

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

        return cls.from_database_record(record)

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
    ]

    field_types = {
        "date": int,
        "authored_words": int,
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
        "subject",
        "authored_content",
    ]
    
    def __init__(self):
        super().__init__(None, None)
        
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
        
        content = cls._get_mbox_content(mbox_message)

        assert content is not None
        
        lines = list()

        for line in content.splitlines():
            line = line.strip()

            if line.startswith(">"):
                continue

            lines.append(line)

        message.content = content
        message.authored_content = "\n".join(lines)
        message.authored_words = len(message.authored_content.split())

        return message

    @classmethod
    def _get_mbox_content(cls, mbox_message):
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

    @classmethod
    def _get_authored_content(cls, content):
        lines = list()

        for line in content.splitlines():
            line = line.strip()

            if line.startswith(">"):
                continue

            lines.append(line)

        return "\n".join(lines)
    
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

    @property
    def name(self):
        return self.subject
    
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
