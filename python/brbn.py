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

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import datetime as _datetime
import logging as _logging
import os as _os
import sys as _sys
import time as _time
import traceback as _traceback
import urllib.parse as _parse

_log = _logging.getLogger("brbn")

_http_date = "%a, %d %b %Y %H:%M:%S %Z"

_content_types_by_extension = {
    ".css": "text/css",
    ".pdf": "application/pdf",
    ".png": "image/png",
    ".html": "application/xhtml+xml; charset=utf-8",
    ".jpeg": "image/jpeg",
    ".jpg": "image/jpeg",
    ".js": "application/javascript",
    ".svg": "image/svg+xml",
    ".woff": "application/font-woff",
}

class BrbnApplication:
    def __init__(self, home_dir=None):
        self.home_dir = home_dir

        self.pages_by_path = dict()
        self.files_by_path = dict()

    def load(self):
        self._load_files()

    def _load_files(self):
        if self.home_dir is None:
            return
        
        files_dir = _os.path.join(self.home_dir, "files")

        if not _os.path.isdir(files_dir):
            return
        
        for root, dirs, files in _os.walk(files_dir):
            for name in files:
                path = _os.path.join(root, name)

                with open(path, "rb") as file:
                    content = file.read()

                path = path[len(files_dir):]

                self.files_by_path[path] = content

    def __call__(self, env, start_response):
        request = _Request(self, env, start_response)
        
        try:
            content = self._process_request(request)
        except _RequestError as e:
            return request.respond_error(e)
        except Exception:
            return request.respond_unexpected_error()

        return content

    def _process_request(self, request):
        request._load()

        return self.receive_request(request)
    
    def receive_request(self, request):
        return self.send_response(request)

    def send_response(self, request):
        try:
            page = self.pages_by_path[request.path]
        except KeyError:
            return self.send_file(request)

        return page(request)
    
    def send_file(self, request, path=None):
        if path is None:
            path = request.path
        
        if path == "/":
            path = "/index.html"
        
        try:
            content = self.files_by_path[path]
        except KeyError:
            return request.respond_not_found()

        name, ext = _os.path.splitext(path)

        try:
            content_type = _content_types_by_extension[ext]
        except KeyError:
            raise Exception("Unknown file type: {}".format(path))

        return request.respond("200 OK", content, content_type)

class BrbnPage:
    def __init__(self, app, parent, title, href):
        self.app = app
        self.parent = parent
        self.title = title
        self.href = href

        self.path = self.href

        if "?" in self.href:
            self.path = self.href.split("?", 1)[0]

        self.app.pages_by_path[self.path] = self

    def get_title(self, obj=None):
        if obj is None:
            return self.title

        return self.title.format(xml_escape(obj.name))

    def get_href(self, obj=None, id=None):
        if obj is None:
            return self.href

        return self.href.format(url_escape(obj.id))

class _Request:
    def __init__(self, app, env, start_response):
        self.app = app
        self.env = env
        self.start_response = start_response

        self.response_headers = list()

        self.abstract_path = None
        self.parameters = None

    def _load(self):
        self.abstract_path = self._parse_path()
        self.parameters = self._parse_query_string()
    
    def _parse_path(self):
        path = self.path
        path = path[1:].split("/")
        path = [url_unescape(x) for x in path]

        return path

    def _parse_query_string(self):
        query_string = None

        if self.method == "GET":
            query_string = self.env["QUERY_STRING"]
        elif self.method == "POST":
            content_type = self.env["CONTENT_TYPE"]

            assert content_type == "application/x-www-form-urlencoded"

            length = int(self.env["CONTENT_LENGTH"])
            query_string = self.env["wsgi.input"].read(length)

        if not query_string:
            return {}

        try:
            return _parse.parse_qs(query_string, False, True)
        except ValueError:
            raise _RequestError(self, "Failed to parse query string")

    @property
    def method(self):
        return self.env["REQUEST_METHOD"]

    @property
    def path(self):
        return self.env["PATH_INFO"]
        
    def is_resource_modified(self, modification_time):
        ims_timestamp = self.env.get("HTTP_IF_MODIFIED_SINCE")

        if ims_timestamp is not None and modification_time is not None:
            modification_time = modification_time.replace(microsecond=0)
            ims_time = _datetime.datetime.strptime(ims_timestamp, _http_date)

            # _log.info("304 if updated {} <= IMS {}".format(update_time, ims_time))

            if modification_time <= ims_time:
                return False

        return True

    def respond(self, status, content=None, content_type=None):
        if content is None:
            self.response_headers.append(("Content-Length", "0"))
            self.start_response(status, self.response_headers)
            return (b"",)

        if isinstance(content, str):
            content = content.encode("utf-8")
        
        assert isinstance(content, bytes), type(content)
        assert content_type is not None

        content_length = len(content)

        self.response_headers.append(("Content-Length", str(content_length)))
        self.response_headers.append(("Content-Type", content_type))

        self.start_response(status, self.response_headers)

        return (content,)

    def respond_ok(self, content, content_type):
        return self.respond("200 OK", content, content_type)
    
    def respond_redirect(self, location):
        self.response_headers.append(("Location", str(location)))

        return self.respond("303 See Other")

    def respond_not_modified(self):
        return self.respond("304 Not Modified")
    
    def respond_not_found(self):
        content = "404 Not Found"
        content_type = "text/plain"
        
        return self.respond("404 Not Found", content, content_type)

    def respond_error(self, error):
        content = "Error! {}".format(str(error))
        content_type = "text/plain"
        
        return self.respond("500 Internal Server Error", content, content_type)

    def respond_unexpected_error(self):
        content = _traceback.format_exc()
        content_type = "text/plain"
        
        return self.respond("500 Internal Server Error", content, content_type)

class _RequestError(Exception):
    def __init__(self, request, message):
        super().__init__(message)

        self.request = request

# URL and XML escaping

from urllib.parse import quote_plus as _url_escape
from urllib.parse import unquote_plus as _url_unescape

from xml.sax.saxutils import escape as _xml_escape
from xml.sax.saxutils import unescape as _xml_unescape

def url_escape(string):
    if string is None:
        return

    return _url_escape(string)

def url_unescape(string):
    if string is None:
        return

    return _url_unescape(string)

_extra_entities = {
    '"': "&quot;",
    "'": "&#x27;",
    "/": "&#x2F;",
}

def xml_escape(string):
    if string is None:
        return

    return _xml_escape(string, _extra_entities)

def xml_unescape(string):
    if string is None:
        return

    return _xml_unescape(string)

class ExampleApplication(BrbnApplication):
    def receive_request(self, request):
        if request.path in ("/", "/index.html"):
            return self.send_file(request, "/example.html")

        return self.send_file(request)
